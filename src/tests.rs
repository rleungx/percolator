use crate::service::{
    add_service, add_transaction_service, Client, TimestampService, TransactionClient,
};
use crate::service::{BeginRequest, CommitRequest, GetRequest, SetRequest};
use crate::MemoryStorage;

use labrpc::*;

fn add_txn_client(rn: &Network, client_name: &str, server_name: &str) -> TransactionClient {
    let client = TransactionClient::new(rn.create_client(client_name.to_owned()));
    rn.enable(client_name, true);
    rn.connect(client_name, server_name);
    client
}

fn add_tso_client(rn: &Network, client_name: &str, server_name: &str) -> Client {
    let client = Client::new(rn.create_client(client_name.to_owned()));
    rn.enable(client_name, true);
    rn.connect(client_name, server_name);
    client
}

#[test]
#[cfg(feature = "no-fail")]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
fn test_predicate_many_preceders_read_predicates() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);

    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    assert!(txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);

    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });
    assert!(txn_client2
        .get(&GetRequest {
            id: 2,
            key: b"3".to_vec()
        })
        .unwrap()
        .value
        .is_empty());

    let client_name = "txn3";
    let txn_client3 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client3.begin(&BeginRequest { id: 3 });
    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"3".to_vec(),
        value: b"30".to_vec(),
    });
    assert!(txn_client3.commit(&CommitRequest { id: 3 }).unwrap().res);
    assert!(txn_client2
        .get(&GetRequest {
            id: 2,
            key: b"3".to_vec()
        })
        .unwrap()
        .value
        .is_empty());
}

#[test]
#[cfg(feature = "no-fail")]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
fn test_predicate_many_preceders_write_predicates() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    assert!(txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);
    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });
    let client_name = "txn3";
    let txn_client3 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client3.begin(&BeginRequest { id: 3 });
    let _ = txn_client2.set(&SetRequest {
        id: 2,
        key: b"1".to_vec(),
        value: b"20".to_vec(),
    });
    let _ = txn_client2.set(&SetRequest {
        id: 2,
        key: b"2".to_vec(),
        value: b"30".to_vec(),
    });
    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"2".to_vec(),
            })
            .unwrap()
            .value,
        b"20".to_vec()
    );
    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"2".to_vec(),
        value: b"40".to_vec(),
    });

    assert!(txn_client2.commit(&CommitRequest { id: 2 }).unwrap().res);
    assert!(!txn_client3.commit(&CommitRequest { id: 3 }).unwrap().res);
}

#[test]
#[cfg(feature = "no-fail")]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#lost-update-p4
fn test_lost_update() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    assert!(txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);

    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });

    let client_name = "txn3";
    let txn_client3 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client3.begin(&BeginRequest { id: 3 });

    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    assert_eq!(
        txn_client3
            .get(&GetRequest {
                id: 3,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    let _ = txn_client2.set(&SetRequest {
        id: 2,
        key: b"1".to_vec(),
        value: b"11".to_vec(),
    });
    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"1".to_vec(),
        value: b"11".to_vec(),
    });

    assert!(txn_client2.commit(&CommitRequest { id: 2 }).unwrap().res);
    assert!(!txn_client3.commit(&CommitRequest { id: 3 }).unwrap().res);
}

#[test]
#[cfg(feature = "no-fail")]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_read_only() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    assert!(txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);

    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });

    let client_name = "txn3";
    let txn_client3 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client3.begin(&BeginRequest { id: 3 });
    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    assert_eq!(
        txn_client3
            .get(&GetRequest {
                id: 3,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    assert_eq!(
        txn_client3
            .get(&GetRequest {
                id: 3,
                key: b"2".to_vec(),
            })
            .unwrap()
            .value,
        b"20".to_vec()
    );
    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"1".to_vec(),
        value: b"12".to_vec(),
    });
    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"2".to_vec(),
        value: b"18".to_vec(),
    });
    assert!(txn_client3.commit(&CommitRequest { id: 3 }).unwrap().res);

    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"2".to_vec(),
            })
            .unwrap()
            .value,
        b"20".to_vec()
    );
}

#[test]
#[cfg(feature = "no-fail")]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_predicate_dependencies() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    assert!(txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);
    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });
    let client_name = "txn3";
    let txn_client3 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client3.begin(&BeginRequest { id: 3 });

    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"2".to_vec(),
            })
            .unwrap()
            .value,
        b"20".to_vec()
    );

    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"3".to_vec(),
        value: b"30".to_vec(),
    });
    assert!(txn_client3.commit(&CommitRequest { id: 3 }).unwrap().res);

    assert!(txn_client2
        .get(&GetRequest {
            id: 2,
            key: b"3".to_vec(),
        })
        .unwrap()
        .value
        .is_empty());
}

#[test]
#[cfg(feature = "no-fail")]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_write_predicate() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    assert!(txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);
    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });
    let client_name = "txn3";
    let txn_client3 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client3.begin(&BeginRequest { id: 3 });

    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    assert_eq!(
        txn_client3
            .get(&GetRequest {
                id: 3,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    assert_eq!(
        txn_client3
            .get(&GetRequest {
                id: 3,
                key: b"2".to_vec(),
            })
            .unwrap()
            .value,
        b"20".to_vec()
    );
    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"1".to_vec(),
        value: b"12".to_vec(),
    });
    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"2".to_vec(),
        value: b"18".to_vec(),
    });
    assert!(txn_client3.commit(&CommitRequest { id: 3 }).unwrap().res);

    let _ = txn_client2.set(&SetRequest {
        id: 2,
        key: b"2".to_vec(),
        value: b"30".to_vec(),
    });
    assert!(!txn_client2.commit(&CommitRequest { id: 2 }).unwrap().res);
}

#[test]
#[cfg(feature = "no-fail")]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#write-skew-g2-item
fn test_write_skew() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    assert!(txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);
    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });
    let client_name = "txn3";
    let txn_client3 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client3.begin(&BeginRequest { id: 3 });

    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"2".to_vec(),
            })
            .unwrap()
            .value,
        b"20".to_vec()
    );
    assert_eq!(
        txn_client3
            .get(&GetRequest {
                id: 3,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    assert_eq!(
        txn_client3
            .get(&GetRequest {
                id: 3,
                key: b"2".to_vec(),
            })
            .unwrap()
            .value,
        b"20".to_vec()
    );
    let _ = txn_client2.set(&SetRequest {
        id: 2,
        key: b"1".to_vec(),
        value: b"11".to_vec(),
    });
    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"2".to_vec(),
        value: b"21".to_vec(),
    });
    assert!(txn_client2.commit(&CommitRequest { id: 2 }).unwrap().res);
    assert!(txn_client3.commit(&CommitRequest { id: 3 }).unwrap().res);
}

#[test]
#[cfg(feature = "no-fail")]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#anti-dependency-cycles-g2
fn test_anti_dependency_cycles() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    assert!(txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);
    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });
    let client_name = "txn3";
    let txn_client3 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client3.begin(&BeginRequest { id: 3 });

    let _ = txn_client2.set(&SetRequest {
        id: 2,
        key: b"3".to_vec(),
        value: b"30".to_vec(),
    });
    let _ = txn_client3.set(&SetRequest {
        id: 3,
        key: b"4".to_vec(),
        value: b"42".to_vec(),
    });
    assert!(txn_client2.commit(&CommitRequest { id: 2 }).unwrap().res);
    assert!(txn_client3.commit(&CommitRequest { id: 3 }).unwrap().res);
    let client_name = "txn4";
    let txn_client4 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client4.begin(&BeginRequest { id: 4 });
    assert_eq!(
        txn_client4
            .get(&GetRequest {
                id: 4,
                key: b"3".to_vec(),
            })
            .unwrap()
            .value,
        b"30".to_vec()
    );
    assert_eq!(
        txn_client4
            .get(&GetRequest {
                id: 4,
                key: b"4".to_vec(),
            })
            .unwrap()
            .value,
        b"42".to_vec()
    );
}

#[test]
#[cfg(not(feature = "no-fail"))]
fn test_commit_primary_then_fail() {
    fail::teardown();
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"3".to_vec(),
        value: b"30".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"4".to_vec(),
        value: b"40".to_vec(),
    });
    fail::setup();
    fail::cfg("commit_secondaries_fail", "return()").unwrap();
    assert!(txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);
    fail::remove("commit_secondaries_fail");
    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });
    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"1".to_vec(),
            })
            .unwrap()
            .value,
        b"10".to_vec()
    );
    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"2".to_vec(),
            })
            .unwrap()
            .value,
        b"20".to_vec()
    );
    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"3".to_vec(),
            })
            .unwrap()
            .value,
        b"30".to_vec()
    );
    assert_eq!(
        txn_client2
            .get(&GetRequest {
                id: 2,
                key: b"4".to_vec(),
            })
            .unwrap()
            .value,
        b"40".to_vec()
    );
}

#[test]
#[cfg(not(feature = "no-fail"))]
fn test_commit_primary_fail() {
    fail::teardown();
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client_name = "txn1";
    let txn_client1 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client1.begin(&BeginRequest { id: 1 });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"1".to_vec(),
        value: b"10".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"2".to_vec(),
        value: b"20".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"3".to_vec(),
        value: b"30".to_vec(),
    });
    let _ = txn_client1.set(&SetRequest {
        id: 1,
        key: b"4".to_vec(),
        value: b"40".to_vec(),
    });
    fail::setup();
    fail::cfg("commit_primary_fail", "return()").unwrap();
    assert!(!txn_client1.commit(&CommitRequest { id: 1 }).unwrap().res);
    fail::remove("commit_primary_fail");
    let client_name = "txn2";
    let txn_client2 = add_txn_client(&rn, client_name, server_name);
    let _ = txn_client2.begin(&BeginRequest { id: 2 });
    assert!(txn_client2
        .get(&GetRequest {
            id: 2,
            key: b"1".to_vec(),
        })
        .unwrap()
        .value
        .is_empty());
    assert!(txn_client2
        .get(&GetRequest {
            id: 2,
            key: b"2".to_vec(),
        })
        .unwrap()
        .value
        .is_empty());
    assert!(txn_client2
        .get(&GetRequest {
            id: 2,
            key: b"3".to_vec(),
        })
        .unwrap()
        .value
        .is_empty());
    assert!(txn_client2
        .get(&GetRequest {
            id: 2,
            key: b"4".to_vec(),
        })
        .unwrap()
        .value
        .is_empty());
}
