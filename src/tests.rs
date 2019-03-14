use crate::client::Client;
use crate::service::{add_transaction_service, add_tso_service, TSOClient, TimestampService};
use crate::MemoryStorage;

use labrpc::*;

fn add_tso_client(rn: &Network, client_name: &str, server_name: &str) -> TSOClient {
    let client = TSOClient::new(rn.create_client(client_name.to_owned()));
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);
    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    let commit_ts = client1.get_timestamp();
    assert!(client1.commit(1, commit_ts));

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);
    assert!(client2.get(2, b"3".to_vec()).is_empty());

    let client3 = Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    let start_ts = client3.get_timestamp();
    client3.begin(3, start_ts);
    client3.set(3, b"3".to_vec(), b"30".to_vec());
    let commit_ts = client3.get_timestamp();
    assert!(client3.commit(3, commit_ts));

    assert!(client2.get(2, b"3".to_vec()).is_empty());
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);
    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    let commit_ts = client1.get_timestamp();
    assert!(client1.commit(1, commit_ts));

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);

    let client3 = Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    let start_ts = client3.get_timestamp();
    client3.begin(3, start_ts);

    client2.set(2, b"1".to_vec(), b"20".to_vec());
    client2.set(2, b"2".to_vec(), b"30".to_vec());
    assert_eq!(client2.get(2, b"2".to_vec()), b"20".to_vec());

    client3.set(3, b"2".to_vec(), b"40".to_vec());
    let commit_ts = client2.get_timestamp();
    assert!(client2.commit(2, commit_ts));

    let commit_ts = client3.get_timestamp();
    assert!(!client3.commit(3, commit_ts));
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);
    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    let commit_ts = client1.get_timestamp();
    assert!(client1.commit(1, commit_ts));

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);

    let client3 = Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    let start_ts = client3.get_timestamp();
    client3.begin(3, start_ts);

    assert_eq!(client2.get(2, b"1".to_vec()), b"10".to_vec());
    assert_eq!(client3.get(3, b"1".to_vec()), b"10".to_vec());

    client2.set(2, b"1".to_vec(), b"11".to_vec());
    client3.set(3, b"1".to_vec(), b"11".to_vec());

    let commit_ts = client2.get_timestamp();
    assert!(client2.commit(2, commit_ts));

    let commit_ts = client3.get_timestamp();
    assert!(!client3.commit(3, commit_ts));
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);
    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    let commit_ts = client1.get_timestamp();
    assert!(client1.commit(1, commit_ts));

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);

    let client3 = Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    let start_ts = client3.get_timestamp();
    client3.begin(3, start_ts);

    assert_eq!(client2.get(2, b"1".to_vec()), b"10".to_vec());
    assert_eq!(client3.get(3, b"1".to_vec()), b"10".to_vec());
    assert_eq!(client3.get(3, b"2".to_vec()), b"20".to_vec());

    client3.set(3, b"1".to_vec(), b"12".to_vec());
    client3.set(3, b"2".to_vec(), b"18".to_vec());

    let commit_ts = client3.get_timestamp();
    assert!(client3.commit(3, commit_ts));

    assert_eq!(client2.get(2, b"2".to_vec()), b"20".to_vec());
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);
    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    let commit_ts = client1.get_timestamp();
    assert!(client1.commit(1, commit_ts));

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);

    let client3 = Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    let start_ts = client3.get_timestamp();
    client3.begin(3, start_ts);

    assert_eq!(client2.get(2, b"1".to_vec()), b"10".to_vec());
    assert_eq!(client2.get(2, b"2".to_vec()), b"20".to_vec());

    client3.set(3, b"3".to_vec(), b"30".to_vec());
    let commit_ts = client3.get_timestamp();
    assert!(client3.commit(3, commit_ts));

    assert!(client2.get(2, b"3".to_vec()).is_empty());
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);
    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    let commit_ts = client1.get_timestamp();
    assert!(client1.commit(1, commit_ts));

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);

    let client3 = Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    let start_ts = client3.get_timestamp();
    client3.begin(3, start_ts);

    assert_eq!(client2.get(2, b"1".to_vec()), b"10".to_vec());
    assert_eq!(client3.get(3, b"1".to_vec()), b"10".to_vec());
    assert_eq!(client3.get(3, b"2".to_vec()), b"20".to_vec());

    client3.set(3, b"1".to_vec(), b"12".to_vec());
    client3.set(3, b"2".to_vec(), b"18".to_vec());

    let commit_ts = client3.get_timestamp();
    assert!(client3.commit(3, commit_ts));

    client2.set(2, b"2".to_vec(), b"30".to_vec());
    let commit_ts = client2.get_timestamp();
    assert!(!client2.commit(2, commit_ts));
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);
    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    let commit_ts = client1.get_timestamp();
    assert!(client1.commit(1, commit_ts));

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);

    let client3 = Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    let start_ts = client3.get_timestamp();
    client3.begin(3, start_ts);

    assert_eq!(client2.get(2, b"1".to_vec()), b"10".to_vec());
    assert_eq!(client2.get(2, b"2".to_vec()), b"20".to_vec());
    assert_eq!(client3.get(3, b"1".to_vec()), b"10".to_vec());
    assert_eq!(client3.get(3, b"2".to_vec()), b"20".to_vec());

    client2.set(2, b"1".to_vec(), b"11".to_vec());
    client3.set(3, b"2".to_vec(), b"21".to_vec());

    let commit_ts = client2.get_timestamp();
    assert!(client2.commit(2, commit_ts));
    let commit_ts = client3.get_timestamp();
    assert!(client3.commit(3, commit_ts));
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);
    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    let commit_ts = client1.get_timestamp();
    assert!(client1.commit(1, commit_ts));

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);

    let client3 = Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    let start_ts = client3.get_timestamp();
    client3.begin(3, start_ts);

    client2.set(2, b"3".to_vec(), b"30".to_vec());
    client3.set(3, b"4".to_vec(), b"42".to_vec());

    let commit_ts = client2.get_timestamp();
    assert!(client2.commit(2, commit_ts));
    let commit_ts = client3.get_timestamp();
    assert!(client3.commit(3, commit_ts));

    let client4 = Client::new(&rn, "txn4", "tso4", "server", "tso_server");
    let start_ts = client4.get_timestamp();
    client4.begin(4, start_ts);

    assert_eq!(client4.get(4, b"3".to_vec()), b"30".to_vec());
    assert_eq!(client4.get(4, b"4".to_vec()), b"42".to_vec());
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);

    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    client1.set(1, b"3".to_vec(), b"30".to_vec());
    client1.set(1, b"4".to_vec(), b"40".to_vec());

    fail::setup();
    fail::cfg("commit_secondaries_fail", "return()").unwrap();

    let commit_ts = client1.get_timestamp();
    assert!(client1.commit(1, commit_ts));

    fail::remove("commit_secondaries_fail");

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);

    assert_eq!(client2.get(2, b"1".to_vec()), b"10".to_vec());
    assert_eq!(client2.get(2, b"2".to_vec()), b"20".to_vec());
    assert_eq!(client2.get(2, b"3".to_vec()), b"30".to_vec());
    assert_eq!(client2.get(2, b"4".to_vec()), b"40".to_vec());
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
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let client_name = "tso_client";
    let client = add_tso_client(&rn, client_name, tso_server_name);
    let store = MemoryStorage::new(client);
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let client1 = Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    let start_ts = client1.get_timestamp();
    client1.begin(1, start_ts);

    client1.set(1, b"1".to_vec(), b"10".to_vec());
    client1.set(1, b"2".to_vec(), b"20".to_vec());
    client1.set(1, b"3".to_vec(), b"30".to_vec());
    client1.set(1, b"4".to_vec(), b"40".to_vec());

    fail::setup();
    fail::cfg("commit_primary_fail", "return()").unwrap();

    let commit_ts = client1.get_timestamp();
    assert!(!client1.commit(1, commit_ts));

    fail::remove("commit_primary_fail");

    let client2 = Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    let start_ts = client2.get_timestamp();
    client2.begin(2, start_ts);

    assert!(client2.get(2, b"1".to_vec()).is_empty());
    assert!(client2.get(2, b"2".to_vec()).is_empty());
    assert!(client2.get(2, b"3".to_vec()).is_empty());
    assert!(client2.get(2, b"4".to_vec()).is_empty());
}
