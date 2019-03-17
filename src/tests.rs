use crate::service::{add_transaction_service, add_tso_service, TimestampService};
use crate::MemoryStorage;

use std::thread;
use std::time::Duration;

use labrpc::*;

#[test]
fn test_get_timestamp_under_unreliable_network() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    rn.add_server(tso_server.clone());
    let client1 = crate::client::Client::new(&rn, "", "tso1", "", "tso_server");
    rn.enable("tso1", false);
    let client2 = crate::client::Client::new(&rn, "", "tso2", "", "tso_server");
    rn.enable("tso2", false);
    let client3 = crate::client::Client::new(&rn, "", "tso3", "", "tso_server");
    rn.enable("tso3", false);

    let handle1 = thread::spawn(move || {
        let res = client1.try_get_timestamp();
        assert!(res.is_ok());
    });

    let handle2 = thread::spawn(move || {
        let res = client2.try_get_timestamp();
        assert!(res.is_ok());
    });

    let handle3 = thread::spawn(move || {
        let res = client3.try_get_timestamp();
        assert_eq!(res, Err(Error::Timeout));
    });

    thread::sleep(Duration::from_millis(100));
    rn.enable("tso1", true);
    thread::sleep(Duration::from_millis(200));
    rn.enable("tso2", true);
    thread::sleep(Duration::from_millis(400));
    rn.enable("tso3", true);

    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
fn test_predicate_many_preceders_read_predicates() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let store: MemoryStorage = Default::default();
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let mut client1 = crate::client::Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    client1.begin();
    client1.set(b"1".to_vec(), b"10".to_vec());
    client1.set(b"2".to_vec(), b"20".to_vec());
    assert!(client1.commit());

    let mut client2 = crate::client::Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    client2.begin();
    assert_eq!(client2.get(b"3".to_vec()), Ok(Vec::new()));

    let mut client3 = crate::client::Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    client3.begin();
    client3.set(b"3".to_vec(), b"30".to_vec());
    assert!(client3.commit());

    assert_eq!(client2.get(b"3".to_vec()), Ok(Vec::new()));
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
fn test_predicate_many_preceders_write_predicates() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let store: MemoryStorage = Default::default();
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let mut client1 = crate::client::Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    client1.begin();
    client1.set(b"1".to_vec(), b"10".to_vec());
    client1.set(b"2".to_vec(), b"20".to_vec());
    assert!(client1.commit());

    let mut client2 = crate::client::Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    client2.begin();

    let mut client3 = crate::client::Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    client3.begin();

    client2.set(b"1".to_vec(), b"20".to_vec());
    client2.set(b"2".to_vec(), b"30".to_vec());
    assert_eq!(client2.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client3.set(b"2".to_vec(), b"40".to_vec());
    assert!(client2.commit());
    assert!(!client3.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#lost-update-p4
fn test_lost_update() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let store: MemoryStorage = Default::default();
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let mut client1 = crate::client::Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    client1.begin();
    client1.set(b"1".to_vec(), b"10".to_vec());
    client1.set(b"2".to_vec(), b"20".to_vec());
    assert!(client1.commit());

    let mut client2 = crate::client::Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    client2.begin();

    let mut client3 = crate::client::Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    client3.begin();

    assert_eq!(client2.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client3.get(b"1".to_vec()), Ok(b"10".to_vec()));

    client2.set(b"1".to_vec(), b"11".to_vec());
    client3.set(b"1".to_vec(), b"11".to_vec());
    assert!(client2.commit());
    assert!(!client3.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_read_only() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let store: MemoryStorage = Default::default();
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let mut client1 = crate::client::Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    client1.begin();
    client1.set(b"1".to_vec(), b"10".to_vec());
    client1.set(b"2".to_vec(), b"20".to_vec());
    assert!(client1.commit());

    let mut client2 = crate::client::Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    client2.begin();

    let mut client3 = crate::client::Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    client3.begin();

    assert_eq!(client2.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client3.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client3.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client3.set(b"1".to_vec(), b"12".to_vec());
    client3.set(b"2".to_vec(), b"18".to_vec());
    assert!(client3.commit());

    assert_eq!(client2.get(b"2".to_vec()), Ok(b"20".to_vec()));
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_predicate_dependencies() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let store: MemoryStorage = Default::default();
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let mut client1 = crate::client::Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    client1.begin();
    client1.set(b"1".to_vec(), b"10".to_vec());
    client1.set(b"2".to_vec(), b"20".to_vec());
    assert!(client1.commit());

    let mut client2 = crate::client::Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    client2.begin();

    let mut client3 = crate::client::Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    client3.begin();

    assert_eq!(client2.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client2.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client3.set(b"3".to_vec(), b"30".to_vec());
    assert!(client3.commit());

    assert_eq!(client2.get(b"3".to_vec()), Ok(Vec::new()));
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_write_predicate() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let store: MemoryStorage = Default::default();
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let mut client1 = crate::client::Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    client1.begin();
    client1.set(b"1".to_vec(), b"10".to_vec());
    client1.set(b"2".to_vec(), b"20".to_vec());
    assert!(client1.commit());

    let mut client2 = crate::client::Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    client2.begin();

    let mut client3 = crate::client::Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    client3.begin();

    assert_eq!(client2.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client3.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client3.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client3.set(b"1".to_vec(), b"12".to_vec());
    client3.set(b"2".to_vec(), b"18".to_vec());
    assert!(client3.commit());

    client2.set(b"2".to_vec(), b"30".to_vec());
    assert!(!client2.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#write-skew-g2-item
fn test_write_skew() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let store: MemoryStorage = Default::default();
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let mut client1 = crate::client::Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    client1.begin();
    client1.set(b"1".to_vec(), b"10".to_vec());
    client1.set(b"2".to_vec(), b"20".to_vec());
    assert!(client1.commit());

    let mut client2 = crate::client::Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    client2.begin();

    let mut client3 = crate::client::Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    client3.begin();

    assert_eq!(client2.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client2.get(b"2".to_vec()), Ok(b"20".to_vec()));
    assert_eq!(client3.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client3.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client2.set(b"1".to_vec(), b"11".to_vec());
    client3.set(b"2".to_vec(), b"21".to_vec());

    assert!(client2.commit());
    assert!(client3.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#anti-dependency-cycles-g2
fn test_anti_dependency_cycles() {
    let rn = Network::new();
    let tso_server_name = "tso_server";
    let mut tso_server_builder = ServerBuilder::new(tso_server_name.to_owned());
    let server_name = "server";
    let mut server_builder = ServerBuilder::new(server_name.to_owned());
    add_tso_service(TimestampService, &mut tso_server_builder).unwrap();
    let store: MemoryStorage = Default::default();
    add_transaction_service(store, &mut server_builder).unwrap();
    let tso_server = tso_server_builder.build();
    let server = server_builder.build();
    rn.add_server(tso_server.clone());
    rn.add_server(server.clone());

    let mut client1 = crate::client::Client::new(&rn, "txn1", "tso1", "server", "tso_server");
    client1.begin();
    client1.set(b"1".to_vec(), b"10".to_vec());
    client1.set(b"2".to_vec(), b"20".to_vec());
    assert!(client1.commit());

    let mut client2 = crate::client::Client::new(&rn, "txn2", "tso2", "server", "tso_server");
    client2.begin();

    let mut client3 = crate::client::Client::new(&rn, "txn3", "tso3", "server", "tso_server");
    client3.begin();

    client2.set(b"3".to_vec(), b"30".to_vec());
    client3.set(b"4".to_vec(), b"42".to_vec());

    assert!(client2.commit());
    assert!(client3.commit());

    let mut client4 = crate::client::Client::new(&rn, "txn4", "tso4", "server", "tso_server");
    client4.begin();

    assert_eq!(client4.get(b"3".to_vec()), Ok(b"30".to_vec()));
    assert_eq!(client4.get(b"4".to_vec()), Ok(b"42".to_vec()));
}
