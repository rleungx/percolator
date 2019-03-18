use crate::client::Client;
use crate::service::{add_transaction_service, add_tso_service, TimestampService};
use crate::service::{TSOClient, TransactionClient};
use crate::MemoryStorage;

use std::thread;
use std::time::Duration;

use labrpc::*;

fn init(num_clinet: usize) -> (Network, Vec<Client>) {
    let mut clients = vec![];
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
    for i in 0..num_clinet {
        let txn_name_string = format!("txn{}", i);
        let txn_name = txn_name_string.as_str();
        let txn_client = TransactionClient::new(rn.create_client(txn_name.to_owned()));
        rn.enable(txn_name, true);
        rn.connect(txn_name, server_name);
        let tso_name_string = format!("tso{}", i);
        let tso_name = tso_name_string.as_str();
        let tso_client = TSOClient::new(rn.create_client(tso_name.to_owned()));
        rn.enable(tso_name, true);
        rn.connect(tso_name, tso_server_name);
        clients.push(crate::client::Client::new(tso_client, txn_client));
    }

    (rn, clients)
}

#[test]
fn test_get_timestamp_under_unreliable_network() {
    let (rn, clients) = init(3);
    let client0 = clients[0].to_owned();
    rn.enable("tso0", false);
    let client1 = clients[1].to_owned();
    rn.enable("tso1", false);
    let client2 = clients[2].to_owned();
    rn.enable("tso2", false);

    let handle1 = thread::spawn(move || {
        let res = client0.try_get_timestamp();
        assert!(res.is_ok());
    });

    let handle2 = thread::spawn(move || {
        let res = client1.try_get_timestamp();
        assert!(res.is_ok());
    });

    let handle3 = thread::spawn(move || {
        let res = client2.try_get_timestamp();
        assert_eq!(res, Err(Error::Timeout));
    });

    thread::sleep(Duration::from_millis(100));
    rn.enable("tso0", true);
    thread::sleep(Duration::from_millis(200));
    rn.enable("tso1", true);
    thread::sleep(Duration::from_millis(400));
    rn.enable("tso2", true);

    handle1.join().unwrap();
    handle2.join().unwrap();
    handle3.join().unwrap();
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
fn test_predicate_many_preceders_read_predicates() {
    let (_, clients) = init(3);
    let mut client0 = clients[0].to_owned();
    client0.begin();
    client0.set(b"1".to_vec(), b"10".to_vec());
    client0.set(b"2".to_vec(), b"20".to_vec());
    assert!(client0.commit());

    let mut client1 = clients[1].to_owned();
    client1.begin();
    assert_eq!(client1.get(b"3".to_vec()), Ok(Vec::new()));

    let mut client2 = clients[2].to_owned();
    client2.begin();
    client2.set(b"3".to_vec(), b"30".to_vec());
    assert!(client2.commit());

    assert_eq!(client1.get(b"3".to_vec()), Ok(Vec::new()));
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
fn test_predicate_many_preceders_write_predicates() {
    let (_, clients) = init(3);

    let mut client0 = clients[0].to_owned();
    client0.begin();
    client0.set(b"1".to_vec(), b"10".to_vec());
    client0.set(b"2".to_vec(), b"20".to_vec());
    assert!(client0.commit());

    let mut client1 = clients[1].to_owned();
    client1.begin();

    let mut client2 = clients[2].to_owned();
    client2.begin();

    client1.set(b"1".to_vec(), b"20".to_vec());
    client1.set(b"2".to_vec(), b"30".to_vec());
    assert_eq!(client1.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client2.set(b"2".to_vec(), b"40".to_vec());
    assert!(client1.commit());
    assert!(!client2.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#lost-update-p4
fn test_lost_update() {
    let (_, clients) = init(3);

    let mut client0 = clients[0].to_owned();
    client0.begin();
    client0.set(b"1".to_vec(), b"10".to_vec());
    client0.set(b"2".to_vec(), b"20".to_vec());
    assert!(client0.commit());

    let mut client1 = clients[1].to_owned();
    client1.begin();

    let mut client2 = clients[2].to_owned();
    client2.begin();

    assert_eq!(client1.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client2.get(b"1".to_vec()), Ok(b"10".to_vec()));

    client1.set(b"1".to_vec(), b"11".to_vec());
    client2.set(b"1".to_vec(), b"11".to_vec());
    assert!(client1.commit());
    assert!(!client2.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_read_only() {
    let (_, clients) = init(3);

    let mut client0 = clients[0].to_owned();
    client0.begin();
    client0.set(b"1".to_vec(), b"10".to_vec());
    client0.set(b"2".to_vec(), b"20".to_vec());
    assert!(client0.commit());

    let mut client1 = clients[1].to_owned();
    client1.begin();

    let mut client2 = clients[2].to_owned();
    client2.begin();

    assert_eq!(client1.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client2.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client2.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client2.set(b"1".to_vec(), b"12".to_vec());
    client2.set(b"2".to_vec(), b"18".to_vec());
    assert!(client2.commit());

    assert_eq!(client1.get(b"2".to_vec()), Ok(b"20".to_vec()));
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_predicate_dependencies() {
    let (_, clients) = init(3);

    let mut client0 = clients[0].to_owned();
    client0.begin();
    client0.set(b"1".to_vec(), b"10".to_vec());
    client0.set(b"2".to_vec(), b"20".to_vec());
    assert!(client0.commit());

    let mut client1 = clients[1].to_owned();
    client1.begin();

    let mut client2 = clients[2].to_owned();
    client2.begin();

    assert_eq!(client1.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client1.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client2.set(b"3".to_vec(), b"30".to_vec());
    assert!(client2.commit());

    assert_eq!(client1.get(b"3".to_vec()), Ok(Vec::new()));
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_write_predicate() {
    let (_, clients) = init(3);

    let mut client0 = clients[0].to_owned();
    client0.begin();
    client0.set(b"1".to_vec(), b"10".to_vec());
    client0.set(b"2".to_vec(), b"20".to_vec());
    assert!(client0.commit());

    let mut client1 = clients[1].to_owned();
    client1.begin();

    let mut client2 = clients[2].to_owned();
    client2.begin();

    assert_eq!(client1.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client2.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client2.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client2.set(b"1".to_vec(), b"12".to_vec());
    client2.set(b"2".to_vec(), b"18".to_vec());
    assert!(client2.commit());

    client1.set(b"2".to_vec(), b"30".to_vec());
    assert!(!client1.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#write-skew-g2-item
fn test_write_skew() {
    let (_, clients) = init(3);

    let mut client0 = clients[0].to_owned();
    client0.begin();
    client0.set(b"1".to_vec(), b"10".to_vec());
    client0.set(b"2".to_vec(), b"20".to_vec());
    assert!(client0.commit());

    let mut client1 = clients[1].to_owned();
    client1.begin();

    let mut client2 = clients[2].to_owned();
    client2.begin();

    assert_eq!(client1.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client1.get(b"2".to_vec()), Ok(b"20".to_vec()));
    assert_eq!(client2.get(b"1".to_vec()), Ok(b"10".to_vec()));
    assert_eq!(client2.get(b"2".to_vec()), Ok(b"20".to_vec()));

    client1.set(b"1".to_vec(), b"11".to_vec());
    client2.set(b"2".to_vec(), b"21".to_vec());

    assert!(client1.commit());
    assert!(client2.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#anti-dependency-cycles-g2
fn test_anti_dependency_cycles() {
    let (_, clients) = init(4);

    let mut client0 = clients[0].to_owned();
    client0.begin();
    client0.set(b"1".to_vec(), b"10".to_vec());
    client0.set(b"2".to_vec(), b"20".to_vec());
    assert!(client0.commit());

    let mut client1 = clients[1].to_owned();
    client1.begin();

    let mut client2 = clients[2].to_owned();
    client2.begin();

    client1.set(b"3".to_vec(), b"30".to_vec());
    client2.set(b"4".to_vec(), b"42".to_vec());

    assert!(client1.commit());
    assert!(client2.commit());

    let mut client3 = clients[3].to_owned();
    client3.begin();

    assert_eq!(client3.get(b"3".to_vec()), Ok(b"30".to_vec()));
    assert_eq!(client3.get(b"4".to_vec()), Ok(b"42".to_vec()));
}
