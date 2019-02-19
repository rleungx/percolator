use crate::Store;
use crate::TestStorageBuilder;
use crate::Transaction;

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
fn test_predicate_many_preceders_read_predicates() {
    let s = TestStorageBuilder::build();
    let mut t = s.begin();
    t.set(b"1".to_vec(), b"10".to_vec());
    t.set(b"2".to_vec(), b"20".to_vec());
    assert!(t.commit());
    let t1 = s.begin();
    assert!(t1.get(b"3".to_vec()).is_none());
    let mut t2 = s.begin();
    t2.set(b"3".to_vec(), b"30".to_vec());
    assert!(t2.commit());
    assert!(t1.get(b"3".to_vec()).is_none());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#predicate-many-preceders-pmp
fn test_predicate_many_preceders_write_predicates() {
    let s = TestStorageBuilder::build();
    let mut t = s.begin();
    t.set(b"1".to_vec(), b"10".to_vec());
    t.set(b"2".to_vec(), b"20".to_vec());
    assert!(t.commit());
    let mut t1 = s.begin();
    let mut t2 = s.begin();
    t1.set(b"1".to_vec(), b"20".to_vec());
    t1.set(b"2".to_vec(), b"30".to_vec());
    assert_eq!(t2.get(b"2".to_vec()).unwrap(), b"20");
    t2.set(b"2".to_vec(), b"40".to_vec());
    assert!(t1.commit());
    assert!(!t2.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#lost-update-p4
fn test_lost_update() {
    let s = TestStorageBuilder::build();
    let mut t = s.begin();
    t.set(b"1".to_vec(), b"10".to_vec());
    t.set(b"2".to_vec(), b"20".to_vec());
    assert!(t.commit());
    let mut t1 = s.begin();
    let mut t2 = s.begin();
    assert_eq!(t1.get(b"1".to_vec()).unwrap(), b"10".to_vec());
    assert_eq!(t2.get(b"1".to_vec()).unwrap(), b"10".to_vec());
    t1.set(b"1".to_vec(), b"11".to_vec());
    t2.set(b"1".to_vec(), b"11".to_vec());
    assert!(t1.commit());
    assert!(!t2.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_read_only() {
    let s = TestStorageBuilder::build();
    let mut t = s.begin();
    t.set(b"1".to_vec(), b"10".to_vec());
    t.set(b"2".to_vec(), b"20".to_vec());
    assert!(t.commit());
    let t1 = s.begin();
    let mut t2 = s.begin();
    assert_eq!(t1.get(b"1".to_vec()).unwrap(), b"10");
    assert_eq!(t2.get(b"1".to_vec()).unwrap(), b"10");
    assert_eq!(t2.get(b"2".to_vec()).unwrap(), b"20");
    t2.set(b"1".to_vec(), b"12".to_vec());
    t2.set(b"2".to_vec(), b"18".to_vec());
    assert!(t2.commit());
    assert_eq!(t1.get(b"2".to_vec()).unwrap(), b"20");
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_predicate_dependencies() {
    let s = TestStorageBuilder::build();
    let mut t = s.begin();
    t.set(b"1".to_vec(), b"10".to_vec());
    t.set(b"2".to_vec(), b"20".to_vec());
    assert!(t.commit());
    let t1 = s.begin();
    let mut t2 = s.begin();
    assert_eq!(t1.get(b"1".to_vec()).unwrap(), b"10");
    assert_eq!(t1.get(b"2".to_vec()).unwrap(), b"20");
    t2.set(b"3".to_vec(), b"30".to_vec());
    assert!(t2.commit());
    assert!(t1.get(b"3".to_vec()).is_none());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#read-skew-g-single
fn test_read_skew_write_predicate() {
    let s = TestStorageBuilder::build();
    let mut t = s.begin();
    t.set(b"1".to_vec(), b"10".to_vec());
    t.set(b"2".to_vec(), b"20".to_vec());
    assert!(t.commit());
    let mut t1 = s.begin();
    let mut t2 = s.begin();
    assert_eq!(t1.get(b"1".to_vec()).unwrap(), b"10");
    assert_eq!(t2.get(b"1".to_vec()).unwrap(), b"10");
    assert_eq!(t2.get(b"2".to_vec()).unwrap(), b"20");
    t2.set(b"1".to_vec(), b"12".to_vec());
    t2.set(b"2".to_vec(), b"18".to_vec());
    assert!(t2.commit());
    t1.set(b"2".to_vec(), b"30".to_vec());
    assert!(!t1.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#write-skew-g2-item
fn test_write_skew() {
    let s = TestStorageBuilder::build();
    let mut t = s.begin();
    t.set(b"1".to_vec(), b"10".to_vec());
    t.set(b"2".to_vec(), b"20".to_vec());
    assert!(t.commit());
    let mut t1 = s.begin();
    let mut t2 = s.begin();
    assert_eq!(t1.get(b"1".to_vec()).unwrap(), b"10");
    assert_eq!(t1.get(b"2".to_vec()).unwrap(), b"20");
    assert_eq!(t2.get(b"1".to_vec()).unwrap(), b"10");
    assert_eq!(t2.get(b"2".to_vec()).unwrap(), b"20");
    t1.set(b"1".to_vec(), b"11".to_vec());
    t2.set(b"2".to_vec(), b"21".to_vec());
    assert!(t1.commit());
    assert!(t2.commit());
}

#[test]
// https://github.com/ept/hermitage/blob/master/sqlserver.md#anti-dependency-cycles-g2
fn test_anti_dependency_cycles() {
    let s = TestStorageBuilder::build();
    let mut t = s.begin();
    t.set(b"1".to_vec(), b"10".to_vec());
    t.set(b"2".to_vec(), b"20".to_vec());
    assert!(t.commit());
    let mut t1 = s.begin();
    let mut t2 = s.begin();
    t1.set(b"3".to_vec(), b"30".to_vec());
    t2.set(b"4".to_vec(), b"42".to_vec());
    assert!(t1.commit());
    assert!(t2.commit());
    let t3 = s.begin();
    assert_eq!(t3.get(b"3".to_vec()).unwrap(), b"30");
    assert_eq!(t3.get(b"4".to_vec()).unwrap(), b"42");
}
