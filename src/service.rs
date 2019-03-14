#[derive(Clone, PartialEq, Message)]
pub struct Timestamp {
    #[prost(uint64, tag = "1")]
    pub ts: u64,
}

#[derive(Clone)]
pub struct TimestampService;

#[derive(Clone, PartialEq, Message)]
pub struct GetTimestamp {}

service! {
    service timestamp {
        rpc get_timestamp(GetTimestamp) returns (Timestamp);
    }
}

pub use timestamp::{add_service as add_tso_service, Client as TSOClient, Service};

#[derive(Clone, PartialEq, Message)]
pub struct GetRequest {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(bytes, tag = "2")]
    pub key: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetResponse {
    #[prost(bytes, tag = "1")]
    pub value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetRequest {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(bytes, tag = "2")]
    pub key: Vec<u8>,
    #[prost(bytes, tag = "3")]
    pub value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SetResponse {}

#[derive(Clone, PartialEq, Message)]
pub struct CommitRequest {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub commit_ts: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct CommitResponse {
    #[prost(bool, tag = "1")]
    pub res: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct BeginRequest {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(uint64, tag = "2")]
    pub start_ts: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct BeginResponse {}

service! {
    service transaction {
        rpc begin(BeginRequest) returns (BeginResponse);
        rpc get(GetRequest) returns (GetResponse);
        rpc set(SetRequest) returns (SetResponse);
        rpc commit(CommitRequest) returns (CommitResponse);
    }
}

pub use transaction::{add_service as add_transaction_service, Client as TransactionClient};
