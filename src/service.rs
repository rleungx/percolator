use crate::msg::{
    CommitRequest, CommitResponse, GetRequest, GetResponse, GetTimestamp, PrewriteRequest,
    PrewriteResponse, Timestamp,
};

service! {
    service timestamp {
        rpc get_timestamp(GetTimestamp) returns (Timestamp);
    }
}

pub use timestamp::{add_service as add_tso_service, Client as TSOClient, Service};

service! {
    service transaction {
        rpc get(GetRequest) returns (GetResponse);
        rpc prewrite(PrewriteRequest) returns (PrewriteResponse);
        rpc commit(CommitRequest) returns (CommitResponse);
    }
}

pub use transaction::{add_service as add_transaction_service, Client as TransactionClient};
