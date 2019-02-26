extern crate jsonrpc_core;
use serde_json;

use std::collections::HashMap;

#[derive(Default, Debug)]
/// RPC instance.
pub struct Rpc {
    /// Underlying `IoHandler`.
    pub io: jsonrpc_core::IoHandler,
}

/// Encoding format.
pub enum Encoding {
    /// Encodes params using `serde::to_string`.
    Compact,
    /// Encodes params using `serde::to_string_pretty`.
    Pretty,
}

impl From<jsonrpc_core::IoHandler> for Rpc {
    fn from(io: jsonrpc_core::IoHandler) -> Self {
        Rpc { io }
    }
}

impl Rpc {
    /// Create a new RPC instance from a single delegate.
    pub fn new<D>(delegate: D) -> Self
    where
        D: Into<HashMap<String, jsonrpc_core::RemoteProcedure<()>>>,
    {
        let mut io = jsonrpc_core::IoHandler::new();
        io.extend_with(delegate);
        io.into()
    }

    /// Perform a single, synchronous method call and return pretty-printed value
    pub fn request(&self, method: &str) -> String {
        self.make_request(method, Encoding::Pretty)
    }

    /// Perform a single, synchronous method call.
    pub fn make_request(&self, method: &str, encoding: Encoding) -> String {
        use self::jsonrpc_core::types::response;

        let request = format!(
            "{{ \"jsonrpc\":\"2.0\", \"id\": 1, \"method\": \"{}\" }}",
            method,
        );

        let response = self
            .io
            .handle_request_sync(&request)
            .expect("We are sending a method call not notification.");

        // extract interesting part from the response

        match serde_json::from_str(&response).expect("We will always get a single output.") {
            response::Output::Success(response::Success { result, .. }) => match encoding {
                Encoding::Compact => serde_json::to_string(&result),
                Encoding::Pretty => serde_json::to_string_pretty(&result),
            },
            response::Output::Failure(response::Failure { error, .. }) => match encoding {
                Encoding::Compact => serde_json::to_string(&error),
                Encoding::Pretty => serde_json::to_string_pretty(&error),
            },
        }
        .expect("Serialization is infallible; qed")
    }

    pub fn get_timestamp(&self) -> u64 {
        let response = self.request("rpc_get_timestamp");
        let ts = response.parse::<u64>().unwrap();
        ts
    }
}
