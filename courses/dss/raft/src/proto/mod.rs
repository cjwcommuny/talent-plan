pub mod raftpb {
    use serde::{Deserialize, Serialize};
    use std::fmt::Debug;

    ///   Example `RequestVote` RPC arguments structure.
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct RequestVoteArgsProst {
        #[prost(bytes, tag = "1")]
        pub data: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct RequestVoteReplyProst {
        #[prost(bytes, tag = "1")]
        pub data: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct AppendEntriesArgsProst {
        #[prost(bytes, tag = "1")]
        pub data: Vec<u8>,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct AppendEntriesReplyProst {
        #[prost(bytes, tag = "1")]
        pub data: Vec<u8>,
    }

    pub fn encode<T: Serialize + Debug>(value: &T) -> Vec<u8> {
        serde_json::to_vec(value).unwrap_or_else(|_| panic!("{}", "encode fail: {value:?}"))
    }

    pub fn decode<'a, T: Deserialize<'a>>(data: &'a [u8]) -> T {
        serde_json::from_slice(data).expect("decode fail")
    }

    labrpc::service! {
        service raft {
            rpc request_vote(RequestVoteArgsProst) returns (RequestVoteReplyProst);
            rpc append_entries(AppendEntriesArgsProst) returns (AppendEntriesReplyProst);
        }
    }
    pub use self::raft::{
        add_service as add_raft_service, Client as RaftClient, Service as RaftService,
    };
}

pub mod kvraftpb {
    /// Put or Append
    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct PutAppendRequest {
        #[prost(string, tag = "1")]
        pub key: std::string::String,
        #[prost(string, tag = "2")]
        pub value: std::string::String,
        /// "Put" or "Append"
        ///
        /// You'll have to add definitions here.
        #[prost(enumeration = "Op", tag = "3")]
        pub op: i32,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct PutAppendReply {
        #[prost(bool, tag = "1")]
        pub wrong_leader: bool,
        #[prost(string, tag = "2")]
        pub err: std::string::String,
    }
    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct GetRequest {
        /// You'll have to add definitions here.
        #[prost(string, tag = "1")]
        pub key: std::string::String,
    }
    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct GetReply {
        #[prost(bool, tag = "1")]
        pub wrong_leader: bool,
        #[prost(string, tag = "2")]
        pub err: std::string::String,
        #[prost(string, tag = "3")]
        pub value: std::string::String,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Op {
        Unknown = 0,
        Put = 1,
        Append = 2,
    }

    labrpc::service! {
        service kv {
            rpc get(GetRequest) returns (GetReply);
            rpc put_append(PutAppendRequest) returns (PutAppendReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)
        }
    }
    pub use self::kv::{add_service as add_kv_service, Client as KvClient, Service as KvService};
}
