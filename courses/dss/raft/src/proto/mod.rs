pub mod raftpb {
    ///   Example `RequestVote` RPC arguments structure.
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct RequestVoteArgs {
        #[prost(message, optional, tag = "1")]
        pub log_state: Option<LogStateProst>,
        #[prost(uint64, tag = "2")]
        pub term: u64,
        #[prost(uint32, tag = "3")]
        pub candidate_id: u32,
    }

    #[derive(Clone, PartialEq, Eq, Copy, prost::Message)]
    pub struct LogStateProst {
        #[prost(uint32, tag = "1")]
        pub index: u32,
        #[prost(uint64, tag = "2")]
        pub term: u64,
    }

    /// Google's Protobuf is shit, which cannot handle sum type well.
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct LogEntryProst {
        /// `LogKind`
        #[prost(bool, tag = "1")]
        pub is_command: bool,
        #[prost(bytes, tag = "2")]
        pub data: Vec<u8>,
        #[prost(uint64, tag = "3")]
        pub term: u64,
    }

    ///   Example RequestVote RPC reply structure.
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct RequestVoteReply {
        #[prost(uint64, tag = "1")]
        pub term: u64,
        #[prost(uint32, tag = "2")]
        pub node_id: u32,
        #[prost(bool, tag = "3")]
        pub vote_granted: bool,
    }

    /// if `log_length == 0`, then `prev_log_term == 0`.
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct AppendEntriesArgs {
        #[prost(uint64, tag = "1")]
        pub term: u64,
        #[prost(uint64, tag = "2")]
        pub leader_id: u64,

        #[prost(message, optional, tag = "3")]
        /// None if logs is empty
        pub log_state: Option<LogStateProst>,
        #[prost(message, repeated, tag = "4")]
        pub entries: Vec<LogEntryProst>,
        #[prost(uint64, tag = "5")]
        pub leader_commit_length: u64,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct AppendEntriesReply {
        #[prost(uint64, tag = "1")]
        pub term: u64,

        /// `Some(match_index)` if success, `None` else.
        /// Note: in the Raft paper, the algorithm returns `success: bool`, but actually
        /// we need to return the `match_index`
        #[prost(uint64, optional, tag = "2")]
        pub match_length: Option<u64>,
        #[prost(uint32, tag = "3")]
        pub node_id: u32,
    }

    labrpc::service! {
        service raft {
            rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);
            rpc append_entries(AppendEntriesArgs) returns (AppendEntriesReply);
        }
    }
    pub use self::raft::{
        add_service as add_raft_service, Client as RaftClient, Service as RaftService,
    };
}

pub mod kvraftpb {
    //// Put or Append
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
