pub mod raftpb {
    ///   Example RequestVote RPC arguments structure.
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
        pub last_log_index: u32,
        #[prost(uint64, tag = "2")]
        pub last_log_term: u64,
    }

    /// Google's Protobuf is shit, which cannot handle sum type well.
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct LogEntryProst {
        // ApplyMsg
        #[prost(bool, tag = "1")]
        pub is_command: bool,
        #[prost(uint64, tag = "2")]
        pub term: u64,

        #[prost(bytes, tag = "3")]
        pub data: Vec<u8>,

        // LogStateProst
        #[prost(uint32, tag = "4")]
        pub last_log_index: u32,
        #[prost(uint64, tag = "5")]
        pub last_log_term: u64,
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
    #[derive(Clone, PartialEq, prost::Message)]
    pub struct AppendEntriesArgs {
        #[prost(uint64, tag = "1")]
        pub term: u64,
        #[prost(uint64, tag = "2")]
        pub leader_id: u64,
        #[prost(message, optional, tag = "3")]
        pub log_state: Option<LogStateProst>,
        #[prost(message, repeated, tag = "4")]
        pub entries: Vec<LogEntryProst>,
        #[prost(uint64, tag = "5")]
        pub leader_commit_index: u64,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct AppendEntriesReply {
        #[prost(uint64, tag = "1")]
        pub term: u64,
        #[prost(bool, tag = "2")]
        pub success: bool,
    }

    labrpc::service! {
        service raft {
            rpc request_vote(RequestVoteArgs) returns (RequestVoteReply);

            rpc append_entries(AppendEntriesArgs) returns (AppendEntriesReply);

            // Your code here if more rpc desired.
            // rpc xxx(yyy) returns (zzz)
        }
    }
    pub use self::raft::{
        add_service as add_raft_service, Client as RaftClient, Service as RaftService,
    };
}

pub mod kvraftpb {
    include!(concat!(env!("OUT_DIR"), "/kvraftpb.rs"));

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
