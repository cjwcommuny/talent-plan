pub mod raftpb {
    ///   Example RequestVote RPC arguments structure.
    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct RequestVoteArgs {
        #[prost(message, optional, tag = "1")]
        pub log_state: Option<LogStateMessage>,
        #[prost(uint64, tag = "2")]
        pub term: u64,
        #[prost(uint32, tag = "3")]
        pub candidate_id: u32,
    }

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct LogStateMessage {
        #[prost(uint32, tag = "1")]
        pub last_log_index: u32,
        #[prost(uint64, tag = "2")]
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

    #[derive(Clone, PartialEq, Eq, prost::Message)]
    pub struct AppendEntriesArgs {
        #[prost(uint64, tag = "1")]
        pub term: u64,
        #[prost(uint64, tag = "2")]
        pub leader_id: u64,
        #[prost(uint32, tag = "3")]
        pub prev_log_index: u32,
        #[prost(uint64, tag = "4")]
        pub prev_log_term: u64,
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
