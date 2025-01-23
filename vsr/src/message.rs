use std::sync::Arc;

use derive_more::derive::IsVariant;
use serde::{Deserialize, Serialize};

use crate::replica::{ClientTable, LogEntry};

/// A message that can be sent between replicas or between a client and a replica.
#[derive(Debug, Clone, Eq, PartialEq, IsVariant, Serialize, Deserialize)]
pub enum Message<Op, StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug, OpResult: Clone> {
    VsrMessage(VsrMessage<Op, StateMachineDelta, OpResult>),
    ClientResponse(ClientResponse<OpResult>),
    ClientRequest(RequestMsg<Op>),
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct VsrMessage<
    Op,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: Clone,
> {
    pub from: usize,
    pub to: usize,
    pub inner: VsrMessageInner<Op, StateMachineDelta, OpResult>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum VsrMessageInner<
    Op,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: Clone,
> {
    Commit(CommitMsg),
    Prepare(PrepareMsg<Op>),
    PrepareOk(PrepareOkMsg),
    StartViewChange(StartViewChangeMsg),
    DoViewChange(DoViewChangeMsg<Op, StateMachineDelta, OpResult>),
    StartView(StartViewMsg<Op, StateMachineDelta, OpResult>),
    GetState(GetStateMsg),
    NewState(NewStateMsg<Op, StateMachineDelta, OpResult>),
    Recovery(RecoveryMsg),
    RecoveryResponse(RecoveryResponseMsg<Op, StateMachineDelta, OpResult>),
}

// --------------------------------------------------------------------------------------------
// Normal Operation
// --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestMsg<Op> {
    pub operation: Arc<Op>,
    pub client_id: usize,
    pub request_num: usize,
    pub replica_id: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrepareMsg<Op> {
    pub view_number: usize,
    pub operation: Arc<Op>,
    pub client_id: usize,
    pub request_num: usize,
    pub op_number: usize,
    pub commit_number: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrepareOkMsg {
    pub view_number: usize,
    // this is the op number of the Prepare message that is being acknowledged
    pub ack_op_number: usize,
    // however, they could be received out of order. This is the highest op number
    // that the replica appended to its log
    pub log_op_number: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CommitMsg {
    pub view_number: usize,
    pub commit_number: usize,
}

// --------------------------------------------------------------------------------------------
// View Change
// --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StartViewChangeMsg {
    pub view_number: usize,
    pub commit_number: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Delta<Op, StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug, OpResult: Clone> {
    pub state_machine_delta: Option<StateMachineDelta>,
    pub log_delta: Vec<LogEntry<Op>>,
    pub client_table: ClientTable<OpResult>,
    pub commit_number: usize,
    pub start_op_number: usize,
    pub end_op_number: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct DoViewChangeMsg<
    Op,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: Clone,
> {
    pub view_number: usize,
    pub delta: Option<Delta<Op, StateMachineDelta, OpResult>>,
    pub view_number_latest_normal: usize,
    pub op_number: usize,
    pub commit_number: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StartViewMsg<
    Op,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: Clone,
> {
    pub delta: Option<Delta<Op, StateMachineDelta, OpResult>>,
    pub view_number: usize,
    pub commit_number: usize,
}

// --------------------------------------------------------------------------------------------
// State Transfer
// --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetStateMsg {
    pub view_number: usize,
    pub op_number: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewStateMsg<
    Op,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: Clone,
> {
    pub view_number: usize,
    pub op_number: usize,
    pub delta: Option<Delta<Op, StateMachineDelta, OpResult>>,
    pub commit_number: usize,
}

// --------------------------------------------------------------------------------------------
// Recovery
// --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoveryMsg {
    pub nonce: u64,
    pub commit_number: usize,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoveryResponseMsg<
    Op,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: Clone,
> {
    pub nonce: u64,
    pub view_number: usize,
    pub delta: Option<Delta<Op, StateMachineDelta, OpResult>>,
    pub op_number: Option<usize>,
    pub commit_number: Option<usize>,
}

// --------------------------------------------------------------------------------------------
// Client Response
// --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientResponse<OpResult> {
    pub client_id: usize,
    pub replica_id: usize,
    pub response: ClientResponseInner<OpResult>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum ClientResponseInner<OpResult> {
    Success {
        view_number: usize,
        request_num: usize,
        result: OpResult,
    },
    NotPrimary {
        primary: usize,
    },
    NotNormal,
}
