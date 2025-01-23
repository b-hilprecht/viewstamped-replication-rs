use std::{
    cmp,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    time::Instant,
};

use derive_more::IsVariant;

use crate::{message::PrepareMsg, Delta, RecoveryMsg, RecoveryResponseMsg, VsrMessageInner};

/// The status of a replica (one of Normal, ViewChange, StateTransfer, Recovering). Depending
/// on the status, the replica will record different information and respond to messages in
/// different ways. Note that in addition to the states described in the paper, we introduce
/// a StateTransfer state to handle the case where a replica is behind and needs to catch up.
/// This is not described in the paper, but is necessary for correctness as described in the
/// blog from Jack Vanlightly.
///
/// https://jack-vanlightly.com/analyses/2022/12/20/paper-vr-revisited-view-change-questions-part1
#[derive(Debug, Clone, PartialEq, Eq, IsVariant)]
pub(crate) enum Status<
    Op,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: PartialEq + Eq + Clone + std::fmt::Debug,
> {
    Normal(NormalStatus<Op>),
    ViewChange(ViewChange<Op, StateMachineDelta, OpResult>),
    StateTransfer(StateTransfer),
    Recovering(Recovering<Op, StateMachineDelta, OpResult>),
}

impl<Op, StateMachineDelta, OpResult> Status<Op, StateMachineDelta, OpResult>
where
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: PartialEq + Eq + Clone + std::fmt::Debug,
{
    pub(crate) fn view_number(&self) -> Option<usize> {
        match self {
            Status::Normal(normal_status) => Some(normal_status.view_number),
            Status::ViewChange(view_change) => Some(view_change.view_number),
            Status::StateTransfer(state_transfer) => Some(state_transfer.view_number),
            Status::Recovering(_) => None,
        }
    }

    pub(crate) fn view_number_latest_normal(&self) -> usize {
        match &self {
            Status::Normal(normal_status) => normal_status.view_number,
            Status::ViewChange(view_change) => view_change.view_number_latest_normal,
            Status::StateTransfer(state_transfer) => state_transfer.view_number_latest_normal,
            Status::Recovering(_) => 0,
        }
    }

    pub(crate) fn accept_non_normal_status(&self, recv_view_number: usize) -> bool {
        let Some(view_number) = self.view_number() else {
            return false;
        };

        view_number < recv_view_number
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InflightPrepare<Op> {
    pub(crate) time_sent: Instant,
    pub(crate) replica_id: usize,
    pub(crate) prepare_msg: PrepareMsg<Op>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PotentialNewLog<
    Op,
    StateMachineDelta: Clone + Eq + PartialEq + std::fmt::Debug,
    OpResult: Clone + Eq + PartialEq + std::fmt::Debug,
> {
    pub(crate) delta: Option<Delta<Op, StateMachineDelta, OpResult>>,
    view_number_latest_normal: usize,
    op_number: usize,
}

impl<Op, StateMachineDelta, OpResult> Ord for PotentialNewLog<Op, StateMachineDelta, OpResult>
where
    Op: Eq + PartialEq,
    StateMachineDelta: Clone + Eq + PartialEq + std::fmt::Debug,
    OpResult: Clone + Eq + PartialEq + std::fmt::Debug,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self
            .view_number_latest_normal
            .cmp(&other.view_number_latest_normal)
        {
            std::cmp::Ordering::Equal => self.op_number.cmp(&other.op_number),
            ordering => ordering,
        }
    }
}

impl<Op, StateMachineDelta, OpResult> PartialOrd
    for PotentialNewLog<Op, StateMachineDelta, OpResult>
where
    Op: Eq + PartialEq,
    StateMachineDelta: Clone + Eq + PartialEq + std::fmt::Debug,
    OpResult: Clone + Eq + PartialEq + std::fmt::Debug,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalStatus<Op> {
    pub(crate) view_number: usize,

    // Primary data structures
    // highest op_number applied to the log of the peer
    peer_log_op_numbers: Vec<usize>,
    // out of order acks received for prepares
    acknowledged_op_numbers: Vec<BTreeSet<usize>>,

    // prepare messages sent by the primary that have not yet been replied back with ok
    pub(crate) inflight_prepares: VecDeque<InflightPrepare<Op>>,

    // Backup data structures
    // prepare messages received by a backup that could not yet be replied back with ok
    // e.g., because they arrived out of order
    pub(crate) pending_prepares: BTreeMap<usize, PrepareMsg<Op>>,
    // time of last heartbeat from the primary
    pub(crate) last_heartbeat: Instant,
}

impl<Op> NormalStatus<Op>
where
    Op: Eq + PartialEq,
{
    pub(crate) fn new(now: Instant, replica_count: usize, view_number: usize) -> Self {
        Self {
            view_number,
            last_heartbeat: now,
            peer_log_op_numbers: vec![0; replica_count],
            acknowledged_op_numbers: vec![BTreeSet::new(); replica_count],
            inflight_prepares: Default::default(),
            pending_prepares: Default::default(),
        }
    }

    pub(crate) fn acknowledge_prepare(
        &mut self,
        replica_id: usize,
        ack_op_number: usize,
        log_op_number: usize,
    ) -> bool {
        let mut change_quorum = false;
        if log_op_number > self.peer_log_op_numbers[replica_id] {
            change_quorum = true;
            self.peer_log_op_numbers[replica_id] = log_op_number;

            // keep only out of order acks (i.e., acks for prepares not yet applied to the log)
            let remaining_ack_op_numbers = self.acknowledged_op_numbers[replica_id]
                .split_off(&(self.peer_log_op_numbers[replica_id] + 1));
            self.acknowledged_op_numbers[replica_id] = remaining_ack_op_numbers;
        }

        if self.acknowledged_op_numbers[replica_id].contains(&ack_op_number) {
            return change_quorum;
        }

        self.acknowledged_op_numbers[replica_id].insert(ack_op_number);
        while self.acknowledged_op_numbers[replica_id]
            .remove(&(self.peer_log_op_numbers[replica_id] + 1))
        {
            change_quorum = true;
            self.peer_log_op_numbers[replica_id] += 1;
        }
        change_quorum
    }

    pub(crate) fn quorum_op_number(
        &self,
        replica_id: usize,
        replica_count: usize,
        op_number: usize,
    ) -> usize {
        let mut peer_op_number = self.peer_log_op_numbers.clone();
        peer_op_number[replica_id] = op_number;
        peer_op_number.sort();
        peer_op_number[replica_count / 2]
    }

    pub(crate) fn acknowledged_op_number(&self, replica_id: usize, op_number: usize) -> bool {
        if op_number <= self.peer_log_op_numbers[replica_id] {
            return true;
        }
        self.acknowledged_op_numbers[replica_id].contains(&op_number)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ViewChangeStatus {
    StartViewChange,
    DoViewChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ViewChange<
    Op,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: PartialEq + Eq + Clone + std::fmt::Debug,
> {
    pub(crate) start_time: Instant,
    pub(crate) view_number: usize,
    pub(crate) view_number_latest_normal: usize,
    start_view_change_received: HashSet<usize>,
    // peer number -> commit number
    pub(crate) do_view_change_received: HashMap<usize, usize>,
    pub(crate) primary_commit_number: Option<usize>,
    pub(crate) potential_new_log: Option<PotentialNewLog<Op, StateMachineDelta, OpResult>>,
    pub(crate) largest_commit_number: usize,
    replica_count: usize,
    status: ViewChangeStatus,
}

impl<Op, StateMachineDelta, OpResult> ViewChange<Op, StateMachineDelta, OpResult>
where
    Op: Eq + PartialEq,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: PartialEq + Eq + Clone + std::fmt::Debug,
{
    pub(crate) fn new(
        start_time: Instant,
        view_number: usize,
        view_number_latest_normal: usize,
        replica_id: usize,
        replica_count: usize,
        commit_number: usize,
    ) -> Self {
        let mut primary_commit_number = None;
        if replica_id == Self::primary(view_number, replica_count) {
            primary_commit_number = Some(commit_number);
        }

        ViewChange {
            start_time,
            view_number,
            view_number_latest_normal,
            start_view_change_received: Default::default(),
            do_view_change_received: Default::default(),
            primary_commit_number,
            potential_new_log: None,
            largest_commit_number: 0,
            replica_count,
            status: ViewChangeStatus::StartViewChange,
        }
    }

    pub(crate) fn register_start_view_change(
        &mut self,
        from_replica_id: usize,
        commit_number: usize,
    ) {
        if from_replica_id == Self::primary(self.view_number, self.replica_count) {
            self.primary_commit_number = Some(commit_number);
        }

        self.start_view_change_received.insert(from_replica_id);
    }

    pub(crate) fn should_send_do_view_change(&mut self) -> bool {
        if self.status != ViewChangeStatus::StartViewChange {
            return false;
        }

        let should = self.start_view_change_received.len() >= self.replica_count / 2
            && self.primary_commit_number.is_some();
        if should {
            self.status = ViewChangeStatus::DoViewChange;
        }
        should
    }

    pub(crate) fn register_do_view_change(
        &mut self,
        from_replica_id: usize,
        delta: Option<Delta<Op, StateMachineDelta, OpResult>>,
        view_number_latest_normal: usize,
        commit_number: usize,
        op_number: usize,
    ) {
        self.do_view_change_received
            .insert(from_replica_id, commit_number);

        self.largest_commit_number = cmp::max(self.largest_commit_number, commit_number);

        let potential_new_log = PotentialNewLog {
            delta,
            view_number_latest_normal,
            op_number,
        };
        if self.potential_new_log.is_none()
            || potential_new_log.cmp(self.potential_new_log.as_ref().unwrap())
                == std::cmp::Ordering::Greater
        {
            self.potential_new_log = Some(potential_new_log);
        }
    }

    pub(crate) fn new_primary(&self) -> usize {
        Self::primary(self.view_number, self.replica_count)
    }

    fn primary(view_number: usize, replica_count: usize) -> usize {
        assert!(replica_count > 0);
        (view_number - 1) % replica_count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateTransfer {
    pub(crate) view_number: usize,
    view_number_latest_normal: usize,
    pub(crate) last_sent: Instant,
}

impl StateTransfer {
    pub(crate) fn new(now: Instant, view_number: usize, view_number_latest_normal: usize) -> Self {
        Self {
            view_number,
            view_number_latest_normal,
            last_sent: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Recovering<
    Op,
    StateMachineDelta: Clone + Eq + PartialEq + std::fmt::Debug,
    OpResult: PartialEq + Eq + Clone + std::fmt::Debug,
> {
    pub(crate) last_sent: Instant,
    nonce: u64,
    replica_count: usize,

    pub(crate) highest_known_view_number: usize,
    acked_view_number: HashSet<usize>,

    pub(crate) potential_delta: Option<Delta<Op, StateMachineDelta, OpResult>>,
    potential_op_number: usize,
    pub(crate) potential_commit_number: usize,
}

impl<Op, StateMachineDelta, OpResult> Recovering<Op, StateMachineDelta, OpResult>
where
    Op: Eq + PartialEq,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: PartialEq + Eq + Clone + std::fmt::Debug,
{
    pub(crate) fn new(nonce: u64, now: Instant, replica_count: usize) -> Self {
        Self {
            replica_count,
            last_sent: now,
            nonce,
            highest_known_view_number: 0,
            acked_view_number: Default::default(),
            potential_delta: Default::default(),
            potential_op_number: 0,
            potential_commit_number: 0,
        }
    }

    pub(crate) fn recovery_msg(&self) -> VsrMessageInner<Op, StateMachineDelta, OpResult> {
        VsrMessageInner::Recovery(RecoveryMsg {
            nonce: self.nonce,
            // for now we assume that no state survives a recovery
            commit_number: 0,
        })
    }

    pub(crate) fn register_response(
        &mut self,
        from: usize,
        recovery_response_msg: RecoveryResponseMsg<Op, StateMachineDelta, OpResult>,
    ) {
        let RecoveryResponseMsg {
            nonce,
            view_number,
            delta,
            op_number,
            commit_number,
        } = recovery_response_msg;

        if nonce != self.nonce {
            return;
        }

        match view_number.cmp(&self.highest_known_view_number) {
            std::cmp::Ordering::Less => return,
            std::cmp::Ordering::Equal => {}
            std::cmp::Ordering::Greater => {
                self.highest_known_view_number = view_number;
                self.acked_view_number.clear();
            }
        }

        self.acked_view_number.insert(from);
        if let Some(delta) = delta {
            self.potential_delta = Some(delta);
            self.potential_op_number = op_number.unwrap();
            self.potential_commit_number = commit_number.unwrap();
        }
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.acked_view_number.len() > self.replica_count / 2 && self.potential_delta.is_some()
    }
}
