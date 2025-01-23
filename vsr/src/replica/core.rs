use std::{
    cmp,
    time::{Duration, Instant},
};

use crate::{
    message::{ClientResponse, PrepareMsg, PrepareOkMsg, RequestMsg},
    replica::status::StateTransfer,
    state_machine::StateMachine,
    ClientResponseInner, CommitMsg, DoViewChangeMsg, GetStateMsg, Message, NewStateMsg,
    RecoveryMsg, RecoveryResponseMsg, StartViewChangeMsg, StartViewMsg, VsrMessage,
    VsrMessageInner,
};

use super::{
    client_table::ClientTable,
    log::{Log, LogEntry},
    status::NormalStatus,
    status::{InflightPrepare, Recovering, Status, ViewChange},
};

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    pub prepare_retry_timeout: Duration,
    pub heartbeat_interval: Duration,
    pub view_change_timeout: Duration,
    pub state_transfer_timeout: Duration,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        Self {
            prepare_retry_timeout: Duration::from_millis(100),
            heartbeat_interval: Duration::from_millis(100),
            view_change_timeout: Duration::from_millis(500),
            state_transfer_timeout: Duration::from_millis(500),
        }
    }
}

/// A replica in the viewstamped replication protocol.
#[derive(Debug)]
pub struct Replica<
    Op: Clone,
    S: StateMachine<Op, StateMachineDelta, OpResult> + std::fmt::Debug,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: PartialEq + Eq + Clone + std::fmt::Debug,
> {
    status: Status<Op, StateMachineDelta, OpResult>,
    replica_count: usize,
    replica_id: usize,
    log: Log<Op, S, StateMachineDelta, OpResult>,
    config: ReplicaConfig,
}

impl<Op, S, StateMachineDelta, OpResult> Replica<Op, S, StateMachineDelta, OpResult>
where
    Op: Clone + Eq + PartialEq + std::fmt::Debug,
    S: StateMachine<Op, StateMachineDelta, OpResult> + std::fmt::Debug,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: PartialEq + Eq + Clone + std::fmt::Debug,
{
    pub fn new(
        now: Instant,
        replica_count: usize,
        replica_id: usize,
        config: ReplicaConfig,
    ) -> Self {
        Replica {
            status: Status::Normal(NormalStatus::new(now, replica_count, 1)),
            replica_count,
            replica_id,
            log: Log::new(replica_id),
            config,
        }
    }

    // Instantiates a new replica in the recovering state. For now recovering replicas cannot
    // consume state previously stored in the state machine. Instead, they are initialized
    // with an empty state machine.
    pub fn new_recovering(
        now: Instant,
        nonce: u64,
        replica_count: usize,
        replica_id: usize,
        config: ReplicaConfig,
    ) -> Self {
        Replica {
            status: Status::Recovering(Recovering::new(nonce, now, replica_count)),
            replica_count,
            replica_id,
            log: Log::new(replica_id),
            config,
        }
    }

    pub fn id(&self) -> usize {
        self.replica_id
    }

    pub fn commit_number(&self) -> usize {
        self.log.commit_number()
    }

    pub fn log(&self) -> &Log<Op, S, StateMachineDelta, OpResult> {
        &self.log
    }

    pub fn client_table(&self) -> &ClientTable<OpResult> {
        self.log.client_table()
    }

    pub fn is_recovering(&self) -> bool {
        self.status.is_recovering()
    }

    pub fn config(&self) -> &ReplicaConfig {
        &self.config
    }

    /// Processes a message received by another replica or client and returns a list of messages
    /// that should be sent in response. The current time is also provided to the replica to allow
    /// a deterministic implementation.
    pub fn process_message(
        &mut self,
        msg: Message<Op, StateMachineDelta, OpResult>,
        now: Instant,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        match msg {
            Message::VsrMessage(VsrMessage { from, to: _, inner }) => {
                if Some(from) == self.primary() {
                    if let Status::Normal(status) = &mut self.status {
                        status.last_heartbeat = now;
                    }
                }

                match inner {
                    VsrMessageInner::Commit(commit_msg) => {
                        self.handle_commit(now, from, commit_msg)
                    }
                    VsrMessageInner::Prepare(prepare_msg) => {
                        self.handle_prepare(now, from, prepare_msg)
                    }
                    VsrMessageInner::PrepareOk(prepare_ok_msg) => {
                        self.handle_prepare_ok(from, prepare_ok_msg)
                    }
                    VsrMessageInner::StartViewChange(start_view_change_msg) => {
                        self.handle_start_view_change(now, from, start_view_change_msg)
                    }
                    VsrMessageInner::DoViewChange(do_view_change_msg) => {
                        self.handle_do_view_change(now, from, do_view_change_msg)
                    }
                    VsrMessageInner::StartView(start_view_msg) => {
                        self.handle_start_view(now, from, start_view_msg)
                    }
                    VsrMessageInner::GetState(get_state_msg) => {
                        self.handle_get_state(from, get_state_msg)
                    }
                    VsrMessageInner::NewState(new_state_msg) => {
                        self.handle_new_state(now, new_state_msg)
                    }
                    VsrMessageInner::Recovery(recovery_msg) => {
                        self.handle_recovery(from, recovery_msg)
                    }
                    VsrMessageInner::RecoveryResponse(recovery_response_msg) => {
                        self.handle_recovery_response(from, recovery_response_msg)
                    }
                }
            }
            Message::ClientRequest(request_msg) => self.handle_request(now, request_msg),
            Message::ClientResponse(_) => unreachable!(),
        }
    }

    // --------------------------------------------------------------------------------------------
    // Normal Operation
    // --------------------------------------------------------------------------------------------

    fn handle_request(
        &mut self,
        now: Instant,
        request_msg: RequestMsg<Op>,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let RequestMsg {
            operation,
            client_id,
            request_num,
            replica_id: _,
        } = request_msg;

        if !self.is_primary() {
            if let Some(primary) = self.primary() {
                return vec![Message::ClientResponse(ClientResponse {
                    client_id,
                    replica_id: self.replica_id,
                    response: ClientResponseInner::NotPrimary { primary },
                })];
            };
        }

        let Status::Normal(normal_status) = &mut self.status else {
            return vec![Message::ClientResponse(ClientResponse {
                client_id,
                replica_id: self.replica_id,
                response: ClientResponseInner::NotNormal,
            })];
        };

        if let Some(response) =
            self.log
                .new_client_request(client_id, normal_status.view_number, request_num)
        {
            return response;
        }

        // execute request
        self.log
            .push_entry_primary(operation.clone(), request_num, client_id);

        normal_status.last_heartbeat = now;
        let view_number = normal_status.view_number;
        self.broadcast(
            now,
            VsrMessageInner::Prepare(PrepareMsg {
                view_number,
                operation,
                client_id,
                request_num,
                op_number: self.log.op_number(),
                commit_number: self.log.commit_number(),
            }),
        )
    }

    fn handle_prepare(
        &mut self,
        now: Instant,
        from: usize,
        prepare_msg: PrepareMsg<Op>,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Status::Normal(normal_status) = &mut self.status else {
            return vec![];
        };

        if let Some(existing_msg) = normal_status.pending_prepares.get(&prepare_msg.op_number) {
            if existing_msg.view_number > prepare_msg.view_number {
                return vec![];
            }
        }

        match prepare_msg.view_number.cmp(&normal_status.view_number) {
            cmp::Ordering::Less => return vec![],
            cmp::Ordering::Equal => {}
            cmp::Ordering::Greater => {
                return self.init_state_transfer(now, from, prepare_msg.view_number)
            }
        }

        let response = PrepareOkMsg {
            view_number: normal_status.view_number,
            ack_op_number: prepare_msg.op_number,
            log_op_number: self.log.op_number(),
        };

        if prepare_msg.op_number > self.log.op_number()
            || prepare_msg.commit_number > self.log.commit_number()
        {
            normal_status
                .pending_prepares
                .insert(prepare_msg.op_number, prepare_msg);
            self.advance_log();
        }

        vec![Message::VsrMessage(VsrMessage {
            from: self.replica_id,
            to: from,
            inner: VsrMessageInner::PrepareOk(response),
        })]
    }

    fn handle_prepare_ok(
        &mut self,
        replica_id: usize,
        prepare_ok_msg: PrepareOkMsg,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let PrepareOkMsg {
            view_number,
            ack_op_number,
            log_op_number,
        } = prepare_ok_msg;

        if !self.is_primary() || self.status.view_number() != Some(view_number) {
            return vec![];
        }

        let Status::Normal(normal_status) = &mut self.status else {
            return vec![];
        };

        let check_quorum =
            normal_status.acknowledge_prepare(replica_id, ack_op_number, log_op_number);
        if !check_quorum {
            return vec![];
        }

        // advance commit number
        let quorum_op_number = normal_status.quorum_op_number(
            self.replica_id,
            self.replica_count,
            self.log.op_number(),
        );
        self.log
            .execute_commits_up_to(quorum_op_number, self.status.view_number().unwrap())
    }

    fn handle_commit(
        &mut self,
        now: Instant,
        from: usize,
        commit: CommitMsg,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let CommitMsg {
            view_number,
            commit_number,
        } = commit;

        let Some(current_view_number) = self.status.view_number() else {
            return vec![];
        };
        if view_number > current_view_number {
            self.init_state_transfer(now, from, view_number);
        }

        let Status::Normal(normal_status) = &mut self.status else {
            return vec![];
        };
        if view_number != normal_status.view_number {
            return vec![];
        }

        normal_status.last_heartbeat = now;
        self.log
            .execute_commits_up_to(commit_number, self.status.view_number().unwrap());

        vec![]
    }

    fn advance_log(&mut self) {
        let mut last_log_op_number = self.log.op_number();

        let Status::Normal(normal_status) = &mut self.status else {
            return;
        };

        let mut new_commit_number = self.log.commit_number();
        while let Some((_, prep_msg)) = normal_status.pending_prepares.pop_first() {
            if prep_msg.view_number != normal_status.view_number {
                continue;
            }

            // op_number matches
            if prep_msg.op_number <= last_log_op_number {
                continue;
            } else if prep_msg.op_number > last_log_op_number + 1 {
                normal_status
                    .pending_prepares
                    .insert(prep_msg.op_number, prep_msg);
                return;
            }

            let PrepareMsg {
                view_number: _,
                operation,
                client_id,
                request_num,
                op_number,
                commit_number,
            } = prep_msg;

            new_commit_number = cmp::max(commit_number, self.log.commit_number());

            self.log.push_entry_backup(LogEntry {
                op_number,
                operation,
                request_num,
                client_id,
            });
            last_log_op_number = self.log.op_number()
        }
        self.log
            .execute_commits_up_to(new_commit_number, self.status.view_number().unwrap());
    }

    // --------------------------------------------------------------------------------------------
    // View Change
    // --------------------------------------------------------------------------------------------

    fn handle_start_view_change(
        &mut self,
        now: Instant,
        from_replica_id: usize,
        start_view_change_msg: StartViewChangeMsg,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let StartViewChangeMsg {
            view_number,
            commit_number: recv_commit_number,
        } = start_view_change_msg;
        if !self.accept_view_change(view_number) {
            return vec![];
        }
        let mut messages = self.init_view_change(now, view_number);
        let Status::ViewChange(view_change) = &mut self.status else {
            // means we are in state transfer
            return vec![];
        };

        view_change.register_start_view_change(from_replica_id, recv_commit_number);
        if view_change.should_send_do_view_change() {
            let delta = self
                .log
                .get_delta(view_change.primary_commit_number.unwrap());
            messages.push(Message::VsrMessage(VsrMessage {
                from: self.replica_id,
                to: view_change.new_primary(),
                inner: VsrMessageInner::DoViewChange(DoViewChangeMsg {
                    view_number: view_change.view_number,
                    delta,
                    view_number_latest_normal: view_change.view_number_latest_normal,
                    op_number: self.log.op_number(),
                    commit_number: self.log.commit_number(),
                }),
            }));
        }
        messages
    }

    fn handle_do_view_change(
        &mut self,
        now: Instant,
        from_replica_id: usize,
        do_view_change_msg: DoViewChangeMsg<Op, StateMachineDelta, OpResult>,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let DoViewChangeMsg {
            view_number,
            delta,
            view_number_latest_normal,
            op_number,
            commit_number,
        } = do_view_change_msg;

        if !self.accept_view_change(view_number) {
            return vec![];
        }

        let mut messages = self.init_view_change(now, view_number);
        let Status::ViewChange(view_change) = &mut self.status else {
            // means we are in state transfer
            return vec![];
        };

        view_change.register_do_view_change(
            from_replica_id,
            delta,
            view_number_latest_normal,
            commit_number,
            op_number,
        );

        if view_change.do_view_change_received.len() == self.replica_count / 2 + 1 {
            let new_log = view_change.potential_new_log.take().unwrap();

            if let Some(delta) = new_log.delta {
                self.log.apply_delta(delta);
            }
            messages.extend(
                self.log
                    .execute_commits_up_to(view_change.largest_commit_number, view_number),
            );

            let min_commit_number = *view_change.do_view_change_received.values().min().unwrap();
            for replica_id in 0..self.replica_count {
                if replica_id == self.replica_id {
                    continue;
                }

                let log_number = match view_change.do_view_change_received.get(&replica_id) {
                    Some(log_number) => *log_number,
                    None => min_commit_number,
                };
                let delta = self.log.get_delta(log_number);

                messages.push(Message::VsrMessage(VsrMessage {
                    from: self.replica_id,
                    to: replica_id,
                    inner: VsrMessageInner::StartView(StartViewMsg {
                        delta,
                        view_number,
                        commit_number: self.log.commit_number(),
                    }),
                }));
            }

            self.status = Status::Normal(NormalStatus::new(now, self.replica_count, view_number));

            // Make sure we obtain prepared OK messages for log entries that have not yet been
            // committed. Usually we receive these as a response to StartView messages, but they
            // could get lost. So by registering those as inflight, we make sure they get resent.
            let Status::Normal(normal_status) = &mut self.status else {
                unreachable!()
            };
            if let Some(last_log_entry) = self.log.last_uncommitted() {
                for replica_id in 0..self.replica_count {
                    if replica_id == self.replica_id {
                        continue;
                    }
                    normal_status.inflight_prepares.push_back(InflightPrepare {
                        time_sent: now,
                        replica_id,
                        prepare_msg: PrepareMsg {
                            view_number: normal_status.view_number,
                            operation: last_log_entry.operation.clone(),
                            client_id: last_log_entry.client_id,
                            request_num: last_log_entry.request_num,
                            op_number: self.log.op_number(),
                            commit_number: self.log.commit_number(),
                        },
                    });
                }
            }
        }

        messages
    }

    fn handle_start_view(
        &mut self,
        now: Instant,
        from: usize,
        start_view_msg: StartViewMsg<Op, StateMachineDelta, OpResult>,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let StartViewMsg {
            delta,
            view_number,
            commit_number,
        } = start_view_msg;

        if !self.accept_view_change(view_number) {
            return vec![];
        }

        // new information for us. Check if the delta works
        if self.commit_number() < commit_number {
            match &delta {
                Some(delta) => {
                    if delta.start_op_number > self.log().commit_number() {
                        return self.init_state_transfer_unchecked(now, from, view_number);
                    }
                }
                None => return vec![],
            }
        }

        if let Some(delta) = delta {
            self.log.apply_delta(delta);
            // Will not accept that committed logs get lost
            assert!(
                self.log.commit_number() <= self.log.op_number(),
                "replica id: {:#?} received start view message with largest op number {} but has commit number: {} (current view number: {:#?}, incoming view number: {:#?})",
                self.replica_id,
                self.log.op_number(),
                self.log.commit_number(),
                self.status.view_number(),
                view_number
            );
        }

        self.log.execute_commits_up_to(commit_number, view_number);
        self.status = Status::Normal(NormalStatus::new(
            Instant::now(),
            self.replica_count,
            view_number,
        ));

        let mut messages = vec![];
        if self.log.last_uncommitted().is_some() {
            messages = vec![Message::VsrMessage(VsrMessage {
                from: self.replica_id,
                to: self.primary().unwrap(),
                inner: VsrMessageInner::PrepareOk(PrepareOkMsg {
                    view_number,
                    ack_op_number: self.log.op_number(),
                    log_op_number: self.log.op_number(),
                }),
            })];
        }
        messages
    }

    fn init_view_change(
        &mut self,
        now: Instant,
        new_view_number: usize,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        if !self.status.accept_non_normal_status(new_view_number) {
            return vec![];
        }

        self.status = Status::ViewChange(ViewChange::new(
            now,
            new_view_number,
            self.status.view_number_latest_normal(),
            self.replica_id,
            self.replica_count,
            self.log.commit_number(),
        ));

        self.broadcast(
            now,
            VsrMessageInner::StartViewChange(StartViewChangeMsg {
                view_number: new_view_number,
                commit_number: self.log.commit_number(),
            }),
        )
    }

    fn accept_view_change(&self, view_number: usize) -> bool {
        let Some(current_view_number) = self.status.view_number() else {
            assert!(self.status.is_recovering());
            return false;
        };

        view_number > current_view_number
            || (view_number == current_view_number && self.status.is_view_change())
    }

    // --------------------------------------------------------------------------------------------
    // State Transfer
    // --------------------------------------------------------------------------------------------

    fn handle_get_state(
        &mut self,
        from: usize,
        get_state_msg: GetStateMsg,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Some(current_view_number) = self.status.view_number() else {
            return vec![];
        };
        if !(current_view_number >= get_state_msg.view_number && self.status.is_normal()) {
            return vec![];
        }
        if self.log.op_number() < get_state_msg.op_number {
            return vec![];
        }

        let new_state_msg = NewStateMsg {
            view_number: current_view_number,
            op_number: self.log.op_number(),
            delta: self.log.get_delta(get_state_msg.op_number),
            commit_number: self.log.commit_number(),
        };
        vec![Message::VsrMessage(VsrMessage {
            from: self.replica_id,
            to: from,
            inner: VsrMessageInner::NewState(new_state_msg),
        })]
    }

    fn handle_new_state(
        &mut self,
        now: Instant,
        new_state_msg: NewStateMsg<Op, StateMachineDelta, OpResult>,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Status::StateTransfer(state_transfer_state) = &self.status else {
            return vec![];
        };

        if new_state_msg.view_number < state_transfer_state.view_number {
            return vec![];
        }

        self.status = Status::Normal(NormalStatus::new(
            now,
            self.replica_count,
            new_state_msg.view_number,
        ));
        if let Some(delta) = new_state_msg.delta {
            self.log.apply_delta(delta);
        } else {
            self.log.truncate_uncommitted();
        }

        vec![]
    }

    fn init_state_transfer(
        &mut self,
        now: Instant,
        from: usize,
        new_view_number: usize,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        if !self.status.accept_non_normal_status(new_view_number) {
            return vec![];
        }

        self.init_state_transfer_unchecked(now, from, new_view_number)
    }

    fn init_state_transfer_unchecked(
        &mut self,
        now: Instant,
        from: usize,
        new_view_number: usize,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        self.status = Status::StateTransfer(StateTransfer::new(
            now,
            new_view_number,
            self.status.view_number_latest_normal(),
        ));
        vec![Message::VsrMessage(VsrMessage {
            from: self.replica_id,
            to: from,
            inner: VsrMessageInner::GetState(GetStateMsg {
                view_number: new_view_number,
                op_number: self.log.commit_number(),
            }),
        })]
    }

    // --------------------------------------------------------------------------------------------
    // Recovery
    // --------------------------------------------------------------------------------------------

    fn handle_recovery(
        &self,
        from: usize,
        recovery_msg: RecoveryMsg,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Status::Normal(normal_status) = &self.status else {
            return vec![];
        };

        let response_msg = match self.is_primary() {
            true => RecoveryResponseMsg {
                nonce: recovery_msg.nonce,
                view_number: normal_status.view_number,
                delta: self.log.get_delta(recovery_msg.commit_number),
                op_number: Some(self.log.op_number()),
                commit_number: Some(self.log.commit_number()),
            },
            false => RecoveryResponseMsg {
                nonce: recovery_msg.nonce,
                view_number: normal_status.view_number,
                delta: None,
                op_number: None,
                commit_number: None,
            },
        };

        vec![Message::VsrMessage(VsrMessage {
            from: self.replica_id,
            to: from,
            inner: VsrMessageInner::RecoveryResponse(response_msg),
        })]
    }

    fn handle_recovery_response(
        &mut self,
        from: usize,
        recovery_response_msg: RecoveryResponseMsg<Op, StateMachineDelta, OpResult>,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let new_view_number;
        {
            let Status::Recovering(recovering_status) = &mut self.status else {
                return vec![];
            };

            recovering_status.register_response(from, recovery_response_msg);
            new_view_number = recovering_status.highest_known_view_number;
            if !recovering_status.is_ready() {
                return vec![];
            }
        }

        let previous_status = std::mem::replace(
            &mut self.status,
            Status::Normal(NormalStatus::new(
                Instant::now(),
                self.replica_count,
                new_view_number,
            )),
        );

        let Status::Recovering(Recovering {
            potential_delta,
            potential_commit_number,
            highest_known_view_number,
            ..
        }) = previous_status
        else {
            unreachable!();
        };

        let delta = potential_delta.unwrap();
        self.log.apply_delta(delta);
        self.log
            .execute_commits_up_to(potential_commit_number, highest_known_view_number);
        vec![]
    }

    // --------------------------------------------------------------------------------------------
    // Utility
    // --------------------------------------------------------------------------------------------

    fn broadcast(
        &mut self,
        now: Instant,
        msg: VsrMessageInner<Op, StateMachineDelta, OpResult>,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let mut messages = Vec::with_capacity(self.replica_count);

        for replica_id in 0..self.replica_count {
            if replica_id == self.replica_id {
                continue;
            }

            if let VsrMessageInner::Prepare(msg) = &msg {
                if let Status::Normal(normal_status) = &mut self.status {
                    normal_status.inflight_prepares.push_back(InflightPrepare {
                        time_sent: now,
                        replica_id,
                        prepare_msg: msg.clone(),
                    });
                };
            }

            messages.push(Message::VsrMessage(VsrMessage {
                from: self.replica_id,
                to: replica_id,
                inner: msg.clone(),
            }));
        }
        messages
    }

    fn primary(&self) -> Option<usize> {
        let current_view_number = self.status.view_number()?;
        let primary_idx = (current_view_number - 1) % self.replica_count;
        Some(primary_idx)
    }

    fn is_primary(&self) -> bool {
        Some(self.replica_id) == self.primary()
    }

    // --------------------------------------------------------------------------------------------
    // Ticks (Periodic Actions)
    // --------------------------------------------------------------------------------------------

    /// Ticks the replica, performing periodic actions and returning any messages that need to be
    /// sent. The current time is also provided to the replica to enable a deterministic implementation.
    pub fn tick(&mut self, now: Instant) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        match (&self.status, self.is_primary()) {
            (Status::Normal(_), true) => {
                return self.tick_primary(now);
            }
            (Status::Normal(normal_status), false) => {
                // check if primary is still alive or we did not receive prepare ok messages
                // from clients (this could indicate that we are not primary anymore)
                if now - normal_status.last_heartbeat > self.config.view_change_timeout {
                    let new_view_number = normal_status.view_number + 1;
                    return self.init_view_change(now, new_view_number + 1);
                }
            }
            (Status::ViewChange(view_change), _) => {
                // check if view change timed out
                if now - view_change.start_time > self.config.view_change_timeout {
                    return self.init_view_change(now, view_change.view_number + 1);
                }
            }
            (Status::StateTransfer(_), _) => {
                return self.tick_state_transfer(now);
            }
            (Status::Recovering(_), _) => {
                return self.tick_recovering(now);
            }
        }
        vec![]
    }

    fn tick_state_transfer(
        &mut self,
        now: Instant,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Status::StateTransfer(state_transfer) = &mut self.status else {
            unreachable!()
        };

        let mut broadcast_msg = None;
        if now - state_transfer.last_sent > self.config.state_transfer_timeout {
            state_transfer.last_sent = now;

            broadcast_msg = Some(VsrMessageInner::GetState(GetStateMsg {
                view_number: state_transfer.view_number,
                op_number: self.log.commit_number(),
            }));
        }

        if let Some(broadcast_msg) = broadcast_msg {
            return self.broadcast(now, broadcast_msg);
        }
        vec![]
    }

    fn tick_recovering(&mut self, now: Instant) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Status::Recovering(recovering_status) = &mut self.status else {
            unreachable!()
        };

        let mut broadcast_msg = None;
        if now - recovering_status.last_sent > self.config.state_transfer_timeout {
            recovering_status.last_sent = now;
            broadcast_msg = Some(recovering_status.recovery_msg());
        }

        if let Some(broadcast_msg) = broadcast_msg {
            return self.broadcast(now, broadcast_msg);
        }
        vec![]
    }

    fn tick_primary(&mut self, now: Instant) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Status::Normal(normal_status) = &mut self.status else {
            unreachable!()
        };

        let mut messages = vec![];
        // retry prepares
        while let Some(inflight_prepare) = normal_status.inflight_prepares.pop_front() {
            let InflightPrepare {
                time_sent,
                replica_id,
                prepare_msg,
            } = inflight_prepare;

            // too recent, no retry needed
            if now - time_sent < self.config.prepare_retry_timeout {
                normal_status.inflight_prepares.push_front(InflightPrepare {
                    time_sent,
                    replica_id,
                    prepare_msg,
                });
                break;
            }

            let PrepareMsg {
                view_number,
                operation,
                client_id,
                request_num,
                op_number,
                commit_number: _commit_number,
            } = prepare_msg;

            if view_number != normal_status.view_number {
                continue;
            }

            // already answered
            if normal_status.acknowledged_op_number(replica_id, op_number) {
                continue;
            }
            // resend prepare
            let new_prepare_msg = PrepareMsg {
                view_number,
                operation,
                client_id,
                request_num,
                op_number,
                // this might have changed in between
                commit_number: self.log.commit_number(),
            };
            normal_status.inflight_prepares.push_back(InflightPrepare {
                time_sent: now,
                replica_id,
                prepare_msg: new_prepare_msg.clone(),
            });
            messages.push(Message::VsrMessage(VsrMessage {
                from: self.replica_id,
                to: replica_id,
                inner: VsrMessageInner::Prepare(new_prepare_msg),
            }));
        }

        // send heartbeat
        if now - normal_status.last_heartbeat > self.config.heartbeat_interval {
            normal_status.last_heartbeat = now;
            messages.extend((0..self.replica_count).filter_map(|replica_id| {
                if replica_id == self.replica_id {
                    None
                } else {
                    Some(Message::VsrMessage(VsrMessage {
                        from: self.replica_id,
                        to: replica_id,
                        inner: VsrMessageInner::Commit(CommitMsg {
                            view_number: normal_status.view_number,
                            commit_number: self.log.commit_number(),
                        }),
                    }))
                }
            }));
        }
        messages
    }
}
