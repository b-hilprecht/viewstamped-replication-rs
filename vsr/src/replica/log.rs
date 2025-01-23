use std::{cmp, collections::VecDeque, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{ClientResponse, ClientResponseInner, Delta, Message, StateMachine};

use super::ClientTable;

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct LogEntry<Op> {
    pub op_number: usize,
    pub operation: Arc<Op>,
    pub request_num: usize,
    pub client_id: usize,
}

/// A log that holds all uncommitted operations. Once an operation is committed, it is applied to the state machine.
/// In addition, the log holds a client table that records for each client the number of its most recent request, plus,
/// if the request has been executed, the result sent for that request.
#[derive(Debug)]
pub struct Log<
    Op: Clone,
    S: StateMachine<Op, DeltaData, OpResult> + std::fmt::Debug,
    DeltaData: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: Clone + std::fmt::Debug,
> {
    phantom: std::marker::PhantomData<DeltaData>,
    /// Holds all uncommitted operations. Once an operation is committed, it is applied to the state machine
    /// and removed from the log.
    log: VecDeque<LogEntry<Op>>,
    client_table: ClientTable<OpResult>,
    replica_id: usize,
    state_machine: S,
}

impl<Op, S, DeltaData, OpResult> Log<Op, S, DeltaData, OpResult>
where
    Op: Clone + std::fmt::Debug,
    S: StateMachine<Op, DeltaData, OpResult> + std::fmt::Debug,
    DeltaData: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: Clone + std::fmt::Debug,
{
    pub fn new(replica_id: usize) -> Self {
        Log {
            phantom: std::marker::PhantomData,
            log: VecDeque::new(),
            client_table: ClientTable::new(),
            replica_id,
            state_machine: S::new(),
        }
    }

    pub fn push_entry_primary(&mut self, operation: Arc<Op>, request_num: usize, client_id: usize) {
        self.log.push_back(LogEntry {
            op_number: self.op_number() + 1,
            operation,
            request_num,
            client_id,
        });
    }

    pub fn push_entry_backup(&mut self, entry: LogEntry<Op>) {
        self.client_table
            .new_inflight_request(entry.client_id, entry.request_num);
        self.log.push_back(entry);
    }

    pub fn execute_commits_up_to(
        &mut self,
        commit_number: usize,
        view_number: usize,
    ) -> Vec<Message<Op, DeltaData, OpResult>> {
        if commit_number <= self.commit_number() {
            return vec![];
        }

        let commit_number = cmp::min(commit_number, self.op_number());
        let mut messages = vec![];

        while let Some(entry) = self.log.pop_front() {
            if entry.op_number <= commit_number {
                messages.push(Message::ClientResponse(
                    self.execute_commit(entry, view_number),
                ));
            } else {
                self.log.push_front(entry);
                break;
            }
        }

        messages
    }

    pub fn op_number(&self) -> usize {
        self.log
            .back()
            .map_or(self.commit_number(), |entry| entry.op_number)
    }

    fn execute_commit(
        &mut self,
        log_entry: LogEntry<Op>,
        view_number: usize,
    ) -> ClientResponse<OpResult> {
        let op_number = log_entry.op_number;
        assert!(
            op_number == self.commit_number() + 1,
            "Next commit number is not expected ({}). Current commit_number is {} at replica {} and next commit number should be {}.",
            op_number,
            self.commit_number(),
            self.replica_id,
            self.commit_number() + 1
        );

        let result = self
            .state_machine
            .apply_operation(&log_entry.operation, log_entry.op_number);

        self.client_table.new_committed_request(
            log_entry.client_id,
            log_entry.request_num,
            result.clone(),
        );
        let response = ClientResponseInner::Success {
            view_number,
            request_num: log_entry.request_num,
            result,
        };
        ClientResponse {
            client_id: log_entry.client_id,
            replica_id: self.replica_id,
            response,
        }
    }

    pub fn commit_number(&self) -> usize {
        self.state_machine.last_commit_number()
    }

    pub fn log(&self) -> &VecDeque<LogEntry<Op>> {
        &self.log
    }

    pub fn client_table(&self) -> &ClientTable<OpResult> {
        &self.client_table
    }

    pub fn new_client_request(
        &mut self,
        client_id: usize,
        view_number: usize,
        request_num: usize,
    ) -> Option<Vec<Message<Op, DeltaData, OpResult>>> {
        self.client_table
            .new_request(client_id, view_number, request_num)
    }

    pub fn state_machine(&self) -> &S {
        &self.state_machine
    }

    pub fn get_delta(&self, start_op_number: usize) -> Option<Delta<Op, DeltaData, OpResult>> {
        if start_op_number > self.op_number() {
            return None;
        }

        let mut state_machine_delta = None;
        if start_op_number < self.commit_number() {
            state_machine_delta = Some(self.state_machine.get_delta(start_op_number));
        }

        let log_delta = self
            .log
            .iter()
            .skip_while(|entry| entry.op_number <= self.commit_number())
            .cloned()
            .collect();

        Some(Delta {
            state_machine_delta,
            log_delta,
            client_table: self.client_table.clone(),
            start_op_number,
            end_op_number: self.op_number(),
            commit_number: self.commit_number(),
        })
    }

    pub fn apply_delta(&mut self, mut delta: Delta<Op, DeltaData, OpResult>) {
        // we should not apply deltas that miss operations
        assert!(delta.start_op_number <= self.op_number() + 1);

        if delta.commit_number < self.commit_number() {
            delta
                .log_delta
                .retain(|entry| entry.op_number > self.commit_number());
            self.client_table.clear_inflight_requests();
            for log in delta.log_delta.iter() {
                self.client_table
                    .new_inflight_request(log.client_id, log.request_num);
            }
        } else {
            self.client_table = delta.client_table;
        }

        if let Some(state_machine_delta) = delta.state_machine_delta {
            self.state_machine.apply_delta(state_machine_delta);
        }

        self.log = delta.log_delta.into();
    }

    pub fn truncate_uncommitted(&mut self) {
        self.log.clear();
        self.client_table.clear_inflight_requests();
    }

    pub fn last_uncommitted(&self) -> Option<&LogEntry<Op>> {
        if self.log.is_empty() {
            return None;
        }
        if self.commit_number() == self.op_number() {
            return None;
        }

        Some(&self.log[self.log.len() - 1])
    }
}
