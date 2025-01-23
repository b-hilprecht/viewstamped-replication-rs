use std::time::Instant;

use glitch::{DeterministicClient, DeterministicNode, NodeId, ProtocolMessage};

use crate::{Client, Message, Replica};

use super::state_machine::{TestLogEntry, TestOp, TestResult, TestStateMachine};

impl ProtocolMessage for Message<TestOp, Vec<TestLogEntry>, TestResult> {
    fn source(&self) -> NodeId {
        match self {
            Message::VsrMessage(vsr_message) => NodeId::Node(vsr_message.from),
            Message::ClientResponse(client_response) => NodeId::Node(client_response.replica_id),
            Message::ClientRequest(request_msg) => NodeId::Client(request_msg.client_id),
        }
    }

    fn destination(&self) -> NodeId {
        match self {
            Message::VsrMessage(vsr_message) => NodeId::Node(vsr_message.to),
            Message::ClientResponse(client_response) => NodeId::Client(client_response.client_id),
            Message::ClientRequest(request_msg) => NodeId::Node(request_msg.replica_id),
        }
    }
}

impl DeterministicNode for Replica<TestOp, TestStateMachine, Vec<TestLogEntry>, TestResult> {
    type Message = Message<TestOp, Vec<TestLogEntry>, TestResult>;

    fn id(&self) -> NodeId {
        NodeId::Node(self.id())
    }

    fn tick(&mut self, now: Instant) -> Vec<Self::Message> {
        self.tick(now)
    }

    fn process_message(&mut self, msg: Self::Message, now: Instant) -> Vec<Self::Message> {
        self.process_message(msg, now)
    }

    fn recover(&mut self, now: Instant, nonce: u64, replica_count: usize) {
        let replica_id = self.id();

        *self =
            Replica::new_recovering(now, nonce, replica_count, replica_id, self.config().clone());
    }

    fn is_recovering(&self) -> bool {
        self.is_recovering()
    }
}

impl DeterministicClient for Client<TestOp, Vec<TestLogEntry>, TestResult> {
    type Message = Message<TestOp, Vec<TestLogEntry>, TestResult>;

    fn id(&self) -> NodeId {
        NodeId::Client(self.id())
    }

    fn tick(&mut self, now: Instant) -> Vec<Self::Message> {
        self.tick(now)
    }

    fn process_message(&mut self, msg: Self::Message, now: Instant) -> Vec<Self::Message> {
        self.process_message(msg, now)
    }

    fn finished(&self) -> bool {
        !self.remaining_requests()
    }
}
