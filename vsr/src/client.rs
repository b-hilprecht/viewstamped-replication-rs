use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{ClientResponseInner, Message, RequestMsg};

/// A request that the client wants to send to the server.
#[derive(Debug)]
struct Request<Op> {
    operation: Arc<Op>,
    request_num: usize,
}

/// A request that has been sent to the server but has not yet been acknowledged.
#[derive(Debug)]
struct InflightRequest<Op> {
    request_num: usize,
    sent_at: Instant,
    operation: Arc<Op>,
}

impl<Op> From<&InflightRequest<Op>> for Request<Op>
where
    Op: Clone,
{
    fn from(inflight_request: &InflightRequest<Op>) -> Self {
        Request {
            operation: inflight_request.operation.clone(),
            request_num: inflight_request.request_num,
        }
    }
}

/// Client code for the viewstamped replication protocol.
#[derive(Debug)]
pub struct Client<Op, StateMachineDelta, OpResult> {
    phantom: std::marker::PhantomData<StateMachineDelta>,

    /// Queue of requests to be sent to the primary.
    request_queue: VecDeque<Request<Op>>,

    /// Number of replicas in the system.
    num_replicas: usize,

    /// Next request number to be assigned.
    next_request_num: usize,

    /// The current request being sent. Might have to be resent.
    inflight_request: Option<InflightRequest<Op>>,

    /// The last known primary replica.
    last_known_primary: Option<usize>,

    /// The number of the last request that was acknowledged.
    ack_req: usize,

    /// The client's ID.
    client_id: usize,

    /// The duration to wait before resending a request.
    request_retry: Duration,

    /// The last result received from the server.
    last_result: Option<(usize, OpResult)>,
}

impl<Op, StateMachineDelta, OpResult> Client<Op, StateMachineDelta, OpResult>
where
    Op: Clone,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    OpResult: PartialEq + Eq + Clone + std::fmt::Debug,
{
    pub fn new(client_id: usize, num_replicas: usize, request_retry: Duration) -> Self {
        Client {
            phantom: std::marker::PhantomData,
            request_queue: Default::default(),
            num_replicas,
            next_request_num: 0,
            inflight_request: None,
            last_known_primary: Some(1),
            client_id,
            ack_req: 0,
            request_retry,
            last_result: None,
        }
    }

    fn send_request_primary(
        &mut self,
        request: Request<Op>,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let replicas = match self.last_known_primary {
            Some(primary) => vec![primary],
            None => (0..self.num_replicas).collect(),
        };
        self.send_requests(replicas, request)
    }

    fn send_requests(
        &mut self,
        replica_range: Vec<usize>,
        request: Request<Op>,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Request {
            operation,
            request_num,
        } = request;

        replica_range
            .into_iter()
            .map(|replica_id| {
                Message::ClientRequest(RequestMsg {
                    operation: operation.clone(),
                    client_id: self.client_id,
                    request_num,
                    replica_id,
                })
            })
            .collect()
    }

    pub fn ack_req(&self) -> usize {
        self.ack_req
    }

    fn next_request(&mut self, now: Instant) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Some(request) = self.request_queue.pop_front() else {
            return vec![];
        };

        self.inflight_request = Some(InflightRequest {
            request_num: request.request_num,
            sent_at: now,
            operation: request.operation.clone(),
        });
        self.send_request_primary(request)
    }

    /// Queue a request to be sent to the server.
    pub fn queue_request(&mut self, operation: Op) -> usize {
        self.next_request_num += 1;
        self.request_queue.push_back(Request {
            operation: Arc::new(operation),
            request_num: self.next_request_num,
        });

        self.next_request_num
    }

    /// Process a message from the server and return any messages that need to be sent. Note
    /// that the current time is also provided to the client to enable a deterministic
    /// execution.
    pub fn process_message(
        &mut self,
        msg: Message<Op, StateMachineDelta, OpResult>,
        now: Instant,
    ) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        let Message::ClientResponse(resp) = msg else {
            panic!("client should only receive responses");
        };

        match resp.response {
            ClientResponseInner::Success {
                view_number,
                request_num,
                result,
            } => {
                // old response ignored
                if request_num <= self.ack_req {
                    return vec![];
                }
                self.ack_req = request_num;
                self.last_known_primary = Some((view_number - 1) % self.num_replicas);
                self.inflight_request = None;
                self.last_result = Some((request_num, result));

                self.next_request(now)
            }
            ClientResponseInner::NotPrimary { .. } | ClientResponseInner::NotNormal => {
                let Some(inflight_request) = &self.inflight_request else {
                    return vec![];
                };
                match resp.response {
                    ClientResponseInner::NotPrimary { primary } => {
                        self.last_known_primary = Some(primary);
                        return self.send_requests(vec![primary], inflight_request.into());
                    }
                    ClientResponseInner::NotNormal => {
                        self.last_known_primary = None;
                    }
                    _ => unreachable!(),
                };
                vec![]
            }
        }
    }

    /// Tick the client. This should be called periodically to check for timeouts and to resend requests.
    /// The current time is provided to the client to enable a deterministic execution.
    pub fn tick(&mut self, now: Instant) -> Vec<Message<Op, StateMachineDelta, OpResult>> {
        match &self.inflight_request {
            Some(inflight_request) => {
                // periodically resend. resending is always broadcast.
                if now.duration_since(inflight_request.sent_at) > self.request_retry {
                    return self
                        .send_requests((0..self.num_replicas).collect(), inflight_request.into());
                }
                vec![]
            }
            None => self.next_request(now),
        }
    }

    pub fn remaining_requests(&self) -> bool {
        !self.request_queue.is_empty() || self.inflight_request.is_some()
    }

    pub fn id(&self) -> usize {
        self.client_id
    }

    pub fn consume_result(&mut self) -> Option<(usize, OpResult)> {
        self.last_result.take()
    }
}
