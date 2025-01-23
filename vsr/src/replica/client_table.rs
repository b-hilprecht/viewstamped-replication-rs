use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{ClientResponse, ClientResponseInner, Message};

/// Records for each client the number of its most recent request, plus, if the request
/// has been executed, the result sent for that request
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTable<OpResult: Clone> {
    /// client_id -> most_recent_request
    inflight_requests: HashMap<usize, usize>,
    /// client_id -> (most_recent_request, result)
    last_acked_request: HashMap<usize, (usize, OpResult)>,
}

impl<OpResult: Clone> Default for ClientTable<OpResult> {
    fn default() -> Self {
        Self::new()
    }
}

impl<OpResult: Clone> ClientTable<OpResult> {
    pub fn new() -> Self {
        Self {
            inflight_requests: Default::default(),
            last_acked_request: Default::default(),
        }
    }

    pub fn clear_inflight_requests(&mut self) {
        self.inflight_requests.clear();
    }

    pub fn new_inflight_request(&mut self, client_id: usize, request_num: usize) {
        self.inflight_requests.insert(client_id, request_num);
    }

    pub fn new_committed_request(
        &mut self,
        client_id: usize,
        request_num: usize,
        result: OpResult,
    ) {
        self.last_acked_request
            .insert(client_id, (request_num, result));
        if let Some(inflight_request) = self.inflight_requests.get(&client_id) {
            if *inflight_request == request_num {
                self.inflight_requests.remove(&client_id);
            }
        }
    }

    pub fn new_request<Op, DeltaData: PartialEq + Eq + Clone + std::fmt::Debug>(
        &mut self,
        client_id: usize,
        view_number: usize,
        request_num: usize,
    ) -> Option<Vec<Message<Op, DeltaData, OpResult>>> {
        if let Some((ack_req_num, result)) = self.last_acked_request.get(&client_id) {
            match request_num.cmp(ack_req_num) {
                // drop if old request
                std::cmp::Ordering::Less => {
                    return Some(vec![]);
                }
                std::cmp::Ordering::Equal => {
                    return Some(vec![Message::ClientResponse(ClientResponse {
                        client_id,
                        replica_id: 0,
                        response: ClientResponseInner::Success {
                            view_number,
                            request_num,
                            result: result.clone(),
                        },
                    })]);
                }
                std::cmp::Ordering::Greater => {}
            }
        }

        if let Some(inflight_req_num) = self.inflight_requests.get(&client_id) {
            // old request or not yet ready to reply
            if request_num <= *inflight_req_num {
                return Some(vec![]);
            }
        }

        self.inflight_requests.insert(client_id, request_num);
        None
    }

    pub fn inflight_requests(&self) -> &HashMap<usize, usize> {
        &self.inflight_requests
    }

    pub fn last_acked_request(&self) -> &HashMap<usize, (usize, OpResult)> {
        &self.last_acked_request
    }
}
