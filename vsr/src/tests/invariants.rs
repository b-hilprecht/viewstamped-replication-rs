use glitch::{InvariantChecker, Node, NodeId};

use crate::{
    replica::{ClientTable, Log},
    Client, Replica,
};
use std::{cmp, collections::HashMap};

use super::state_machine::{TestLogEntry, TestOp, TestResult, TestStateMachine};

#[derive(Debug, Default)]
pub struct VsrInvariantChecker;

impl
    InvariantChecker<
        Replica<TestOp, TestStateMachine, Vec<TestLogEntry>, TestResult>,
        Client<TestOp, Vec<TestLogEntry>, TestResult>,
    > for VsrInvariantChecker
{
    fn check_invariants(
        &self,
        seed: u64,
        nodes: &[Node<Replica<TestOp, TestStateMachine, Vec<TestLogEntry>, TestResult>>],
        clients: &[Client<TestOp, Vec<TestLogEntry>, TestResult>],
    ) {
        // get log of node with highest commit number among active nodes
        let max_commit_number = nodes
            .iter()
            .filter(|node| node.is_up())
            .map(|node| node.node().commit_number())
            .max()
            .unwrap();

        let max_log = reconstruct_log(
            nodes
                .iter()
                .filter(|node| node.is_up())
                .find(|node| node.node().commit_number() == max_commit_number)
                .unwrap()
                .node()
                .log(),
        );

        let mut req_replication_count = HashMap::new();

        for node in nodes {
            if !node.is_up() {
                continue;
            }

            let log = reconstruct_log(node.node().log());

            // check that the client table is correct
            validate_client_table(
                node.id(),
                seed,
                node.node().client_table(),
                &log,
                node.node().commit_number(),
            );

            // check that everything up to commit number is same as in long log
            for (i, entry) in max_log.iter().enumerate() {
                if entry.op_number > node.node().commit_number() {
                    break;
                }

                assert_eq!(
                    entry,
                    &log[i],
                    "Log mismatch at index {} for node {:?} (seed: {})",
                    i,
                    node.id(),
                    seed
                );
            }

            // Check that request numbers are increasing by one by client and op numbers are increasing by one
            let mut last_request_num = HashMap::new();
            let mut last_op_num = 0;

            for entry in log {
                assert_eq!(
                    entry.op_number,
                    last_op_num + 1,
                    "Operation numbers are not increasing by one on node {:?}. Previous: {}, Current: {} (seed: {})",
                    node.id(),
                    last_op_num,
                    entry.op_number,
                    seed
                );
                last_op_num = entry.op_number;

                // Track replication count for each request
                match node.id() {
                    NodeId::Node(_) => {
                        *req_replication_count
                            .entry((entry.client_id, entry.request_num))
                            .or_insert(0) += 1;
                    }
                    NodeId::Client(_) => {
                        unreachable!(
                            "Node wrapper should only contain Replica nodes (seed: {})",
                            seed
                        )
                    }
                }

                let last_client_req = *last_request_num.get(&entry.client_id).unwrap_or(&0);
                if entry.op_number <= node.node().commit_number() {
                    assert!(
                        entry.request_num == last_client_req + 1,
                        "Request numbers are not increasing by one on node {:?}. Previous: {}, Current: {} (seed: {})",
                        node.id(),
                        last_client_req,
                        entry.request_num,
                        seed
                    );
                }
                last_request_num.insert(entry.client_id, entry.request_num);
            }
        }

        // Check that acknowledged requests are in the majority of active logs (
        // excluding failed nodes and recovering nodes)
        let one_below_quorum = nodes.len() / 2;
        let majority_threshold = one_below_quorum
            - cmp::min(
                nodes.iter().filter(|n| !n.is_up()).count(),
                one_below_quorum,
            );

        for client in clients {
            for req_num in 1..=client.ack_req() {
                let count = *req_replication_count
                    .get(&(client.id(), req_num))
                    .unwrap_or(&0);

                assert!(
                    count > majority_threshold,
                    "Request {} from client {} is not replicated in the majority of active logs. \
                     Replicated in {} logs, need more than {} for majority of {} active nodes (seed: {})",
                    req_num,
                    client.id(),
                    count,
                    majority_threshold,
                    nodes.len(),
                    seed
                );
            }
        }
    }
}

fn reconstruct_log(
    log: &Log<TestOp, TestStateMachine, Vec<TestLogEntry>, TestResult>,
) -> Vec<TestLogEntry> {
    let mut reconstructed_log = log.state_machine().state.clone();
    log.log().iter().for_each(|entry| {
        reconstructed_log.push(TestLogEntry {
            client_id: entry.client_id,
            request_num: entry.request_num,
            op_number: entry.op_number,
        });
    });
    reconstructed_log
}

fn validate_client_table(
    replica_id: NodeId,
    seed: u64,
    client_table: &ClientTable<TestResult>,
    log: &Vec<TestLogEntry>,
    commit_number: usize,
) {
    let mut expected_last_acked_request = HashMap::new();
    let mut expected_inflight_requests = HashMap::new();

    for entry in log {
        if entry.op_number <= commit_number {
            expected_last_acked_request.insert(entry.client_id, entry.request_num);
        } else {
            expected_inflight_requests.insert(entry.client_id, entry.request_num);
        }
    }

    assert_eq!(
        &expected_inflight_requests,
        client_table.inflight_requests(),
        "inflight_requests {} (seed: {})",
        replica_id,
        seed
    );

    assert_eq!(
        expected_last_acked_request,
        client_table
            .last_acked_request()
            .iter()
            .map(|(k, (v, _))| (*k, *v))
            .collect(),
        "last_acked_request {} (seed: {})",
        replica_id,
        seed
    );
}
