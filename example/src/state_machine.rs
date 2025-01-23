use serde::{Deserialize, Serialize};

use tracing::info;
use vsr::StateMachine;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogAppendOp {
    pub entry: String,
    pub op_number: usize,
}

#[derive(Clone, Debug, Default)]
pub struct LogStateMachine {
    pub log: Vec<LogAppendOp>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppendResult {
    pub log_len: usize,
}

impl StateMachine<String, Vec<LogAppendOp>, AppendResult> for LogStateMachine {
    fn new() -> Self {
        LogStateMachine { log: Vec::new() }
    }

    fn apply_operation(&mut self, op: &String, op_number: usize) -> AppendResult {
        if op_number != self.last_commit_number() + 1 {
            panic!(
                "Expected op_number to be {}, but was {}",
                self.last_commit_number() + 1,
                op_number
            );
        }
        self.log.push({
            LogAppendOp {
                entry: op.clone(),
                op_number,
            }
        });
        info!(log =?self.log, "Applied operation to log", );
        AppendResult {
            log_len: self.log.len(),
        }
    }

    fn get_delta(&self, op_number: usize) -> Vec<LogAppendOp> {
        self.log[op_number..].to_vec()
    }

    fn apply_delta(&mut self, state_machine_delta: Vec<LogAppendOp>) {
        let end_op_number = self.last_commit_number();
        if state_machine_delta.is_empty() {
            return;
        }

        for entry in state_machine_delta
            .into_iter()
            .skip_while(|e| e.op_number <= end_op_number)
        {
            self.log.push(entry);
        }
        info!(log =?self.log, "Applied delta to log", );
    }

    fn last_commit_number(&self) -> usize {
        self.log.last().map(|entry| entry.op_number).unwrap_or(0)
    }
}
