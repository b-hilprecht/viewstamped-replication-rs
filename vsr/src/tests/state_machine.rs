use crate::StateMachine;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TestOp {
    pub client_id: usize,
    pub request_num: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TestLogEntry {
    pub client_id: usize,
    pub request_num: usize,
    pub op_number: usize,
}

#[derive(Clone, Debug, Default)]
pub struct TestStateMachine {
    pub state: Vec<TestLogEntry>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TestResult {
    pub client_id: usize,
    pub request_num: usize,
    pub op_number: usize,
}

impl StateMachine<TestOp, Vec<TestLogEntry>, TestResult> for TestStateMachine {
    fn new() -> Self {
        TestStateMachine { state: Vec::new() }
    }

    fn apply_operation(&mut self, op: &TestOp, op_number: usize) -> TestResult {
        if op_number != self.last_commit_number() + 1 {
            panic!(
                "Expected op_number to be {}, but was {}",
                self.last_commit_number() + 1,
                op_number
            );
        }
        self.state.push(TestLogEntry {
            client_id: op.client_id,
            request_num: op.request_num,
            op_number,
        });
        TestResult {
            client_id: op.client_id,
            request_num: op.request_num,
            op_number,
        }
    }

    fn get_delta(&self, op_number: usize) -> Vec<TestLogEntry> {
        self.state[op_number..].to_vec()
    }

    fn apply_delta(&mut self, state_machine_delta: Vec<TestLogEntry>) {
        let end_op_number = self.last_commit_number();
        if state_machine_delta.is_empty() {
            return;
        }
        if state_machine_delta[0].op_number > end_op_number + 1 {
            panic!(
                "Expected op_number to be {}, but was {}",
                end_op_number + 1,
                state_machine_delta[0].op_number
            );
        }

        for entry in state_machine_delta
            .into_iter()
            .skip_while(|e| e.op_number <= end_op_number)
        {
            self.state.push(entry);
        }
    }

    fn last_commit_number(&self) -> usize {
        self.state.last().map(|entry| entry.op_number).unwrap_or(0)
    }
}
