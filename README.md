# Viewstamped Replication (VSR) Consensus Protocol

A Rust implementation of the Viewstamped Replication (VSR) consensus protocol, enabling state machine replication across a distributed system while tolerating network partitions and node failures. Fully deterministic implementation. Tested using simulation simulation testing with [glitch](https://github.com/b-hilprecht/glitch).

## Features

### Core Protocol

Implements VSR subprotocols:

- Normal operation (primary-backup replication)
- View changes (leader election on primary failure)
- State transfer (catch-up mechanism for lagging replicas)
- Recovery (node rejoin after crash)
- State machine delta mechanism for efficient state synchronization. Instead of transferring the entire state, only the delta between the last known state and the current state is transferred.

## Testing

Extensively [tested](./vsr/src/tests/simulation_tests.rs) using deterministic simulation (via the `glitch` framework) to verify correctness under:

- Network partitions
- Message drops and reordering
- Node crashes and recoveries
- Primary failures and view changes

## Usage

1. Implement the `StateMachine` trait for your replicated state:

```rust
pub trait StateMachine<Op, StateMachineDelta, Result> {
    fn new() -> Self;
    fn apply_operation(&mut self, operation: &Op, commit_number: usize) -> Result;
    fn get_delta(&self, commit_number: usize) -> StateMachineDelta;
    fn apply_delta(&mut self, snapshot_delta: StateMachineDelta);
    fn last_commit_number(&self) -> usize;
}
```

2. Create replicas using the `Replica` struct:

```rust
let replica = Replica::new(
    start_time,
    replica_count,
    replica_id,
    config
);
```

3. Create clients using the `Client` struct:

```rust
let client = Client::new(
    client_id,
    replica_count,
    retry_interval
);
```

See the `example/` directory for a complete implementation of a replicated log, including:

- TCP-based networking layer
- Command-line client and server
- Example state machine implementation

## References

- [VSR Protocol Paper](https://pmg.csail.mit.edu/papers/vr-revisited.pdf)
- [VSR Protocol Analysis](https://jack-vanlightly.com/analyses/2022/12/20/paper-vr-revisited-view-change-questions-part1)
