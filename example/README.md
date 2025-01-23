# Example implementation

Just implements an in-memory log that is replicated across a set of servers.

```bash
# server
cargo run --bin server -- --replica-id 0 --replicas localhost:50051 --replicas localhost:50052 --replicas localhost:50053
cargo run --bin server -- --replica-id 1 --replicas localhost:50051 --replicas localhost:50052 --replicas localhost:50053
cargo run --bin server -- --replica-id 2 --replicas localhost:50051 --replicas localhost:50052 --replicas localhost:50053

# on restart add --recovery

# client
cargo run --bin client -- --replicas localhost:50051 --replicas localhost:50052 --replicas localhost:50053
cargo run --bin client -- --client-id 2 --replicas localhost:50051 --replicas localhost:50052 --replicas localhost:50053

# allow to add to the log, e.g., by typing append hello-world
```
