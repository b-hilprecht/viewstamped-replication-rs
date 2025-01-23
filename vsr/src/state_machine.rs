/// A trait for a state machine that can be used with the viewstamped replication protocol.
///
/// The state machine is responsible for applying operations and keeping track of the state.
/// It is guaranteed that the state machine will only receive operations in order and only
/// committed operations will be applied.
pub trait StateMachine<Op, StateMachineDelta, Result>
where
    Op: Clone,
    StateMachineDelta: PartialEq + Eq + Clone + std::fmt::Debug,
    Result: std::fmt::Debug,
{
    fn new() -> Self;

    /// Perform the operation on the state machine and return the result.
    fn apply_operation(&mut self, operation: &Op, commit_number: usize) -> Result;

    /// Returns a delta between the current commit number and the supplied commit number.
    /// Can be used to bring a stale state machine up to date (that has seen all committed
    /// operations up to and including the supplied commit number). Note that if the state
    /// machine has a small state this can be as simple as returning the state itself.
    /// It is essential that the deltas produced by this function are compatble with the
    /// apply_delta function.
    fn get_delta(&self, commit_number: usize) -> StateMachineDelta;

    /// Apply a delta to the state machine to bring it up to date. It is guaranteed that the
    /// delta will only be called on a state machine that has seen at least all operations up to
    /// the start commit number of the delta and that it will only contain committed operations.
    /// However, the state machine must be able to handle cases where the delta contains some
    /// operations that were already applied (in order), i.e., the delta is too large.
    fn apply_delta(&mut self, snapshot_delta: StateMachineDelta);

    /// Returns the commit number of the last operation that was applied to the state machine.
    fn last_commit_number(&self) -> usize;
}
