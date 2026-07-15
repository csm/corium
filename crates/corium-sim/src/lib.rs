//! Deterministic simulation harness skeleton.

/// Abstract deterministic clock used by simulations.
pub trait Clock {
    /// Current logical milliseconds.
    fn now_millis(&self) -> i64;
}

/// Abstract byte storage used by simulations.
pub trait Storage {
    /// Reads bytes by key.
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;

    /// Writes bytes by key.
    fn put(&mut self, key: Vec<u8>, value: Vec<u8>);
}
