use crate::shared::{Counter, Pid};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::{cmp::Ordering, collections::HashMap};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Vector clock implementation
pub struct VersionVector {
    set: HashMap<Pid, Counter>,
}

impl PartialOrd for VersionVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut has_smaller = false;
        let mut has_greater = false;

        for pid in self.set.keys().chain(other.set.keys()) {
            match self.set.get(pid).cmp(&other.set.get(pid)) {
                Ordering::Greater => has_greater = true,
                Ordering::Less => has_smaller = true,
                _ => {}
            }
        }

        match (has_smaller, has_greater) {
            (false, false) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (true, true) => None, // concurrent
        }
    }
}

impl VersionVector {
    pub fn new() -> Self {
        Self {
            set: HashMap::new(),
        }
    }

    /// Get the counter for a given process ID
    pub fn get(&self, pid: &Pid) -> u64 {
        *self.set.get(pid).unwrap_or(&0)
    }

    /// Increment the clock for the given process ID
    pub fn increment(&mut self, pid: Pid) {
        let counter = self.set.entry(pid).or_insert(0);
        *counter += 1;
    }

    /// Merge another version vector into this one
    pub fn merge(&mut self, other: &VersionVector) {
        for (pid, counter) in &other.set {
            let entry = self.set.entry(*pid).or_insert(0);
            *entry = (*entry).max(*counter);
        }
    }
}

pub trait DeltaCRDT: Clone + Debug + 'static {
    type Query;
    type Update;
    type Delta: Clone + Debug + Serialize + DeserializeOwned + Send + Sync;
    type Response;

    /// Gets the state
    fn query(&self, query: Self::Query) -> Self::Response;

    /// Mutates the state, records the delta and returns the client response
    fn update(&mut self, update: Self::Update);

    /// Returns the deltas between the current state and the state represented by the given version vector
    fn get_delta(&self, version_vector: &VersionVector) -> Option<Self::Delta>;

    /// Applies the remote delta to the local state
    fn merge_delta(&mut self, delta: Self::Delta);

    fn get_version_vector(&self) -> &VersionVector;

    fn generate_random_update(&self) -> Self::Update;
}
