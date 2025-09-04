use crate::{
    crdt::{DeltaCRDT, VersionVector},
    shared::{Counter, Pid},
};
use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

pub trait OrSetItem: Eq + Hash + Clone {}
impl<T: Eq + Hash + Clone> OrSetItem for T {}

pub struct OrSet<T: OrSetItem> {
    pid: Pid,
    adds: HashMap<T, HashSet<(Pid, Counter)>>,
    tombstones: HashSet<(Pid, Counter)>,
    version_vector: VersionVector,
}

pub enum OrSetQuery<T: OrSetItem> {
    Exists(T),
    Members,
}

pub enum OrSetUpdate<T: OrSetItem> {
    Add(T),
    Remove(T),
}

pub enum OrSetResponse<T: OrSetItem> {
    Exists(T, bool),
    Members(Vec<T>),
}

pub struct OrSetDelta<T: OrSetItem> {
    adds: HashMap<T, HashSet<(Pid, Counter)>>,
    tombstones: HashSet<(Pid, Counter)>,
    version_vector: VersionVector,
}

impl<T: OrSetItem> DeltaCRDT for OrSet<T> {
    type Query = OrSetQuery<T>;
    type Update = OrSetUpdate<T>;
    type Response = OrSetResponse<T>;
    type Delta = OrSetDelta<T>;

    fn query(&self, query: Self::Query) -> Self::Response {
        match query {
            OrSetQuery::Exists(item) => {
                let exists = self
                    .adds
                    .get(&item)
                    .map(|tags| !tags.is_subset(&self.tombstones))
                    .unwrap_or(false);
                OrSetResponse::Exists(item, exists)
            }
            OrSetQuery::Members => {
                let members = self
                    .adds
                    .iter()
                    .filter_map(|(item, tags)| {
                        if !tags.is_subset(&self.tombstones) {
                            Some(item.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                OrSetResponse::Members(members)
            }
        }
    }

    fn update(&mut self, update: Self::Update) {
        match update {
            OrSetUpdate::Add(item) => {
                self.version_vector.increment(self.pid);
                let counter = self.version_vector.get(&self.pid);
                let tag = (self.pid, counter);

                self.adds.entry(item).or_default().insert(tag);
            }
            OrSetUpdate::Remove(item) => {
                if let Some(tags) = self.adds.get(&item) {
                    for tag in tags {
                        self.tombstones.insert(*tag);
                    }
                }
            }
        }
    }

    fn get_delta(&self, version_vector: &VersionVector) -> Option<Self::Delta> {
        // Everything is up-to-date
        if version_vector >= &self.version_vector {
            return None;
        }

        let mut delta_adds: HashMap<T, HashSet<(Pid, Counter)>> = HashMap::new();
        let mut delta_tombstones = HashSet::new();

        // Collect new adds
        for (item, tags) in &self.adds {
            for (pid, counter) in tags {
                if *counter > version_vector.get(pid) {
                    delta_adds
                        .entry(item.clone())
                        .or_default()
                        .insert((*pid, *counter));
                }
            }
        }

        // Collect new tombstones
        for (pid, counter) in &self.tombstones {
            if *counter > version_vector.get(pid) {
                delta_tombstones.insert((*pid, *counter));
            }
        }

        Some(OrSetDelta {
            adds: delta_adds,
            tombstones: delta_tombstones,
            version_vector: self.version_vector.clone(),
        })
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        // Merge adds
        for (item, tags) in delta.adds {
            self.adds.entry(item).or_default().extend(tags);
        }

        // Merge tombstones
        self.tombstones.extend(delta.tombstones);

        // Merge version vectors
        self.version_vector.merge(&delta.version_vector);
    }
}

impl<T: OrSetItem> OrSet<T> {
    fn new(pid: Pid) -> Self {
        Self {
            pid,
            adds: HashMap::new(),
            tombstones: HashSet::new(),
            version_vector: VersionVector::new(),
        }
    }
}
