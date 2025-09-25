use crate::{
    crdt::{DeltaCRDT, VersionVector},
    shared::{Counter, Pid},
};
use rand::Rng;
use rand::{rngs::ThreadRng, seq::IteratorRandom};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
};

pub trait Randomizable {
    fn random() -> Self;
}

impl Randomizable for i32 {
    fn random() -> Self {
        let mut rng = rand::thread_rng();
        rng.gen()
    }
}

pub trait OrSetItem:
    Eq + Hash + Clone + Randomizable + Serialize + DeserializeOwned + Send + Sync + Debug + 'static
{
}
impl<
        T: Eq
            + Hash
            + Clone
            + Randomizable
            + Serialize
            + DeserializeOwned
            + Send
            + Sync
            + Debug
            + 'static,
    > OrSetItem for T
{
}

#[derive(Debug, Clone, Serialize)]
pub struct OrSet<T: OrSetItem> {
    pid: Pid,
    adds: HashMap<T, HashSet<(Pid, Counter)>>,
    #[serde(with = "tuple_key_map")]
    tombstones: HashMap<(Pid, Counter), (Pid, Counter)>,
}

pub enum OrSetQuery<T: OrSetItem> {
    Exists(T),
    Members,
}

pub enum OrSetUpdate<T: OrSetItem> {
    Add(T),
    Remove(T),
}

#[derive(Debug)]
pub enum OrSetResponse<T: OrSetItem> {
    Exists(T, bool),
    Members(Vec<T>),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrSetDelta<T: OrSetItem> {
    #[serde(bound = "")]
    adds: HashMap<T, HashSet<(Pid, Counter)>>,
    #[serde(with = "tuple_key_map")]
    tombstones: HashMap<(Pid, Counter), (Pid, Counter)>,
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
                    .map(|tags| tags.iter().any(|tag| !&self.tombstones.contains_key(tag)))
                    .unwrap_or(false);
                OrSetResponse::Exists(item, exists)
            }
            OrSetQuery::Members => {
                let members = self
                    .adds
                    .iter()
                    .filter_map(|(item, tags)| {
                        if tags.iter().any(|tag| !self.tombstones.contains_key(tag)) {
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

    fn update(&mut self, update: Self::Update, tag: (Pid, Counter)) {
        match update {
            OrSetUpdate::Add(item) => {
                self.adds.entry(item).or_default().insert(tag);
            }
            OrSetUpdate::Remove(item) => {
                if let Some(tags) = self.adds.get(&item) {
                    for tag_to_remove in tags {
                        self.tombstones.entry(*tag_to_remove).or_insert(tag);
                    }
                }
            }
        }
    }

    fn get_delta(&self, version_vector: &VersionVector) -> Self::Delta {
        let mut delta_adds: HashMap<T, HashSet<(Pid, Counter)>> = HashMap::new();
        let mut delta_tombstones = HashMap::new();

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
        for (item_to_remove, removal_timestamp) in &self.tombstones {
            let (pid, counter) = removal_timestamp;
            if *counter > version_vector.get(pid) {
                delta_tombstones.insert(*item_to_remove, *removal_timestamp);
            }
        }

        OrSetDelta {
            adds: delta_adds,
            tombstones: delta_tombstones,
        }
    }

    fn merge_delta(&mut self, delta: Self::Delta) {
        // Merge adds
        for (item, tags) in delta.adds {
            self.adds.entry(item).or_default().extend(tags);
        }

        // Merge tombstones
        self.tombstones.extend(delta.tombstones);
    }

    fn generate_random_update(&self, rng: &mut ThreadRng) -> Self::Update {
        let b: bool = rng.gen();
        if b {
            let item = T::random();
            Self::Update::Add(item)
        } else {
            let elem = self.adds.iter().choose(rng);
            if let Some((item, _)) = elem {
                Self::Update::Remove(item.clone())
            } else {
                let item = T::random();
                Self::Update::Add(item)
            }
        }
    }

    fn show_state(&self) {
        println!("adds: {:?}, tombstones: {:?}", self.adds, self.tombstones);
        println!("{:?}", self.query(OrSetQuery::Members));
    }
}

impl<T: OrSetItem> OrSet<T> {
    pub fn new(pid: Pid) -> Self {
        Self {
            pid,
            adds: HashMap::new(),
            tombstones: HashMap::new(),
        }
    }
}


mod tuple_key_map {
    use std::collections::HashMap;
    use super::*;
    use serde::{Serializer, Deserializer};
    use serde::de::Error;

    pub fn serialize<S>(
        map: &HashMap<(Pid, Counter), (Pid, Counter)>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let converted: HashMap<String, &(Pid, Counter)> = map
            .iter()
            .map(|((pid, counter), value)| {
                (format!("{}:{}", pid, counter), value)
            })
            .collect();
        converted.serialize(serializer)
    }

    pub fn deserialize<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<(Pid, Counter), (Pid, Counter)>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw: HashMap<String, (Pid, Counter)> = HashMap::deserialize(deserializer)?;
        raw.into_iter()
            .map(|(k, v)| {
                let mut parts = k.splitn(2, ':');
                let pid = parts.next().ok_or_else(|| D::Error::custom("missing pid"))?;
                let counter = parts.next().ok_or_else(|| D::Error::custom("missing counter"))?
                    .parse::<Counter>()
                    .map_err(D::Error::custom)?;
                let pid_val: Pid = pid.parse().map_err(D::Error::custom)?;
                Ok(((pid_val, counter), v))
            })
            .collect()
    }
}