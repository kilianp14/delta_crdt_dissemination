use std::str::FromStr;
use serde::{Deserialize, Serialize};

pub type Pid = u32;
pub type Counter = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TestbedConfig {
    pub server_id: Pid,
    pub peers: Vec<Pid>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DisseminationStrategy {
    Push,
    Pull,
    PushPull,
}

impl FromStr for DisseminationStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "push" => Ok(DisseminationStrategy::Push),
            "pull" => Ok(DisseminationStrategy::Pull),
            "push-pull" => Ok(DisseminationStrategy::PushPull),
            other => Err(format!("Unknown Dissemination Strategy: {}", other)),
        }
    }
}
