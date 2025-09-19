use serde::{Deserialize, Serialize};

pub type Pid = u32;
pub type Counter = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TestbedConfig {
    pub server_id: Pid,
    pub peers: Vec<Pid>,
}
