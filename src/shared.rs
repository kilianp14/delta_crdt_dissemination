use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{
    str::FromStr,
    time::{Duration, Instant},
};

pub type Pid = u32;
pub type Counter = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TestbedConfig {
    pub server_id: Pid,
    pub peers: Vec<Pid>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum DisseminationStrategy {
    Proactive,
    Reactive,
    Hybrid,
}

impl FromStr for DisseminationStrategy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "push" => Ok(DisseminationStrategy::Proactive),
            "pull" => Ok(DisseminationStrategy::Reactive),
            "push-pull" => Ok(DisseminationStrategy::Hybrid),
            other => Err(format!("Unknown Dissemination Strategy: {}", other)),
        }
    }
}

pub struct JitteredInterval {
    base_interval: Duration,
    max_jitter: Duration,
    next_elapse: Instant,
}

impl JitteredInterval {
    pub fn new(base_interval: Duration, max_jitter: Duration) -> Self {
        let mut rng = rand::thread_rng();

        // Convert to milliseconds, generate random, then convert back
        let jitter_ms = rng.gen_range(0..=max_jitter.as_millis() as u64);
        let jitter = Duration::from_millis(jitter_ms);

        Self {
            base_interval,
            max_jitter,
            next_elapse: std::time::Instant::now() + base_interval + jitter,
        }
    }

    pub async fn tick(&mut self) -> std::time::Instant {
        tokio::time::sleep_until(self.next_elapse.into()).await;
        let now = std::time::Instant::now();

        let mut rng = rand::thread_rng();
        let jitter_ms = rng.gen_range(0..=self.max_jitter.as_millis() as u64);
        let jitter = Duration::from_millis(jitter_ms);

        self.next_elapse = now + self.base_interval + jitter;
        now
    }
}
