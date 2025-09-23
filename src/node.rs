use crate::crdt::{DeltaCRDT, VersionVector};
use crate::network;
use crate::shared::{DisseminationStrategy, JitteredInterval, Pid};
use rand::rngs::ThreadRng;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);
const UPDATE_TIMEOUT: Duration = Duration::from_secs(1);
const LOG_STATE_TIMEOUT: Duration = Duration::from_secs(5);

const NETWORK_BATCH_SIZE: usize = 100;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage<T: DeltaCRDT> {
    // Informs about new update
    Info(VersionVector),
    // Requests delta to sync
    DeltaRequest(VersionVector),
    // Returns delta with optional vv for two-way-sync
    Delta(T::Delta, VersionVector),
}

pub struct Node<T: DeltaCRDT> {
    pid: Pid,
    crdt: T,
    version_vector: VersionVector,
    peers: Vec<Pid>,
    incoming_messages: Receiver<(Pid, ClusterMessage<T>)>,
    outgoing_messages: Sender<(Pid, ClusterMessage<T>)>,
    strategy: DisseminationStrategy,
    rng: ThreadRng,
    number_of_updates: u64,
}

impl<T: DeltaCRDT> Node<T> {
    pub async fn new(pid: Pid, peers: Vec<Pid>, crdt: T, strategy: DisseminationStrategy) -> Self {
        let (incoming_messages, outgoing_messages) =
            network::launch::<ClusterMessage<T>>(pid, peers.clone()).await;
        let rng = rand::thread_rng();
        Self {
            pid,
            crdt,
            peers,
            version_vector: VersionVector::new(),
            incoming_messages,
            outgoing_messages,
            strategy,
            rng,
            number_of_updates: 0,
        }
    }

    pub async fn run(&mut self) {
        let is_pull = matches!(
            self.strategy,
            DisseminationStrategy::Reactive | DisseminationStrategy::Hybrid,
        );

        let mut poll_interval = JitteredInterval::new(POLL_TIMEOUT, POLL_TIMEOUT);
        let mut update_interval = JitteredInterval::new(UPDATE_TIMEOUT, UPDATE_TIMEOUT);

        let mut log_state_interval = tokio::time::interval(LOG_STATE_TIMEOUT);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        loop {
            tokio::select! {
                _ = poll_interval.tick(), if is_pull => {
                    let rand_peer = self.peers.iter().choose(&mut self.rng).unwrap();
                    let _ = self.outgoing_messages.send((*rand_peer, ClusterMessage::DeltaRequest(self.version_vector.clone()))).await;
                },
                 _ = update_interval.tick() => {
                    if self.number_of_updates < 10 {
                        self.number_of_updates += 1;
                        let update = self.crdt.generate_random_update(&mut self.rng);
                        self.version_vector.increment(self.pid);
                        self.crdt.update(update, (self.pid, self.version_vector.get(&self.pid)));
                        match self.strategy {
                            DisseminationStrategy::Proactive => {
                                for peer in self.peers.iter() {
                                    let _ = self.outgoing_messages.send((*peer, ClusterMessage::Info(self.version_vector.clone()))).await;
                                }
                            }
                            DisseminationStrategy::Hybrid => {
                                let rand_peer = self.peers.iter().choose(&mut self.rng).unwrap();
                                let _ = self.outgoing_messages.send((*rand_peer, ClusterMessage::DeltaRequest(self.version_vector.clone()))).await;
                            }
                            _ => {}
                        }
                    }
                },
                _ = async {
                    self.incoming_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = log_state_interval.tick() => {
                    println!("{:?}", self.version_vector);
                    let test = self.crdt.show_state();
                },
            }
        }
    }

    async fn handle_cluster_messages(
        &mut self,
        cluster_messages: &mut Vec<(Pid, ClusterMessage<T>)>,
    ) {
        for (sender, msg) in cluster_messages.drain(..) {
            match msg {
                ClusterMessage::Info(version_vector) => {
                    if matches!(self.strategy, DisseminationStrategy::Proactive)
                        && !(version_vector < self.version_vector)
                    {
                        let _ = self
                            .outgoing_messages
                            .send((sender, ClusterMessage::DeltaRequest(version_vector)))
                            .await;
                    }
                }
                ClusterMessage::DeltaRequest(version_vector) => {
                    if !(version_vector < self.version_vector) {
                        let delta = self.crdt.get_delta(&version_vector);
                        let msg = ClusterMessage::Delta(delta, self.version_vector.clone());
                        let _ = self.outgoing_messages.send((sender, msg)).await;
                    }
                }
                ClusterMessage::Delta(delta, version_vector) => {
                    if !(version_vector < self.version_vector) {
                        self.crdt.merge_delta(delta);
                        self.version_vector.merge(&version_vector);
                        match self.strategy {
                            DisseminationStrategy::Proactive => {
                                for peer in self.peers.iter() {
                                    if *peer != sender {
                                        let msg = ClusterMessage::Info(self.version_vector.clone());
                                        let _ = self.outgoing_messages.send((*peer, msg)).await;
                                    }
                                }
                            }
                            DisseminationStrategy::Hybrid => {
                                let delta = self.crdt.get_delta(&version_vector);
                                let msg = ClusterMessage::Delta(delta, self.version_vector.clone());
                                let _ = self.outgoing_messages.send((sender, msg)).await;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }
}
