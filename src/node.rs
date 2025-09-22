use crate::crdt::{DeltaCRDT, VersionVector};
use crate::network;
use crate::shared::{DisseminationStrategy, JitteredInterval, Pid};
use rand::rngs::ThreadRng;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);
const UPDATE_TIMEOUT: Duration = Duration::from_secs(2);
const LOG_STATE_TIMEOUT: Duration = Duration::from_secs(10);

const NETWORK_BATCH_SIZE: usize = 100;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage<T: DeltaCRDT> {
    // Informs about new update
    InfoMessage(VersionVector),
    // Requests delta to sync
    DeltaRequestMessage(VersionVector),
    // Returns delta with optional vv for two-way-sync
    DeltaMessage(T::Delta, Option<VersionVector>),
}

pub struct Node<T: DeltaCRDT> {
    pid: Pid,
    crdt: T,
    peers: Vec<Pid>,
    incoming_messages: Receiver<(Pid, ClusterMessage<T>)>,
    outgoing_messages: Sender<(Pid, ClusterMessage<T>)>,
    strategy: DisseminationStrategy,
    rng: ThreadRng,
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
            incoming_messages,
            outgoing_messages,
            strategy,
            rng,
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
                    let _ = self.outgoing_messages.send((*rand_peer, ClusterMessage::DeltaRequestMessage(self.crdt.get_version_vector().clone()))).await;
                },
                 _ = update_interval.tick() => {
                    let update = self.crdt.generate_random_update(&mut self.rng);
                    self.crdt.update(update);
                    match self.strategy {
                        DisseminationStrategy::Proactive => {
                            for peer in self.peers.iter() {
                                let _ = self.outgoing_messages.send((*peer, ClusterMessage::InfoMessage(self.crdt.get_version_vector().clone()))).await;
                            }
                        }
                        DisseminationStrategy::Hybrid => {
                            let rand_peer = self.peers.iter().choose(&mut self.rng).unwrap();
                            let _ = self.outgoing_messages.send((*rand_peer, ClusterMessage::InfoMessage(self.crdt.get_version_vector().clone()))).await;
                        }
                        _ => {}
                    }
                },
                _ = async {
                    self.incoming_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = log_state_interval.tick() => {
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
                ClusterMessage::InfoMessage(version_vector) => {
                    if matches!(
                        self.strategy,
                        DisseminationStrategy::Proactive | DisseminationStrategy::Hybrid
                    ) {
                        let vv = self.crdt.get_version_vector();
                        if !(&version_vector < vv) {
                            let _ = self
                                .outgoing_messages
                                .send((sender, ClusterMessage::DeltaRequestMessage(vv.clone())));
                        }
                    }
                }
                ClusterMessage::DeltaRequestMessage(version_vector) => {
                    if let Some(delta) = self.crdt.get_delta(&version_vector) {
                        let return_vv: Option<VersionVector> =
                            if matches!(self.strategy, DisseminationStrategy::Hybrid) {
                                Some(self.crdt.get_version_vector().clone())
                            } else {
                                None
                            };
                        let msg = ClusterMessage::DeltaMessage(delta.clone(), return_vv);
                        let _ = self.outgoing_messages.send((sender, msg)).await;
                    }
                }
                ClusterMessage::DeltaMessage(delta, version_vector) => {
                    self.crdt.merge_delta(delta);
                    if let Some(vv) = version_vector {
                        if let Some(ret_delta) = self.crdt.get_delta(&vv) {
                            let msg = ClusterMessage::DeltaMessage(ret_delta.clone(), None);
                            let _ = self.outgoing_messages.send((sender, msg));
                        }
                    }
                    if matches!(self.strategy, DisseminationStrategy::Proactive) {
                        for peer in self.peers.iter() {
                            let _ = self.outgoing_messages.send((
                                *peer,
                                ClusterMessage::InfoMessage(self.crdt.get_version_vector().clone()),
                            ));
                        }
                    }
                }
            }
        }
    }
}
