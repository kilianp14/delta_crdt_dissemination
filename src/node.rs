use crate::crdt::{DeltaCRDT, VersionVector};
use crate::network::launch;
use crate::shared::Pid;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};

const POLL_TIMEOUT: Duration = Duration::from_millis(100);
const UPDATE_TIMEOUT: Duration = Duration::from_secs(2);
const LOG_STATE_TIMEOUT: Duration = Duration::from_secs(5);

const NETWORK_BATCH_SIZE: usize = 100;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage<T: DeltaCRDT> {
    VersionVectorMessage(VersionVector),
    DeltaMessage(T::Delta),
}

pub struct Node<T: DeltaCRDT> {
    pid: Pid,
    crdt: T,
    peers: Vec<Pid>,
    incoming_messages: Receiver<(Pid, ClusterMessage<T>)>,
    outgoing_messages: Sender<(Pid, ClusterMessage<T>)>,
}

impl<T: DeltaCRDT> Node<T> {
    pub async fn new(pid: Pid, peers: Vec<Pid>, crdt: T) -> Self {
        let (incoming_messages, outgoing_messages) =
            launch::<ClusterMessage<T>>(pid, peers.clone()).await;
        Self {
            pid,
            crdt,
            peers,
            incoming_messages,
            outgoing_messages,
        }
    }

    pub async fn run(&mut self) {
        let mut rng = rand::thread_rng();
        let mut poll_interval = tokio::time::interval(POLL_TIMEOUT);
        let mut update_interval = tokio::time::interval(UPDATE_TIMEOUT);
        let mut log_state_interval = tokio::time::interval(LOG_STATE_TIMEOUT);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    let rand_peer = self.peers.iter().choose(&mut rng).unwrap();
                    let _ = self.outgoing_messages.send((*rand_peer, ClusterMessage::VersionVectorMessage(self.crdt.get_version_vector().clone()))).await;
                },
                _ = log_state_interval.tick() => {
                    let test = self.crdt.show_state();
                },
                 _ = update_interval.tick() => {
                    let update = self.crdt.generate_random_update();
                    self.crdt.update(update);
                },
                _ = async {
                    self.incoming_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
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
                ClusterMessage::VersionVectorMessage(version_vector) => {
                    if let Some(d) = self.crdt.get_delta(&version_vector) {
                        let msg = ClusterMessage::DeltaMessage(d.clone());
                        let _ = self.outgoing_messages.send((sender, msg)).await;
                    }
                }
                ClusterMessage::DeltaMessage(delta) => {
                    self.crdt.merge_delta(delta);
                    self.crdt.get_version_vector_mut().increment(self.pid);
                },
            }
        }
    }
}
