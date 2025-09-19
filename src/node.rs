use crate::crdt::{DeltaCRDT, VersionVector};
use crate::network::Network;
use crate::shared::Pid;
use rand::seq::IteratorRandom;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const POLL_TIMEOUT: Duration = Duration::from_millis(100);
const UPDATE_TIMEOUT: Duration = Duration::from_secs(2);
const NETWORK_BATCH_SIZE: usize = 100;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage<T: DeltaCRDT> {
    VersionVectorMessage(VersionVector),
    DeltaMessage(T::Delta),
}

pub struct Node<T: DeltaCRDT> {
    server_id: Pid,
    network: Network<ClusterMessage<T>>,
    crdt: T,
}

impl<T: DeltaCRDT> Node<T> {
    pub fn new(network: Network<ClusterMessage<T>>, server_id: Pid, crdt: T) -> Self {
        Self {
            server_id,
            network,
            crdt,
        }
    }

    pub async fn run(&mut self) {
        let mut rng = rand::thread_rng();
        let mut poll_interval = tokio::time::interval(POLL_TIMEOUT);
        let mut update_interval = tokio::time::interval(UPDATE_TIMEOUT);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut counter = 0;
        loop {
            tokio::select! {
                _ = poll_interval.tick() => {
                    let rand_peer = self.network.peers.iter().choose(&mut rng).unwrap();
                    self.network.send_to_cluster(*rand_peer, ClusterMessage::VersionVectorMessage(self.crdt.get_version_vector().clone())).await;
                },
                 _ = update_interval.tick() => {
                    let update = self.crdt.generate_random_update();
                    self.crdt.update(update);
                },
                _ = async {
                    self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
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
                ClusterMessage::VersionVectorMessage(version_vector) => {}
                ClusterMessage::DeltaMessage(_) => todo!(),
            }
        }
    }
}
