use crate::{
    crdt::{DeltaCRDT, VersionVector},
    network,
    shared::{now_micros, DisseminationStrategy, JitteredInterval, Pid},
};
use csv::Writer;
use rand::{rngs::ThreadRng, seq::IteratorRandom};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::sleep,
};

const POLL_TIMEOUT: Duration = Duration::from_millis(500);
const ADDITIONAL_SYNC_TIME: Duration = Duration::from_secs(20);
const NUMBER_OF_UPDATES: u64 = 100;
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
    // Sender id, message, message size in bytes
    incoming_messages: Receiver<(Pid, ClusterMessage<T>, usize)>,
    outgoing_messages: Sender<(Pid, ClusterMessage<T>)>,
    strategy: DisseminationStrategy,
    rng: ThreadRng,
    number_of_updates: u64,
    update_interval: Duration,
    get_delta_times: Vec<u128>,
    merge_delta_times: Vec<u128>,
    received_message_sizes: Vec<(usize, bool)>,
    version_vector_states: Vec<(u128, VersionVector)>,
    data_dir: PathBuf,
}

impl<T: DeltaCRDT> Node<T> {
    pub async fn new(
        pid: Pid,
        peers: Vec<Pid>,
        crdt: T,
        strategy: DisseminationStrategy,
        update_interval: Duration,
        data_dir: PathBuf,
    ) -> Self {
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
            update_interval,
            get_delta_times: Vec::with_capacity(1000),
            merge_delta_times: Vec::with_capacity(1000),
            received_message_sizes: Vec::with_capacity(10000),
            version_vector_states: Vec::with_capacity(1000),
            data_dir,
        }
    }

    pub async fn run(&mut self) {
        let is_pull = matches!(
            self.strategy,
            DisseminationStrategy::Reactive | DisseminationStrategy::Hybrid,
        );

        let mut poll_interval = JitteredInterval::new(POLL_TIMEOUT, POLL_TIMEOUT);
        let mut update_interval = JitteredInterval::new(self.update_interval, self.update_interval);

        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        loop {
            tokio::select! {
                _ = poll_interval.tick(), if is_pull => {
                    let rand_peer = self.peers.iter().choose(&mut self.rng).unwrap();
                    let _ = self.outgoing_messages.send((*rand_peer, ClusterMessage::DeltaRequest(self.version_vector.clone()))).await;
                },
                 _ = update_interval.tick() => {
                    if self.number_of_updates >= NUMBER_OF_UPDATES {
                        break;
                    }
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
                    self.version_vector_states.push((now_micros(), self.version_vector.clone()));
                },
                _ = async {
                    self.incoming_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
            }
        }

        // Give servers a bit more time to sync
        loop {
            tokio::select! {
                _ = sleep(ADDITIONAL_SYNC_TIME) => {
                        break;
                }
                _ = async {
                    self.incoming_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
            }
        }
        self.save_metrics().await.expect("Saving metrics failed");
    }

    async fn handle_cluster_messages(
        &mut self,
        cluster_messages: &mut Vec<(Pid, ClusterMessage<T>, usize)>,
    ) {
        for (sender, msg, msg_size) in cluster_messages.drain(..) {
            match msg {
                ClusterMessage::Info(version_vector) => {
                    if !matches!(self.strategy, DisseminationStrategy::Proactive) {
                        continue;
                    }
                    if version_vector <= self.version_vector {
                        self.received_message_sizes.push((msg_size, true));
                        continue;
                    }
                    self.received_message_sizes.push((msg_size, false));
                    let _ = self
                        .outgoing_messages
                        .send((
                            sender,
                            ClusterMessage::DeltaRequest(self.version_vector.clone()),
                        ))
                        .await;
                }

                ClusterMessage::DeltaRequest(version_vector) => {
                    if version_vector >= self.version_vector {
                        self.received_message_sizes.push((msg_size, true));
                        continue;
                    }
                    self.received_message_sizes.push((msg_size, false));

                    let start = now_micros();
                    let delta = self.crdt.get_delta(&version_vector);
                    let end = now_micros();
                    self.get_delta_times.push(end - start);

                    let msg = ClusterMessage::Delta(delta, self.version_vector.clone());
                    let _ = self.outgoing_messages.send((sender, msg)).await;
                }

                ClusterMessage::Delta(delta, version_vector) => {
                    if version_vector <= self.version_vector {
                        self.received_message_sizes.push((msg_size, true));
                        continue;
                    }

                    self.received_message_sizes.push((msg_size, false));
                    let start = now_micros();
                    self.crdt.merge_delta(delta);
                    let end = now_micros();
                    self.merge_delta_times.push(end - start);

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
                            let start = now_micros();
                            let delta = self.crdt.get_delta(&version_vector);
                            let end = now_micros();
                            self.get_delta_times.push(end - start);

                            let msg = ClusterMessage::Delta(delta, self.version_vector.clone());
                            let _ = self.outgoing_messages.send((sender, msg)).await;
                        }
                        _ => {}
                    }

                    self.version_vector_states
                        .push((now_micros(), self.version_vector.clone()));
                }
            }
        }
    }

    async fn save_metrics(&self) -> std::io::Result<()> {
        // Save get_delta_times
        let mut wtr = Writer::from_path(self.data_dir.join("get_delta_times.csv"))?;
        wtr.write_record(["time_micros"])?;
        for t in &self.get_delta_times {
            wtr.write_record(&[t.to_string()])?;
        }
        wtr.flush()?;

        // Save merge_delta_times
        let mut wtr = Writer::from_path(self.data_dir.join("merge_delta_times.csv"))?;
        wtr.write_record(["time_micros"])?;
        for t in &self.merge_delta_times {
            wtr.write_record(&[t.to_string()])?;
        }
        wtr.flush()?;

        // Save received_message_sizes
        let mut wtr = Writer::from_path(self.data_dir.join("received_message_sizes.csv"))?;
        wtr.write_record(["size_bytes", "redundant"])?;
        for (size, redundant) in &self.received_message_sizes {
            wtr.write_record(&[size.to_string(), redundant.to_string()])?;
        }
        wtr.flush()?;

        // Save version_vector_states
        let mut wtr = Writer::from_path(self.data_dir.join("version_vector_states.csv"))?;
        // Assuming VersionVector implements Display or Debug
        let vv_set = self.version_vector.get_set();
        let mut all_process_keys = vv_set.keys()
            .collect::<Vec<_>>();
        all_process_keys.sort();
        let mut keys_as_strings = all_process_keys.iter().map(|k| k.to_string()).collect::<Vec<_>>();
        keys_as_strings.insert(0, String::from("timestamp_micros"));
        wtr.write_record(keys_as_strings)?;
        for (ts, vv) in &self.version_vector_states {
            let sorted_keys = all_process_keys.clone();
            let mut version_vector_values = Vec::with_capacity(sorted_keys.len());
            for k in sorted_keys {
                version_vector_values.push(vv.get_set().get(k).unwrap_or(&0).to_string());
            }
            version_vector_values.insert(0, ts.to_string());
            wtr.write_record(version_vector_values)?;
        }
        wtr.flush()?;

        Ok(())
    }
}
