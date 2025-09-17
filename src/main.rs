use std::{env, fs};
use crate::experiment_engine::ExperimentEngine;
use crate::shared::TestbedConfig;

mod crdt;
mod or_set;
mod shared;
mod network;
mod experiment_engine;
mod experiments;

//const NETWORK_BATCH_SIZE: usize = 100;

#[tokio::main]
async fn main() {
    env_logger::init();
    let config_file = match env::var("CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CONFIG_FILE environment variable"),
    };
    let config_string = fs::read_to_string(config_file).unwrap();
    let server_config: TestbedConfig = match toml::from_str(&config_string) {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    println!("{server_config:?}");
    let experiment_engine = ExperimentEngine::new(server_config.experiment.as_str());
    experiment_engine.run();
    /*let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
    let network = network::Network::new(server_config.server_id, server_config.nodes, NETWORK_BATCH_SIZE).await;
    loop {
        network.send_to_cluster((server_config.server_id % 3) + 1, ClusterMessage::TestMessage(server_config.server_id)).await;
        let mut cluster_messages = network.cluster_messages.lock().await;
        cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await;
        handle_cluster_messages(&mut cluster_msg_buf).await;
    }*/
}
/*
async fn handle_cluster_messages(cluster_messages: &mut Vec<(Pid, ClusterMessage)>) {
    for msg in cluster_messages.drain(..) {
        info!("Received message: {:?}", msg);
    }
}*/