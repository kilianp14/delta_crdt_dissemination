use std::{env, fs};
use crate::node::Node;
use crate::or_set::OrSet;
use crate::shared::TestbedConfig;

mod crdt;
mod or_set;
mod shared;
mod network;
mod node;

const NETWORK_BATCH_SIZE: usize = 100;

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
    //let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
    let network = network::Network::new(server_config.server_id, server_config.nodes, NETWORK_BATCH_SIZE).await;
    let crdt: OrSet<i32> = OrSet::new(server_config.server_id);
    let mut node: Node<OrSet<i32>> = Node::new(network, server_config.server_id, crdt);
    node.run().await;
}
/*
async fn handle_cluster_messages(cluster_messages: &mut Vec<(Pid, ClusterMessage)>) {
    for msg in cluster_messages.drain(..) {
        info!("Received message: {:?}", msg);
    }
}*/