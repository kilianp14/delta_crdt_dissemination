use crate::node::Node;
use crate::or_set::OrSet;
use crate::shared::TestbedConfig;
use std::{env, fs};

mod crdt;
mod network;
mod node;
mod or_set;
mod shared;

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
    let crdt: OrSet<i32> = OrSet::new(server_config.server_id);
    let mut node: Node<OrSet<i32>> =
        Node::new(server_config.server_id, server_config.peers, crdt).await;
    node.run().await;
}
/*
async fn handle_cluster_messages(cluster_messages: &mut Vec<(Pid, ClusterMessage)>) {
    for msg in cluster_messages.drain(..) {
        info!("Received message: {:?}", msg);
    }
}*/
