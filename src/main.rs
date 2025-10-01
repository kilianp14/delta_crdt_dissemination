use crate::shared::FanOut;
use crate::{
    node::Node,
    or_set::OrSet,
    shared::{to_absolute, DisseminationStrategy, NetworkConfig, Pid},
};
use std::{env, fs, str::FromStr, time::Duration};

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
    let data_dir = match env::var("DATA_DIR") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires DATA_DIR environment variable"),
    };
    let dissemination_strategy = match env::var("DISSEMINATION_STRATEGY") {
        Ok(dis_str) => DisseminationStrategy::from_str(&dis_str)
            .expect("Invalid dissemination strategy: {dis_str}"),
        Err(_) => panic!("Requires DISSEMINATION_STRATEGY environment variable"),
    };
    let fan_out = match env::var("FAN_OUT") {
        Ok(f) => FanOut::from_str(&f).expect("Invalid fan out: {f}"),
        Err(_) => panic!("Requires FAN_OUT environment variable"),
    };
    let update_interval_millis: u64 = env::var("UPDATE_INTERVAL")
        .expect("Missing UPDATE_INTERVAL")
        .parse::<u64>()
        .expect("UPDATE_INTERVAL must be valid");
    let pid_string = env::var("SERVER_ID").expect("Missing SERVER_ID");
    let config_string = fs::read_to_string(config_file).unwrap();
    let server_config: NetworkConfig = toml::from_str(&config_string).unwrap();

    let peers = server_config
        .servers
        .get(&pid_string)
        .ok_or("Invalid pid")
        .unwrap();
    let pid: Pid = pid_string.parse().unwrap();
    let crdt: OrSet<i32> = OrSet::new(pid);
    let mut node: Node<OrSet<i32>> = Node::new(
        pid,
        peers.clone(),
        crdt,
        dissemination_strategy,
        fan_out.get_fanout(peers.len()),
        Duration::from_millis(update_interval_millis),
        to_absolute(data_dir),
    )
    .await;
    node.run().await;
}
