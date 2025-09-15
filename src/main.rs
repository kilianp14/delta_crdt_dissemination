use std::{env, fs};
use crate::shared::TestbedConfig;

mod crdt;
mod or_set;
mod shared;
mod network;

fn main() {
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
}
