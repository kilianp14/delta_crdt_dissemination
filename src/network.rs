use crate::shared::Pid;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};
use tokio::{
    io::{
        self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines, ReadHalf, WriteHalf,
    },
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{channel, Receiver, Sender},
        Mutex,
    },
};

pub async fn launch<T>(pid: Pid, peers: Vec<Pid>) -> (Receiver<(Pid, T)>, Sender<(Pid, T)>)
where
    T: Send + 'static + Serialize + DeserializeOwned + Debug + Sync + Clone,
{
    let address = get_node_addr(pid).unwrap();
    let listener = TcpListener::bind(address)
        .await
        .expect("Failed to bind to address");

    let (cluster_sender, cluster_receiver) = channel::<(Pid, T)>(1000);
    let (user_sender, mut user_receiver) = channel::<(Pid, T)>(1000);

    // Map of peer -> sender to its write loop
    let peer_senders: Arc<Mutex<HashMap<Pid, Sender<T>>>> = Arc::new(Mutex::new(HashMap::new()));

    // Accept incoming connections
    {
        let cluster_sender = cluster_sender.clone();
        let peer_senders = peer_senders.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                handle_new_stream::<T>(stream, pid, cluster_sender.clone(), peer_senders.clone())
                    .await;
            }
        });
    }

    // Connect eagerly to peers
    for id in peers {
        if id == pid {
            continue;
        }
        let cluster_sender = cluster_sender.clone();
        let peer_senders = peer_senders.clone();
        tokio::spawn(async move {
            if let Ok(addr) = get_node_addr(id) {
                if let Ok(stream) = TcpStream::connect(addr).await {
                    handle_new_stream::<T>(stream, pid, cluster_sender, peer_senders).await;
                }
            }
        });
    }

    // Pump user_sender -> actual peer_senders
    tokio::spawn(async move {
        while let Some((peer, msg)) = user_receiver.recv().await {
            if let Some(sender) = peer_senders.lock().await.get(&peer) {
                let _ = sender.send(msg).await;
            }
        }
    });

    (cluster_receiver, user_sender)
}

async fn handle_new_stream<T>(
    mut stream: TcpStream,
    pid: Pid,
    cluster_sender: Sender<(Pid, T)>,
    peer_senders: Arc<Mutex<HashMap<Pid, Sender<T>>>>,
) where
    T: Send + 'static + Serialize + DeserializeOwned + Debug + Sync + Clone,
{
    // Exchange PIDs
    stream.write_u32(pid).await.unwrap();
    let peer_pid = stream.read_u32().await.unwrap();

    let (read_half, write_half) = io::split(stream);
    let reader = BufReader::new(read_half).lines();

    let (out_sender, out_receiver) = channel::<T>(100);
    peer_senders.lock().await.insert(peer_pid, out_sender);

    tokio::spawn(read_loop::<T>(reader, peer_pid, cluster_sender));
    tokio::spawn(write_loop::<T>(write_half, out_receiver));
}

async fn read_loop<T>(
    mut reader: Lines<BufReader<ReadHalf<TcpStream>>>,
    peer: Pid,
    cluster_sender: Sender<(Pid, T)>,
) where
    T: Send + 'static + Serialize + DeserializeOwned + Debug + Sync + Clone,
{
    while let Ok(Some(line)) = reader.next_line().await {
        let message: T = serde_json::from_str(&line).unwrap();
        let _ = cluster_sender.send((peer, message)).await;
    }
}

async fn write_loop<T>(mut writer: WriteHalf<TcpStream>, mut receiver: Receiver<T>)
where
    T: Send + 'static + Serialize + DeserializeOwned + Debug + Sync + Clone,
{
    while let Some(request) = receiver.recv().await {
        let json = serde_json::to_string(&request).unwrap();
        let _ = writer.write_all(json.as_bytes()).await;
        let _ = writer.write_all(b"\n").await;
    }
}

fn get_node_addr(node: Pid) -> Result<SocketAddr, std::io::Error> {
    let node_port = 8000 + node as u16;
    let dns_name: String = format!("s{node}:{node_port}");
    let address = dns_name.to_socket_addrs()?.next().unwrap();
    Ok(address)
}
