use crate::shared::Pid;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    net::{SocketAddr, ToSocketAddrs},
};
use tokio::{
    io::{
        self, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader, Lines, ReadHalf, WriteHalf,
    },
    net::{TcpListener, TcpStream},
    sync::mpsc::{channel, Receiver, Sender},
};

pub struct Network<T> {
    pub peers: Vec<Pid>,
    pub cluster_messages: Receiver<(Pid, T)>,
    peer_senders: HashMap<Pid, Sender<T>>,
    listener: TcpListener,
    pid: Pid,
    address: SocketAddr,
}

impl<T: Send + 'static + Serialize + DeserializeOwned + Debug + Sync + Clone> Network<T> {
    pub async fn new(pid: Pid, peers: Vec<Pid>) -> Self {
        let address = get_node_addr(pid).unwrap();
        let (msg_sender, msg_receiver) = channel::<(Pid, T)>(1000);
        let listener = TcpListener::bind(address)
            .await
            .expect("Failed to bind to internal address");

        let mut net = Network {
            peers: Vec::new(),
            cluster_messages: msg_receiver,
            peer_senders: HashMap::new(),
            listener,
            pid,
            address,
        };

        // Connect eagerly to known peers
        for id in peers {
            let peer_address = get_node_addr(id).unwrap();
            net.connect_to_peer(peer_address, msg_sender.clone()).await;
        }

        net
    }

    /// Accept one incoming connection (to be polled by the caller in a loop).
    pub async fn accept_incoming(&mut self) {
        let (stream, _) = self.listener.accept().await.unwrap();
        let (msg_sender, _) = channel::<(Pid, T)>(1000); // re-use outer sender
        self.handle_new_stream(stream, msg_sender).await;
    }

    async fn connect_to_peer(&mut self, address: SocketAddr, msg_sender: Sender<(Pid, T)>) {
        if address == self.address || address < self.address {
            return;
        }
        if let Ok(stream) = TcpStream::connect(address).await {
            self.handle_new_stream(stream, msg_sender).await;
        }
    }

    async fn handle_new_stream(&mut self, mut stream: TcpStream, msg_sender: Sender<(Pid, T)>) {
        stream.write_u32(self.pid).await.unwrap();
        let pid = stream.read_u32().await.unwrap();

        let (read_half, stream_writer) = io::split(stream);
        let stream_reader = BufReader::new(read_half).lines();

        let (out_sender, out_receiver) = channel::<T>(100);
        self.peers.push(pid);
        self.peer_senders.insert(pid, out_sender);

        tokio::spawn(async move {
            Self::read_loop(stream_reader, pid, msg_sender).await;
        });
        tokio::spawn(async move {
            Self::write_loop(stream_writer, out_receiver).await;
        });
    }

    async fn read_loop(
        mut reader: Lines<BufReader<ReadHalf<TcpStream>>>,
        peer: Pid,
        msg_sender: Sender<(Pid, T)>,
    ) {
        while let Ok(Some(line)) = reader.next_line().await {
            let message: T = serde_json::from_str(&line).unwrap();
            msg_sender.send((peer, message)).await.unwrap();
        }
    }

    async fn write_loop(mut writer: WriteHalf<TcpStream>, mut receiver: Receiver<T>) {
        while let Some(request) = receiver.recv().await {
            let json = serde_json::to_string(&request).unwrap();
            let _ = writer.write_all(json.as_bytes()).await;
            let _ = writer.write_all(b"\\n").await;
        }
    }

    pub async fn send_to_cluster(&self, peer: Pid, message: T) {
        if let Some(sender) = self.peer_senders.get(&peer) {
            let _ = sender.send(message).await;
        }
    }

    pub async fn send_to_all(&self, message: T) {
        for sender in self.peer_senders.values() {
            let _ = sender.send(message.clone()).await;
        }
    }
}

fn get_node_addr(node: Pid) -> Result<SocketAddr, std::io::Error> {
    let node_port = 8000 + node as u16;
    let dns_name: String = format!("s{node}:{node_port}");
    let address = dns_name.to_socket_addrs()?.next().unwrap();
    Ok(address)
}
