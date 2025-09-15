use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio::sync::mpsc::{Sender, Receiver, UnboundedSender};
use tokio_util::sync::CancellationToken;
use log::{error, info, warn};
use tokio::io::AsyncWriteExt;
use tokio::task::JoinHandle;
use crate::shared::{frame_cluster_connection, frame_registration_connection, ClusterMessage, Pid, RegistrationMessage};

pub struct Network {
    id: Pid,
    peers: Vec<Pid>,
    peer_connections: Arc<Mutex<Vec<Option<PeerConnection>>>>,
    batch_size: usize,
    pub cluster_message_sender: Sender<(Pid, ClusterMessage)>,
    pub cluster_messages: Arc<Mutex<Receiver<(Pid, ClusterMessage)>>>,
    cancel_token: CancellationToken,
}

impl Network {
    // Creates a new network with connections other server nodes in the cluster and any clients.
    // Waits until connections to all servers and clients are established before resolving.
    pub async fn new(
        id: Pid,
        nodes: Vec<Pid>,
        batch_size: usize,
    ) -> Self {
        let peers: Vec<u32> = nodes.iter().cloned().filter(|node| *node != id).collect();
        let mut cluster_connections = vec![];
        cluster_connections.resize_with(peers.len(), Default::default);
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(batch_size);
        let mut network = Self {
            id,
            peers,
            peer_connections: Arc::new(Mutex::new(cluster_connections)),
            batch_size,
            cluster_message_sender,
            cluster_messages: Arc::new(Mutex::new(cluster_messages)),
            cancel_token: CancellationToken::new(),
        };
        network.initialize_connections().await;
        network
    }

    async fn initialize_connections(&mut self) {
        let (connection_sink, mut connection_source) = mpsc::channel(30);
        let listener_handle = self.spawn_connection_listener(connection_sink.clone());
        self.spawn_peer_connectors(connection_sink.clone());
        while let Some(new_connection) = connection_source.recv().await {
            let peer_idx = self.cluster_id_to_idx(new_connection.peer_id).unwrap();
            let peer_conns_clone = Arc::clone(&self.peer_connections);
            let mut peer_connections = peer_conns_clone.lock().await;
            peer_connections[peer_idx] = Some(new_connection);
            let peer_conns_clone = Arc::clone(&self.peer_connections);
            let peer_connections = peer_conns_clone.lock().await;
            let all_cluster_connected = peer_connections.iter().all(|c| c.is_some());
            if all_cluster_connected {
                listener_handle.abort();
                break;
            }
        }
    }

    fn spawn_connection_listener(
        &self,
        connection_sender: Sender<PeerConnection>,
    ) -> tokio::task::JoinHandle<()> {
        let port = 8000 + self.id as u16;
        let listening_address = SocketAddr::from(([0, 0, 0, 0], port));
        let cluster_sender = self.cluster_message_sender.clone();
        let batch_size = self.batch_size;
        let cancel_token = self.cancel_token.clone();
        tokio::spawn(async move {
            let listener = TcpListener::bind(listening_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        info!("New connection from {socket_addr}");
                        tcp_stream.set_nodelay(true).unwrap();
                        tokio::spawn(Self::handle_incoming_connection(
                            tcp_stream,
                            Some(cluster_sender.clone()),
                            connection_sender.clone(),
                            batch_size,
                            cancel_token.clone(),
                        ));
                    }
                    Err(e) => error!("Error listening for new connection: {:?}", e),
                }
            }
        })
    }

    async fn handle_incoming_connection(
        connection: TcpStream,
        cluster_message_sender: Option<Sender<(Pid, ClusterMessage)>>,
        connection_sender: Sender<PeerConnection>,
        batch_size: usize,
        cancel_token: CancellationToken,
    ) {
        // Identify connector's ID and type by handshake
        let mut registration_connection = frame_registration_connection(connection);
        let registration_message = registration_connection.next().await;
        let new_connection = match registration_message {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                match cluster_message_sender {
                    Some(sender) => {
                        Some(PeerConnection::new(
                            node_id,
                            underlying_stream,
                            batch_size,
                            sender,
                            cancel_token,
                        ))
                    }
                    None => None
                }
            }
            Some(Err(err)) => {
                error!("Error deserializing handshake: {:?}", err);
                return;
            }
            None => {
                info!("Connection to unidentified source dropped");
                return;
            }
        };
        match new_connection {
            Some(connection) => {
                connection_sender.send(connection).await.unwrap();
            }
            None => {}
        }
    }

    fn spawn_peer_connectors(&self, connection_sender: Sender<PeerConnection>) {
        let my_id = self.id;
        let peers_to_contact: Vec<Pid> =
            self.peers.iter().cloned().filter(|&p| p > my_id).collect();
        for peer in peers_to_contact {
            let to_address = match get_node_addr(peer) {
                Ok(addr) => addr,
                Err(e) => {
                    error!("Error resolving DNS name of node {peer}: {e}");
                    return;
                }
            };
            let reconnect_delay = Duration::from_secs(1);
            let mut reconnect_interval = tokio::time::interval(reconnect_delay);
            let cluster_sender = self.cluster_message_sender.clone();
            let connection_sender = connection_sender.clone();
            let batch_size = self.batch_size;
            let cancel_token = self.cancel_token.clone();
            tokio::spawn(async move {
                // Establish connection
                let peer_connection = loop {
                    reconnect_interval.tick().await;
                    match TcpStream::connect(to_address).await {
                        Ok(connection) => {
                            info!("New connection to node {peer}");
                            connection.set_nodelay(true).unwrap();
                            break connection;
                        }
                        Err(err) => {
                            error!("Establishing connection to node {peer} failed: {err}")
                        }
                    }
                };
                // Send handshake
                let mut registration_connection = frame_registration_connection(peer_connection);
                let handshake = RegistrationMessage::NodeRegister(my_id);
                if let Err(err) = registration_connection.send(handshake).await {
                    error!("Error sending handshake to {peer}: {err}");
                    return;
                }
                let underlying_stream = registration_connection.into_inner().into_inner();
                // Create connection actor
                let peer_actor = PeerConnection::new(
                    peer,
                    underlying_stream,
                    batch_size,
                    cluster_sender,
                    cancel_token,
                );
                connection_sender.send(peer_actor).await.unwrap();
            });
        }
    }

    pub async fn send_to_cluster(&self, to: Pid, msg: ClusterMessage) {
        let peer_conns_clone = Arc::clone(&self.peer_connections);
        let mut peer_connections = peer_conns_clone.lock().await;
        match self.cluster_id_to_idx(to) {
            Some(idx) => match peer_connections[idx] {
                Some(ref mut connection) => {
                    if let Err(err) = connection.send(msg) {
                        warn!("Couldn't send msg to peer {to}: {err}");
                        peer_connections[idx] = None;
                    }
                }
                None => warn!("Not connected to node {to}"),
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    // Removes all peer connections, but waits for queued writes to the peers to finish first
    #[allow(dead_code)]
    pub async fn shutdown(&mut self) {
        self.cancel_token.cancel();
        let peer_conns_clone = Arc::clone(&self.peer_connections);
        let mut peer_connections = peer_conns_clone.lock().await;
        for peer_connection in peer_connections.drain(..) {
            if let Some(connection) = peer_connection {
                connection.wait_for_writes_and_shutdown().await;
            }
        }
        for _ in 0..self.peers.len() {
            peer_connections.push(None);
        }
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: Pid) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}

fn get_node_addr(
    node: Pid,
) -> Result<SocketAddr, std::io::Error> {
    let node_port = 8000 + node as u16;
    let dns_name: String = format!("s{node}:{node_port}");
    let address = dns_name.to_socket_addrs()?.next().unwrap();
    Ok(address)
}

struct PeerConnection {
    peer_id: Pid,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ClusterMessage>,
}

impl PeerConnection {
    pub fn new(
        peer_id: Pid,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(Pid, ClusterMessage)>,
        cancel_token: CancellationToken,
    ) -> Self {
        let (reader, mut writer) = frame_cluster_connection(connection);
        // Reader Actor
        let _reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            if let Err(_) = incoming_messages.send((peer_id, m)).await {
                                break;
                            };
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                        }
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            loop {
                tokio::select! {
                    biased;
                    num_messages = message_rx.recv_many(&mut buffer, batch_size) => {
                        if num_messages == 0 { break; }
                        for msg in buffer.drain(..) {
                            if let Err(err) = writer.feed(msg).await {
                                error!("Couldn't send message to node {peer_id}: {err}");
                                break;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't send message to node {peer_id}: {err}");
                            break;
                        }
                    },
                    _ = cancel_token.cancelled() => {
                        // Try to empty the outgoing message queue before exiting
                        while let Ok(msg) = message_rx.try_recv() {
                            if let Err(err) = writer.feed(msg).await {
                                error!("Couldn't send message to node {peer_id}: {err}");
                                break;
                            }
                        }
                        if let Err(err) = writer.flush().await {
                            error!("Couldn't send message to node {peer_id}: {err}");
                            break;
                        }

                        // Gracefully shut down the writing half of the connection
                        let mut underlying_socket = writer.into_inner().into_inner();
                        if let Err(err) = underlying_socket.shutdown().await {
                            error!("Error shutting down the stream to node {peer_id}: {err}");
                        }
                        break;
                    }
                }
            }
            info!("Connection to node {peer_id} closed");
        });
        PeerConnection {
            peer_id,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ClusterMessage,
    ) -> Result<(), mpsc::error::SendError<ClusterMessage>> {
        self.outgoing_messages.send(msg)
    }

    #[allow(dead_code)]
    async fn wait_for_writes_and_shutdown(self) {
        let _ = tokio::time::timeout(Duration::from_secs(5), self.writer_task).await;
    }
}