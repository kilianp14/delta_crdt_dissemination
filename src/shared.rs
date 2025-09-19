use serde::{Deserialize, Serialize};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
pub use tokio_serde::{formats::Bincode, Framed};
use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};
use crate::crdt::{DeltaCRDT, VersionVector};
use crate::or_set::OrSet;

pub type Pid = u32;
pub type Counter = u64;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TestbedConfig {
    pub server_id: Pid,
    pub nodes: Vec<Pid>,
}

pub type FromNodeConnection = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ClusterMessage<OrSet<i32>>,
    (),
    Bincode<ClusterMessage<OrSet<i32>>, ()>,
>;
pub type ToNodeConnection = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    ClusterMessage<OrSet<i32>>,
    Bincode<(), ClusterMessage<OrSet<i32>>>,
>;

pub type RegistrationConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    RegistrationMessage,
    RegistrationMessage,
    Bincode<RegistrationMessage, RegistrationMessage>,
>;

pub fn frame_registration_connection(stream: TcpStream) -> RegistrationConnection {
    let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    Framed::new(length_delimited, Bincode::default())
}

pub fn frame_cluster_connection(stream: TcpStream) -> (FromNodeConnection, ToNodeConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromNodeConnection::new(stream, Bincode::default()),
        ToNodeConnection::new(sink, Bincode::default()),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RegistrationMessage {
    NodeRegister(Pid),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage<T: DeltaCRDT> {
    VersionVectorMessage(VersionVector),
    DeltaMessage(T::Delta),
}