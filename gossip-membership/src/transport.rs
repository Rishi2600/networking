/// UDP transport layer.
///
/// A thin wrapper around `tokio::net::UdpSocket` that encodes/decodes
/// `Message` values. Malformed or corrupted datagrams are silently discarded
/// (the call loops and waits for the next datagram), matching the behaviour
/// of `SimulatedSocket` in tcp-over-udp.
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;

use crate::message::{Message, MessageError};

/// Practical UDP MTU ceiling — avoids fragmentation on most Ethernet paths.
const MAX_DATAGRAM: usize = 1_472;

// ── Errors ────────────────────────────────────────────────────────────────────
#[derive(Debug)]
pub enum TransportError {
    Io(std::io::Error),
    Message(MessageError),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Message(e) => write!(f, "message: {e}"),
        }
    }
}

impl std::error::Error for TransportError {}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

// ── Transport ─────────────────────────────────────────────────────────────────
pub struct Transport {
    pub local_addr: SocketAddr,
    socket: Arc<UdpSocket>,
}

impl Transport {
    /// Bind a UDP socket to `addr`.
    pub async fn bind(addr: SocketAddr) -> Result<Self, TransportError> {
        let socket = UdpSocket::bind(addr).await?;
        let local_addr = socket.local_addr()?;
        Ok(Self {
            local_addr,
            socket: Arc::new(socket),
        })
    }

    /// Clone the underlying socket handle (cheap — it's an `Arc`).
    pub fn clone_socket(&self) -> Arc<UdpSocket> {
        self.socket.clone()
    }

    /// Encode `msg` and transmit to `dest`.
    pub async fn send_to(&self, msg: &Message, dest: SocketAddr) -> Result<(), TransportError> {
        let bytes = msg.encode().map_err(TransportError::Message)?;
        self.socket.send_to(&bytes, dest).await?;
        Ok(())
    }

    /// Wait for the next well-formed datagram.
    ///
    /// Datagrams that fail checksum verification or are structurally malformed
    /// are silently dropped and the loop continues — gossip is best-effort.
    pub async fn recv_from(&self) -> Result<(Message, SocketAddr), TransportError> {
        let mut buf = vec![0u8; MAX_DATAGRAM];
        loop {
            let (n, from) = self.socket.recv_from(&mut buf).await?;
            match Message::decode(&buf[..n]) {
                Ok(msg) => return Ok((msg, from)),
                Err(e) => {
                    log::debug!("[transport] dropped malformed datagram from {from}: {e}");
                    // Continue — treat as a lost packet.
                }
            }
        }
    }
}
