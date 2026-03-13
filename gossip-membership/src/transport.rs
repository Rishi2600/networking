/// UDP transport layer.
///
/// A thin wrapper around `tokio::net::UdpSocket` that encodes/decodes
/// `Message` values.  When a `ClusterKey` is configured, all outgoing
/// datagrams are encrypted with ChaCha20-Poly1305 and incoming datagrams
/// are decrypted and authenticated before being returned.
///
/// Malformed, corrupted, or unauthenticated datagrams are silently
/// discarded (the call loops and waits for the next datagram), matching
/// the behaviour of `SimulatedSocket` in tcp-over-udp.
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::UdpSocket;

use crate::crypto::{ClusterKey, CryptoError};
use crate::message::{Message, MessageError};

/// Practical UDP MTU ceiling — avoids fragmentation on most Ethernet paths.
const MAX_DATAGRAM: usize = 1_472;

// ── Errors ────────────────────────────────────────────────────────────────────
#[derive(Debug)]
pub enum TransportError {
    Io(std::io::Error),
    Message(MessageError),
    Crypto(CryptoError),
    /// Cleartext sender_id (AAD) does not match the sender_id inside the
    /// decrypted message — possible replay or forgery attempt.
    AuthenticationFailed,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Message(e) => write!(f, "message: {e}"),
            Self::Crypto(e) => write!(f, "crypto: {e}"),
            Self::AuthenticationFailed => write!(f, "sender authentication failed"),
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
    key: Option<ClusterKey>,
}

impl Transport {
    /// Bind a UDP socket to `addr` (plaintext mode — no encryption).
    pub async fn bind(addr: SocketAddr) -> Result<Self, TransportError> {
        let socket = UdpSocket::bind(addr).await?;
        let local_addr = socket.local_addr()?;
        Ok(Self {
            local_addr,
            socket: Arc::new(socket),
            key: None,
        })
    }

    /// Attach a cluster key, enabling encryption on all subsequent I/O.
    pub fn with_key(mut self, key: ClusterKey) -> Self {
        self.key = Some(key);
        self
    }

    /// Clone the underlying socket handle (cheap — it's an `Arc`).
    pub fn clone_socket(&self) -> Arc<UdpSocket> {
        self.socket.clone()
    }

    /// Returns `true` if encryption is enabled.
    pub fn is_encrypted(&self) -> bool {
        self.key.is_some()
    }

    /// Encode `msg` and transmit to `dest`.
    ///
    /// When a cluster key is configured the encoded bytes are encrypted
    /// with ChaCha20-Poly1305 before being sent.  The sender's node ID
    /// is bound as Additional Authenticated Data (AAD).
    pub async fn send_to(&self, msg: &Message, dest: SocketAddr) -> Result<(), TransportError> {
        let encoded = msg.encode().map_err(TransportError::Message)?;
        let wire_bytes = match &self.key {
            Some(key) => key
                .encrypt(&encoded, msg.sender_id)
                .map_err(TransportError::Crypto)?,
            None => encoded,
        };
        self.socket.send_to(&wire_bytes, dest).await?;
        Ok(())
    }

    /// Wait for the next well-formed (and, if encrypted, authenticated) datagram.
    ///
    /// Datagrams that fail decryption, checksum verification, or structural
    /// validation are silently dropped and the loop continues — gossip is
    /// best-effort.
    pub async fn recv_from(&self) -> Result<(Message, SocketAddr), TransportError> {
        let mut buf = vec![0u8; MAX_DATAGRAM];
        loop {
            let (n, from) = self.socket.recv_from(&mut buf).await?;
            match self.decode_datagram(&buf[..n]) {
                Ok(msg) => return Ok((msg, from)),
                Err(e) => {
                    log::debug!("[transport] dropped datagram from {from}: {e}");
                }
            }
        }
    }

    /// Decrypt (if needed) and decode a raw datagram.
    fn decode_datagram(&self, raw: &[u8]) -> Result<Message, TransportError> {
        match &self.key {
            Some(key) => {
                let (plaintext, aad_sender_id) =
                    key.decrypt(raw).map_err(TransportError::Crypto)?;
                let msg = Message::decode(&plaintext).map_err(TransportError::Message)?;
                // Verify the AAD-authenticated sender_id matches the one
                // inside the decrypted message.  A mismatch means either a
                // bug or an attempted forgery.
                if msg.sender_id != aad_sender_id {
                    return Err(TransportError::AuthenticationFailed);
                }
                Ok(msg)
            }
            None => Message::decode(raw).map_err(TransportError::Message),
        }
    }
}
