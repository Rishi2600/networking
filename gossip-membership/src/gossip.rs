/// Gossip round logic — peer selection and message construction.
///
/// This module is pure logic with no I/O. It operates on the membership table
/// and produces `Message` values; the event loop is responsible for sending them.
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::time::SystemTime;

use crate::membership::MembershipTable;
use crate::message::{build_gossip, Message};
use crate::node::NodeId;

/// Pick a random live peer from the membership table, excluding `self_id`.
///
/// Uses `DefaultHasher` + `SystemTime` for pseudorandomness (no `rand` dep),
/// matching the ISN-generation pattern from tcp-over-udp's `connection.rs`.
///
/// Returns `None` if no live peers are known.
pub fn pick_random_peer(
    table: &MembershipTable,
    self_id: NodeId,
) -> Option<(NodeId, SocketAddr)> {
    let live: Vec<(NodeId, SocketAddr)> = table
        .entries
        .values()
        .filter(|e| {
            e.node_id != self_id
                && matches!(
                    e.status,
                    crate::node::NodeStatus::Alive | crate::node::NodeStatus::Suspect
                )
        })
        .map(|e| (e.node_id, e.addr))
        .collect();

    if live.is_empty() {
        return None;
    }

    // Cheap pseudorandom index.
    let mut h = DefaultHasher::new();
    SystemTime::now().hash(&mut h);
    self_id.hash(&mut h);
    let idx = (h.finish() as usize) % live.len();
    Some(live[idx])
}

/// Pick up to `k` random live peers, excluding `self_id` and `exclude`.
/// Used to select indirect-probe intermediaries.
pub fn pick_k_random_peers(
    table: &MembershipTable,
    self_id: NodeId,
    exclude: NodeId,
    k: usize,
) -> Vec<(NodeId, SocketAddr)> {
    let mut live: Vec<(NodeId, SocketAddr)> = table
        .entries
        .values()
        .filter(|e| {
            e.node_id != self_id
                && e.node_id != exclude
                && matches!(
                    e.status,
                    crate::node::NodeStatus::Alive | crate::node::NodeStatus::Suspect
                )
        })
        .map(|e| (e.node_id, e.addr))
        .collect();

    if live.is_empty() {
        return vec![];
    }

    // Shuffle by rotating a hash-derived offset.
    let mut h = DefaultHasher::new();
    SystemTime::now().hash(&mut h);
    (self_id, exclude).hash(&mut h);
    let offset = (h.finish() as usize) % live.len();
    live.rotate_left(offset);
    live.truncate(k);
    live
}

/// Build the GOSSIP message to broadcast this round.
pub fn build_gossip_message(
    table: &MembershipTable,
    sender_id: NodeId,
    sender_heartbeat: u32,
    sender_incarnation: u32,
    fanout: usize,
) -> Message {
    let entries = table.gossip_wire_entries(fanout);
    build_gossip(sender_id, sender_heartbeat, sender_incarnation, entries)
}
