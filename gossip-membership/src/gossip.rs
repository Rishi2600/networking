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

/// Pick up to `max_targets` distinct random live peers for gossip this round.
///
/// This is the rate-limiting mechanism: instead of always gossiping to
/// exactly one peer, the event loop calls this once per gossip tick to get
/// the set of targets for the round, bounded by `max_targets` and the
/// number of live peers.
pub fn pick_gossip_targets(
    table: &MembershipTable,
    self_id: NodeId,
    max_targets: usize,
) -> Vec<(NodeId, SocketAddr)> {
    let mut live: Vec<(NodeId, SocketAddr)> = table
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
        return vec![];
    }

    // Shuffle by rotating a hash-derived offset, then truncate.
    let mut h = DefaultHasher::new();
    SystemTime::now().hash(&mut h);
    self_id.hash(&mut h);
    let offset = (h.finish() as usize) % live.len();
    live.rotate_left(offset);
    live.truncate(max_targets);
    live
}

/// Compute the effective gossip fanout (max entries per message).
///
/// When `adaptive` is `true`, scales with cluster size:
/// `base * ceil(log2(n)).max(1)`, so larger clusters carry more entries
/// per message and dissemination still completes in O(log n) rounds.
///
/// When `adaptive` is `false`, returns `base` unchanged.
pub fn effective_fanout(base: usize, cluster_size: usize, adaptive: bool) -> usize {
    if !adaptive || cluster_size <= 2 {
        return base;
    }
    let log2_n = (cluster_size as f64).log2().ceil().max(1.0) as usize;
    base.saturating_mul(log2_n)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use crate::node::NodeState;

    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    // ── effective_fanout tests ────────────────────────────────────────────────

    #[test]
    fn fanout_non_adaptive_returns_base() {
        assert_eq!(effective_fanout(10, 100, false), 10);
        assert_eq!(effective_fanout(10, 1, false), 10);
    }

    #[test]
    fn fanout_adaptive_small_cluster_returns_base() {
        // ≤ 2 nodes: no scaling.
        assert_eq!(effective_fanout(10, 1, true), 10);
        assert_eq!(effective_fanout(10, 2, true), 10);
    }

    #[test]
    fn fanout_adaptive_scales_with_cluster_size() {
        // 8 nodes: ceil(log2(8)) = 3 → 10 * 3 = 30
        assert_eq!(effective_fanout(10, 8, true), 30);
        // 16 nodes: ceil(log2(16)) = 4 → 10 * 4 = 40
        assert_eq!(effective_fanout(10, 16, true), 40);
        // 100 nodes: ceil(log2(100)) = 7 → 10 * 7 = 70
        assert_eq!(effective_fanout(10, 100, true), 70);
    }

    #[test]
    fn fanout_adaptive_three_nodes() {
        // 3 nodes: ceil(log2(3)) = 2 → 10 * 2 = 20
        assert_eq!(effective_fanout(10, 3, true), 20);
    }

    // ── pick_gossip_targets tests ─────────────────────────────────────────────

    #[test]
    fn targets_empty_when_alone() {
        let t = MembershipTable::new(1, make_addr(1000));
        let targets = pick_gossip_targets(&t, 1, 5);
        assert!(targets.is_empty());
    }

    #[test]
    fn targets_capped_at_max() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        for i in 2..=10u64 {
            t.merge_entry(&NodeState::new_alive(i, make_addr(1000 + i as u16), i as u32));
        }
        let targets = pick_gossip_targets(&t, 1, 3);
        assert_eq!(targets.len(), 3);
        // All targets must be unique.
        let ids: std::collections::HashSet<_> = targets.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn targets_capped_at_live_peers() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        t.merge_entry(&NodeState::new_alive(2, make_addr(2000), 1));
        // Request more targets than available.
        let targets = pick_gossip_targets(&t, 1, 10);
        assert_eq!(targets.len(), 1);
    }

    #[test]
    fn targets_excludes_self() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        t.merge_entry(&NodeState::new_alive(2, make_addr(2000), 1));
        let targets = pick_gossip_targets(&t, 1, 5);
        assert!(targets.iter().all(|(id, _)| *id != 1));
    }
}
