/// Membership table: stores the cluster-wide view of node state and implements
/// the gossip merge rules.
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::message::{status, WireNodeEntry};
use crate::node::{NodeId, NodeState, NodeStatus};

// ── Table ─────────────────────────────────────────────────────────────────────
pub struct MembershipTable {
    pub entries: HashMap<NodeId, NodeState>,
    pub local_id: NodeId,
    local_addr: SocketAddr,
    local_heartbeat: u32,
}

impl MembershipTable {
    pub fn new(local_id: NodeId, local_addr: SocketAddr) -> Self {
        let mut t = Self {
            entries: HashMap::new(),
            local_id,
            local_addr,
            local_heartbeat: 0,
        };
        // Seed our own entry as Alive with heartbeat 0.
        t.entries.insert(
            local_id,
            NodeState::new_alive(local_id, local_addr, 0),
        );
        t
    }

    pub fn our_heartbeat(&self) -> u32 {
        self.local_heartbeat
    }

    /// Increment our own heartbeat counter and refresh our membership entry.
    /// Called on every heartbeat tick.
    pub fn tick_heartbeat(&mut self) {
        self.local_heartbeat = self.local_heartbeat.wrapping_add(1);
        let hb = self.local_heartbeat;
        let addr = self.local_addr;
        let id = self.local_id;
        let entry = self.entries.entry(id).or_insert_with(|| {
            NodeState::new_alive(id, addr, 0)
        });
        entry.heartbeat = hb;
        entry.status = NodeStatus::Alive;
        entry.last_update = Instant::now();
    }

    // ── Merge rules ───────────────────────────────────────────────────────────
    /// Merge a single incoming node state from a gossip digest.
    ///
    /// Rules (in priority order):
    /// 1. Ignore entries about ourselves — we are the authority on our own state.
    /// 2. Insert unknown entries unconditionally.
    /// 3. Dead entries are terminal — never downgrade from Dead.
    /// 4. Higher heartbeat wins, carrying the incoming status.
    /// 5. Same heartbeat: more severe status wins (Dead > Suspect > Alive).
    /// 6. Otherwise discard — existing entry is equally or more current.
    pub fn merge_entry(&mut self, incoming: &NodeState) {
        // Rule 1: never let remote nodes overwrite our own entry.
        if incoming.node_id == self.local_id {
            // Exception: if remote gossip says we are Suspect/Dead, we must
            // refute it by bumping our heartbeat immediately.
            if incoming.status != NodeStatus::Alive {
                log::info!(
                    "[membership] received refutation-needed entry: we appear {:?} to others; bumping heartbeat",
                    incoming.status
                );
                self.local_heartbeat = self.local_heartbeat.wrapping_add(10);
                let hb = self.local_heartbeat;
                let addr = self.local_addr;
                let id = self.local_id;
                let e = self.entries.entry(id).or_insert_with(|| NodeState::new_alive(id, addr, 0));
                e.heartbeat = hb;
                e.status = NodeStatus::Alive;
                e.last_update = Instant::now();
            }
            return;
        }

        let now = Instant::now();
        let existing = self.entries.get(&incoming.node_id);

        match existing {
            // Rule 2: new node.
            None => {
                self.entries.insert(incoming.node_id, {
                    let mut s = incoming.clone();
                    s.last_update = now;
                    s
                });
                log::info!(
                    "[membership] new node {} @ {} (hb={}, status={:?})",
                    incoming.node_id,
                    incoming.addr,
                    incoming.heartbeat,
                    incoming.status
                );
            }
            Some(existing) => {
                // Rule 3: Dead is terminal.
                if existing.status == NodeStatus::Dead {
                    return;
                }

                let update = if incoming.heartbeat > existing.heartbeat {
                    // Rule 4: higher heartbeat wins.
                    true
                } else if incoming.heartbeat == existing.heartbeat
                    && incoming.status > existing.status
                {
                    // Rule 5: same heartbeat, more severe status wins.
                    true
                } else {
                    // Rule 6: discard.
                    false
                };

                if update {
                    let old_status = existing.status;
                    let entry = self.entries.get_mut(&incoming.node_id).unwrap();
                    entry.heartbeat = incoming.heartbeat;
                    entry.addr = incoming.addr;
                    entry.last_update = now;

                    if incoming.status != old_status {
                        log::info!(
                            "[membership] node {} status: {:?} → {:?} (hb={})",
                            incoming.node_id,
                            old_status,
                            incoming.status,
                            incoming.heartbeat
                        );
                    }

                    entry.status = incoming.status;
                    if incoming.status == NodeStatus::Suspect && old_status != NodeStatus::Suspect {
                        entry.suspect_since = Some(now);
                    } else if incoming.status != NodeStatus::Suspect {
                        entry.suspect_since = None;
                    }
                }
            }
        }
    }

    /// Merge a full gossip digest (slice of node states).
    pub fn merge_digest(&mut self, entries: &[NodeState]) {
        for e in entries {
            self.merge_entry(e);
        }
    }

    // ── Status transitions ────────────────────────────────────────────────────
    pub fn suspect(&mut self, id: NodeId) {
        if let Some(e) = self.entries.get_mut(&id) {
            if e.status == NodeStatus::Alive {
                log::info!("[membership] node {} → Suspect", id);
                e.status = NodeStatus::Suspect;
                e.suspect_since = Some(Instant::now());
                e.last_update = Instant::now();
            }
        }
    }

    pub fn declare_dead(&mut self, id: NodeId) {
        if let Some(e) = self.entries.get_mut(&id) {
            if e.status != NodeStatus::Dead {
                log::info!("[membership] node {} → Dead", id);
                e.status = NodeStatus::Dead;
                e.suspect_since = None;
                e.last_update = Instant::now();
            }
        }
    }

    // ── Queries ───────────────────────────────────────────────────────────────
    /// Return IDs of all nodes currently Alive or Suspect, excluding ourselves.
    pub fn live_nodes(&self) -> Vec<NodeId> {
        self.entries
            .values()
            .filter(|e| {
                e.node_id != self.local_id
                    && matches!(e.status, NodeStatus::Alive | NodeStatus::Suspect)
            })
            .map(|e| e.node_id)
            .collect()
    }

    /// Return all entries where `suspect_since` has exceeded `timeout`, so the
    /// event loop can promote them to Dead.
    pub fn expired_suspects(&self, timeout: Duration) -> Vec<NodeId> {
        let now = Instant::now();
        self.entries
            .values()
            .filter(|e| {
                e.status == NodeStatus::Suspect
                    && e.suspect_since
                        .map(|s| now.duration_since(s) >= timeout)
                        .unwrap_or(false)
            })
            .map(|e| e.node_id)
            .collect()
    }

    /// Remove Dead entries that have been dead for longer than `retention`.
    /// This prevents unbounded growth of the gossip digest.
    pub fn gc_dead(&mut self, retention: Duration) {
        let now = Instant::now();
        self.entries.retain(|_, e| {
            if e.status == NodeStatus::Dead {
                now.duration_since(e.last_update) < retention
            } else {
                true
            }
        });
    }

    /// Return up to `max_entries` entries for gossiping, prioritising recently
    /// updated entries so fresh information spreads faster (infection-style).
    pub fn gossip_digest(&self, max_entries: usize) -> Vec<NodeState> {
        let mut all: Vec<&NodeState> = self.entries.values().collect();
        // Most recently updated first.
        all.sort_by(|a, b| b.last_update.cmp(&a.last_update));
        all.truncate(max_entries);
        all.into_iter().cloned().collect()
    }

    /// Convert a gossip digest into wire entries.
    pub fn gossip_wire_entries(&self, max_entries: usize) -> Vec<WireNodeEntry> {
        self.gossip_digest(max_entries)
            .iter()
            .filter_map(|s| node_state_to_wire(s))
            .collect()
    }

    /// Add a bootstrap peer to the table (used at startup before any gossip).
    pub fn add_bootstrap_peer(&mut self, addr: SocketAddr) {
        // We don't know the peer's node_id yet; derive a placeholder key from
        // the address. The real entry will be corrected by the first gossip round.
        let placeholder_id = addr_to_placeholder_id(addr);
        self.entries.entry(placeholder_id).or_insert_with(|| {
            log::debug!("[membership] bootstrap peer {} (placeholder id={})", addr, placeholder_id);
            NodeState::new_alive(placeholder_id, addr, 0)
        });
    }
}

// ── Conversion helpers ────────────────────────────────────────────────────────
/// Convert a `NodeState` to a wire entry. Returns `None` for non-IPv4 addresses.
pub fn node_state_to_wire(s: &NodeState) -> Option<WireNodeEntry> {
    let (ip, port) = match s.addr {
        SocketAddr::V4(a) => (u32::from(*a.ip()), a.port()),
        SocketAddr::V6(_) => return None, // IPv6 not supported in this wire format.
    };
    Some(WireNodeEntry {
        node_id: s.node_id,
        heartbeat: s.heartbeat,
        status: s.status.to_wire(),
        ip,
        port,
    })
}

/// Convert a received wire entry into a local `NodeState`.
pub fn wire_to_node_state(e: &WireNodeEntry) -> Option<NodeState> {
    let addr = std::net::SocketAddr::V4(e.addr());
    let status = NodeStatus::from_wire(e.status)?;
    Some(NodeState {
        node_id: e.node_id,
        addr,
        heartbeat: e.heartbeat,
        status,
        last_update: Instant::now(),
        suspect_since: if status == NodeStatus::Suspect {
            Some(Instant::now())
        } else {
            None
        },
    })
}

/// Derive a deterministic but unique placeholder NodeId from a SocketAddr,
/// used for bootstrap peers whose real IDs we don't yet know.
fn addr_to_placeholder_id(addr: SocketAddr) -> NodeId {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    addr.hash(&mut h);
    // XOR with a sentinel to distinguish placeholder IDs from real ones.
    h.finish() ^ 0xDEAD_BEEF_0000_0000
}

// ── Status wire constants exposed for tests ───────────────────────────────────
pub use status::{ALIVE, DEAD, SUSPECT};

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
    }

    #[test]
    fn insert_new_entry() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        let peer = NodeState::new_alive(2, make_addr(2000), 1);
        t.merge_entry(&peer);
        assert_eq!(t.entries[&2].heartbeat, 1);
        assert_eq!(t.entries[&2].status, NodeStatus::Alive);
    }

    #[test]
    fn higher_heartbeat_wins() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        t.merge_entry(&NodeState::new_alive(2, make_addr(2000), 1));
        t.merge_entry(&NodeState::new_alive(2, make_addr(2000), 5));
        assert_eq!(t.entries[&2].heartbeat, 5);
    }

    #[test]
    fn lower_heartbeat_ignored() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        t.merge_entry(&NodeState::new_alive(2, make_addr(2000), 5));
        t.merge_entry(&NodeState::new_alive(2, make_addr(2000), 3));
        assert_eq!(t.entries[&2].heartbeat, 5);
    }

    #[test]
    fn dead_is_terminal() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        let mut dead = NodeState::new_alive(2, make_addr(2000), 10);
        dead.status = NodeStatus::Dead;
        t.merge_entry(&dead);
        // Alive with higher heartbeat must not resurrect.
        t.merge_entry(&NodeState::new_alive(2, make_addr(2000), 20));
        assert_eq!(t.entries[&2].status, NodeStatus::Dead);
    }

    #[test]
    fn same_heartbeat_suspect_beats_alive() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        t.merge_entry(&NodeState::new_alive(2, make_addr(2000), 3));
        let mut suspect = NodeState::new_alive(2, make_addr(2000), 3);
        suspect.status = NodeStatus::Suspect;
        t.merge_entry(&suspect);
        assert_eq!(t.entries[&2].status, NodeStatus::Suspect);
    }

    #[test]
    fn self_entry_not_overwritten() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        t.tick_heartbeat();
        let fake = NodeState::new_alive(1, make_addr(9999), 999);
        t.merge_entry(&fake);
        // Our address and heartbeat are ours — addr must not be overwritten.
        assert_eq!(t.entries[&1].addr, make_addr(1000));
    }

    #[test]
    fn tick_heartbeat_increments() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        t.tick_heartbeat();
        t.tick_heartbeat();
        assert_eq!(t.our_heartbeat(), 2);
        assert_eq!(t.entries[&1].heartbeat, 2);
    }

    #[test]
    fn expired_suspects_returned() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        let mut s = NodeState::new_alive(2, make_addr(2000), 1);
        s.status = NodeStatus::Suspect;
        s.suspect_since = Some(Instant::now() - Duration::from_secs(10));
        t.entries.insert(2, s);
        let expired = t.expired_suspects(Duration::from_secs(5));
        assert!(expired.contains(&2));
    }

    #[test]
    fn gc_removes_old_dead_entries() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        let mut dead = NodeState::new_alive(2, make_addr(2000), 5);
        dead.status = NodeStatus::Dead;
        dead.last_update = Instant::now() - Duration::from_secs(100);
        t.entries.insert(2, dead);
        t.gc_dead(Duration::from_secs(30));
        assert!(!t.entries.contains_key(&2));
    }

    #[test]
    fn live_nodes_excludes_dead_and_self() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        t.merge_entry(&NodeState::new_alive(2, make_addr(2000), 1));
        let mut dead = NodeState::new_alive(3, make_addr(3000), 1);
        dead.status = NodeStatus::Dead;
        t.merge_entry(&dead);
        let live = t.live_nodes();
        assert!(live.contains(&2));
        assert!(!live.contains(&3));
        assert!(!live.contains(&1)); // self
    }

    #[test]
    fn gossip_digest_most_recent_first() {
        let mut t = MembershipTable::new(1, make_addr(1000));
        // Age the self entry so it sorts below the peers we are about to insert.
        t.entries.get_mut(&1).unwrap().last_update =
            Instant::now() - Duration::from_secs(10);
        for i in 2..=6u64 {
            let mut s = NodeState::new_alive(i, make_addr(i as u16 * 1000), i as u32);
            // Stagger last_update: node 6 is most recent (400 ms ago).
            s.last_update = Instant::now() - Duration::from_millis((10 - i) * 100);
            t.entries.insert(i, s);
        }
        let digest = t.gossip_digest(3);
        // Node 6 has the most recent last_update and must appear first.
        assert_eq!(digest[0].node_id, 6);
    }
}
