/// Network simulator — configurable loss, partitions, and delay for testing.
///
/// `NetSim` is a shared-state object (wrapped in `Arc<Mutex<>>`) that the
/// `Transport` layer consults before delivering each datagram.  It provides:
///
/// - **Packet loss**: random drop based on a configurable probability.
/// - **Partitions**: bidirectional blocks between pairs of addresses.
/// - **Delay**: configurable base delay per packet (applied in Transport).
///
/// All randomness is driven by a deterministic SplitMix64 PRNG seeded at
/// construction, so simulations are reproducible.
use std::collections::HashSet;
use std::net::SocketAddr;

// ── Deterministic PRNG (SplitMix64) ─────────────────────────────────────────
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ── NetSim ──────────────────────────────────────────────────────────────────
pub struct NetSim {
    rng: SplitMix64,
    loss_rate: f64,
    /// Base delay in milliseconds applied to every delivered packet.
    /// 0 = no delay.
    delay_ms: u64,
    /// Bidirectional partitions stored as canonical `(min, max)` pairs.
    partitions: HashSet<(SocketAddr, SocketAddr)>,
}

impl NetSim {
    pub fn new(seed: u64) -> Self {
        Self {
            rng: SplitMix64::new(seed),
            loss_rate: 0.0,
            delay_ms: 0,
            partitions: HashSet::new(),
        }
    }

    // ── Builder methods ──────────────────────────────────────────────────

    pub fn with_loss(mut self, rate: f64) -> Self {
        self.loss_rate = rate.clamp(0.0, 1.0);
        self
    }

    pub fn with_delay_ms(mut self, ms: u64) -> Self {
        self.delay_ms = ms;
        self
    }

    // ── Runtime mutation ─────────────────────────────────────────────────

    pub fn set_loss_rate(&mut self, rate: f64) {
        self.loss_rate = rate.clamp(0.0, 1.0);
    }

    pub fn set_delay_ms(&mut self, ms: u64) {
        self.delay_ms = ms;
    }

    /// Block all traffic between `a` and `b` (bidirectional).
    pub fn add_partition(&mut self, a: SocketAddr, b: SocketAddr) {
        self.partitions.insert(Self::canonical(a, b));
    }

    /// Restore traffic between `a` and `b`.
    pub fn remove_partition(&mut self, a: SocketAddr, b: SocketAddr) {
        self.partitions.remove(&Self::canonical(a, b));
    }

    /// Remove all partitions.
    pub fn clear_partitions(&mut self) {
        self.partitions.clear();
    }

    // ── Query ────────────────────────────────────────────────────────────

    /// Decide whether a packet from `from` to `to` should be delivered.
    ///
    /// Returns `false` if the packet is lost (random drop) or the path
    /// is partitioned.  Advances the PRNG state on every call.
    pub fn should_deliver(&mut self, from: SocketAddr, to: SocketAddr) -> bool {
        if self.is_partitioned(from, to) {
            return false;
        }
        if self.loss_rate > 0.0 && self.rng.next_f64() < self.loss_rate {
            return false;
        }
        true
    }

    /// Returns `true` if there is an active partition between `a` and `b`.
    pub fn is_partitioned(&self, a: SocketAddr, b: SocketAddr) -> bool {
        self.partitions.contains(&Self::canonical(a, b))
    }

    /// Configured delay in milliseconds.
    pub fn delay_ms(&self) -> u64 {
        self.delay_ms
    }

    // ── Internal ─────────────────────────────────────────────────────────

    fn canonical(a: SocketAddr, b: SocketAddr) -> (SocketAddr, SocketAddr) {
        if a <= b {
            (a, b)
        } else {
            (b, a)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    // ── PRNG determinism ─────────────────────────────────────────────────

    #[test]
    fn prng_deterministic_with_same_seed() {
        let mut a = SplitMix64::new(42);
        let mut b = SplitMix64::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn prng_different_seeds_differ() {
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(2);
        // Extremely unlikely for all 10 to match.
        let matches = (0..10).filter(|_| a.next_u64() == b.next_u64()).count();
        assert!(matches < 10);
    }

    #[test]
    fn prng_f64_in_unit_range() {
        let mut rng = SplitMix64::new(0);
        for _ in 0..1000 {
            let v = rng.next_f64();
            assert!((0.0..1.0).contains(&v), "f64 out of range: {v}");
        }
    }

    // ── Loss ─────────────────────────────────────────────────────────────

    #[test]
    fn zero_loss_always_delivers() {
        let mut sim = NetSim::new(0);
        for _ in 0..100 {
            assert!(sim.should_deliver(addr(1), addr(2)));
        }
    }

    #[test]
    fn full_loss_never_delivers() {
        let mut sim = NetSim::new(0).with_loss(1.0);
        for _ in 0..100 {
            assert!(!sim.should_deliver(addr(1), addr(2)));
        }
    }

    #[test]
    fn partial_loss_drops_some() {
        let mut sim = NetSim::new(42).with_loss(0.5);
        let mut delivered = 0;
        let total = 1000;
        for _ in 0..total {
            if sim.should_deliver(addr(1), addr(2)) {
                delivered += 1;
            }
        }
        // With 50% loss, expect roughly 400–600 delivered.
        assert!(
            delivered > 300 && delivered < 700,
            "expected ~500 delivered, got {delivered}"
        );
    }

    #[test]
    fn loss_rate_clamped() {
        let mut sim = NetSim::new(0).with_loss(2.0);
        // Clamped to 1.0 → never delivers.
        assert!(!sim.should_deliver(addr(1), addr(2)));

        sim.set_loss_rate(-1.0);
        // Clamped to 0.0 → always delivers.
        assert!(sim.should_deliver(addr(1), addr(2)));
    }

    #[test]
    fn loss_deterministic_same_seed() {
        let results_a: Vec<bool> = {
            let mut sim = NetSim::new(99).with_loss(0.3);
            (0..100).map(|_| sim.should_deliver(addr(1), addr(2))).collect()
        };
        let results_b: Vec<bool> = {
            let mut sim = NetSim::new(99).with_loss(0.3);
            (0..100).map(|_| sim.should_deliver(addr(1), addr(2))).collect()
        };
        assert_eq!(results_a, results_b);
    }

    // ── Partitions ───────────────────────────────────────────────────────

    #[test]
    fn no_partition_by_default() {
        let sim = NetSim::new(0);
        assert!(!sim.is_partitioned(addr(1), addr(2)));
    }

    #[test]
    fn add_partition_blocks_traffic() {
        let mut sim = NetSim::new(0);
        sim.add_partition(addr(1), addr(2));
        assert!(!sim.should_deliver(addr(1), addr(2)));
        assert!(!sim.should_deliver(addr(2), addr(1))); // bidirectional
    }

    #[test]
    fn partition_does_not_affect_other_pairs() {
        let mut sim = NetSim::new(0);
        sim.add_partition(addr(1), addr(2));
        assert!(sim.should_deliver(addr(1), addr(3)));
        assert!(sim.should_deliver(addr(3), addr(2)));
    }

    #[test]
    fn remove_partition_restores_traffic() {
        let mut sim = NetSim::new(0);
        sim.add_partition(addr(1), addr(2));
        assert!(!sim.should_deliver(addr(1), addr(2)));
        sim.remove_partition(addr(1), addr(2));
        assert!(sim.should_deliver(addr(1), addr(2)));
    }

    #[test]
    fn remove_partition_reversed_order() {
        let mut sim = NetSim::new(0);
        sim.add_partition(addr(1), addr(2));
        // Remove with reversed argument order — canonical key should match.
        sim.remove_partition(addr(2), addr(1));
        assert!(sim.should_deliver(addr(1), addr(2)));
    }

    #[test]
    fn clear_partitions_removes_all() {
        let mut sim = NetSim::new(0);
        sim.add_partition(addr(1), addr(2));
        sim.add_partition(addr(3), addr(4));
        sim.clear_partitions();
        assert!(sim.should_deliver(addr(1), addr(2)));
        assert!(sim.should_deliver(addr(3), addr(4)));
    }

    #[test]
    fn partition_checked_before_loss() {
        // Even with 0% loss, partitioned traffic is blocked.
        let mut sim = NetSim::new(0);
        sim.add_partition(addr(1), addr(2));
        assert!(!sim.should_deliver(addr(1), addr(2)));
    }

    // ── Delay ────────────────────────────────────────────────────────────

    #[test]
    fn default_delay_is_zero() {
        let sim = NetSim::new(0);
        assert_eq!(sim.delay_ms(), 0);
    }

    #[test]
    fn with_delay_sets_value() {
        let sim = NetSim::new(0).with_delay_ms(50);
        assert_eq!(sim.delay_ms(), 50);
    }

    #[test]
    fn set_delay_updates_value() {
        let mut sim = NetSim::new(0);
        sim.set_delay_ms(100);
        assert_eq!(sim.delay_ms(), 100);
    }

    // ── Combined ─────────────────────────────────────────────────────────

    #[test]
    fn partition_plus_loss() {
        let mut sim = NetSim::new(0).with_loss(0.5);
        sim.add_partition(addr(1), addr(2));
        // Partitioned: always blocked regardless of loss dice.
        for _ in 0..100 {
            assert!(!sim.should_deliver(addr(1), addr(2)));
        }
        // Unpartitioned pair: subject to 50% loss.
        let mut delivered = 0;
        for _ in 0..100 {
            if sim.should_deliver(addr(1), addr(3)) {
                delivered += 1;
            }
        }
        assert!(delivered > 0 && delivered < 100);
    }
}
