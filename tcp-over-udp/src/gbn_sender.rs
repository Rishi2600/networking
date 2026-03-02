//! Go-Back-N send-side state machine.
//!
//! [`GbnSender`] maintains a sliding window of up to `N` in-flight segments.
//! Unlike stop-and-wait, multiple segments may be outstanding simultaneously.
//!
//! # Protocol contract
//!
//! - At most `window_size` segments may be in-flight at once.
//! - ACKs are **cumulative**: `ack_num = K` means the receiver has accepted
//!   all bytes up to (but not including) sequence number `K`.
//! - On timeout the caller retransmits **all** unacked segments from
//!   `send_base` onwards (go back to N).
//! - Sequence numbers are u32 and wrap around; comparisons use the convention
//!   that two sequence numbers are "close" when their difference fits in
//!   `u32::MAX / 2`.
//!
//! # RTT sampling (Karn's algorithm)
//!
//! [`on_ack`] returns an [`AckResult`] that includes an optional RTT sample.
//! The sample is taken from the **oldest** newly-acked segment but only when
//! that segment was sent exactly once (`tx_count == 1`).  If the segment was
//! ever retransmitted the sample is `None` — it would be ambiguous which
//! transmission the ACK is responding to (Karn's algorithm, RFC 6298 §4).
//!
//! [`on_ack`]: GbnSender::on_ack

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::packet::{flags, Header, Packet};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` when sequence number `a` is ≤ `b` in wrap-around space.
///
/// Correct as long as the two values differ by less than `u32::MAX / 2`,
/// which is always the case for a window of reasonable size.
#[inline]
pub(crate) fn seq_le(a: u32, b: u32) -> bool {
    b.wrapping_sub(a) <= (u32::MAX / 2)
}

// ---------------------------------------------------------------------------
// AckResult
// ---------------------------------------------------------------------------

/// Result returned by [`GbnSender::on_ack`].
#[derive(Debug)]
pub struct AckResult {
    /// Number of segments newly acknowledged by this ACK.
    pub acked_count: usize,

    /// RTT sample from the oldest newly-acked segment.
    ///
    /// `None` when nothing was newly acknowledged, or when the oldest
    /// newly-acked segment was retransmitted (`tx_count > 1`).  Callers
    /// must not feed a `None` sample into the RTT estimator (Karn's algorithm).
    pub rtt_sample: Option<Duration>,
}

// ---------------------------------------------------------------------------
// GbnEntry
// ---------------------------------------------------------------------------

/// A single in-flight segment occupying one slot in the retransmit window.
#[derive(Debug, Clone)]
pub struct GbnEntry {
    /// The segment ready to hand to the socket.
    pub packet: Packet,
    /// Total number of times this segment has been transmitted (1 = first send).
    pub tx_count: u32,
    /// Wall-clock time of the most recent transmission.
    pub sent_at: Instant,
}

// ---------------------------------------------------------------------------
// GbnSender
// ---------------------------------------------------------------------------

/// Go-Back-N send-side state for one connection.
///
/// # Sequence-number layout
///
/// ```text
///  send_base          next_seq
///      │                  │
///  ────┼──────────────────┼──────────────────▶ seq space
///      │ <── in flight ──▶│ <── sendable ───▶
/// ```
#[derive(Debug)]
pub struct GbnSender {
    /// Sequence number of the **oldest** unacked segment (left window edge).
    pub send_base: u32,

    /// Sequence number to use for the **next** new segment.
    pub next_seq: u32,

    /// Maximum segments in-flight simultaneously (N in Go-Back-N).
    window_size: usize,

    /// In-flight segments ordered by sequence number (front = oldest).
    window: VecDeque<GbnEntry>,
}

impl GbnSender {
    /// Create a new [`GbnSender`].
    ///
    /// `seq_start` is the first data sequence number (typically `ISN + 1`).
    /// `window_size` is the GBN window N (≥ 1).
    pub fn new(seq_start: u32, window_size: usize) -> Self {
        assert!(window_size >= 1, "window_size must be at least 1");
        Self {
            send_base: seq_start,
            next_seq: seq_start,
            window_size,
            window: VecDeque::with_capacity(window_size),
        }
    }

    /// `true` when there is room for at least one more in-flight segment.
    pub fn can_send(&self) -> bool {
        self.window.len() < self.window_size
    }

    /// Number of segments currently awaiting acknowledgement.
    pub fn in_flight(&self) -> usize {
        self.window.len()
    }

    /// `true` when at least one segment is awaiting acknowledgement.
    pub fn has_unacked(&self) -> bool {
        !self.window.is_empty()
    }

    /// Build a data segment using the current `next_seq`.
    ///
    /// Call [`record_sent`] immediately after to advance `next_seq` and
    /// register the segment in the window.
    ///
    /// [`record_sent`]: Self::record_sent
    pub fn build_data_packet(&self, payload: Vec<u8>, ack: u32, window: u16) -> Packet {
        Packet {
            header: Header {
                seq: self.next_seq,
                ack,
                flags: flags::ACK, // data segments piggyback the receiver's ACK
                window,
                checksum: 0,
            },
            payload,
        }
    }

    /// Register a just-transmitted segment in the window and advance `next_seq`.
    ///
    /// The `sent_at` timestamp is set to `Instant::now()` here and used later
    /// to compute the RTT sample when the segment is acknowledged.
    ///
    /// # Panics
    ///
    /// Panics in debug mode when the window is already full.  Check
    /// [`can_send`] first.
    ///
    /// [`can_send`]: Self::can_send
    pub fn record_sent(&mut self, packet: Packet) {
        debug_assert!(
            self.can_send(),
            "record_sent on a full window ({}/{})",
            self.window.len(),
            self.window_size
        );
        let payload_len = packet.payload.len() as u32;
        self.window.push_back(GbnEntry {
            packet,
            tx_count: 1,
            sent_at: Instant::now(),
        });
        self.next_seq = self.next_seq.wrapping_add(payload_len);
    }

    /// Process a cumulative ACK.
    ///
    /// Removes all window entries whose data ends at or before `ack_num`,
    /// advances `send_base`, and returns an [`AckResult`] containing:
    ///
    /// - `acked_count`: how many segments were newly acknowledged.
    /// - `rtt_sample`: elapsed time since the oldest acked segment was sent,
    ///   or `None` when nothing was newly acked or the oldest segment was
    ///   retransmitted (Karn's algorithm).
    ///
    /// Returns a zero count for duplicate or out-of-range ACKs.
    pub fn on_ack(&mut self, ack_num: u32) -> AckResult {
        let mut result = AckResult {
            acked_count: 0,
            rtt_sample: None,
        };

        // Reject ACKs behind send_base or beyond next_seq.
        if !seq_le(self.send_base, ack_num) || !seq_le(ack_num, self.next_seq) {
            return result;
        }
        // ack_num == send_base means nothing new.
        if ack_num == self.send_base {
            return result;
        }

        let mut is_oldest = true;

        while let Some(front) = self.window.front() {
            // `seg_end` is the sequence number of the first byte AFTER this segment.
            let seg_end = front
                .packet
                .header
                .seq
                .wrapping_add(front.packet.payload.len() as u32);

            if !seq_le(seg_end, ack_num) {
                break;
            }

            // Capture RTT sample from the oldest newly-acked segment only.
            // Karn's algorithm: discard the sample if the segment was retransmitted.
            if is_oldest {
                if front.tx_count == 1 {
                    result.rtt_sample = Some(front.sent_at.elapsed());
                }
                is_oldest = false;
            }

            self.send_base = seg_end;
            self.window.pop_front();
            result.acked_count += 1;
        }

        result
    }

    /// Iterate over all in-flight segments from oldest to newest.
    ///
    /// Used by the connection layer to retransmit the full window on timeout
    /// (the "go back N" step).
    pub fn window_entries(&self) -> impl Iterator<Item = &GbnEntry> {
        self.window.iter()
    }

    /// Increment the transmission count and refresh `sent_at` for every
    /// in-flight segment.
    ///
    /// Call this immediately **after** retransmitting the entire window so
    /// subsequent RTT samples from these retransmitted segments will be
    /// suppressed by Karn's algorithm.
    pub fn on_retransmit(&mut self) {
        let now = Instant::now();
        for entry in self.window.iter_mut() {
            entry.tx_count += 1;
            entry.sent_at = now;
        }
    }

    /// Wall-clock time when the oldest in-flight segment was last sent.
    ///
    /// Returns `None` when the window is empty.
    pub fn oldest_sent_at(&self) -> Option<Instant> {
        self.window.front().map(|e| e.sent_at)
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pkt(seq: u32, payload_len: usize) -> Packet {
        Packet {
            header: Header {
                seq,
                ack: 0,
                flags: flags::ACK,
                window: 8192,
                checksum: 0,
            },
            payload: vec![0u8; payload_len],
        }
    }

    #[test]
    fn initial_state() {
        let s = GbnSender::new(100, 4);
        assert_eq!(s.send_base, 100);
        assert_eq!(s.next_seq, 100);
        assert!(s.can_send());
        assert!(!s.has_unacked());
        assert_eq!(s.in_flight(), 0);
    }

    #[test]
    fn record_sent_advances_next_seq() {
        let mut s = GbnSender::new(0, 4);
        let pkt = s.build_data_packet(vec![1, 2, 3], 0, 8192);
        s.record_sent(pkt);

        assert_eq!(s.next_seq, 3);
        assert_eq!(s.send_base, 0);
        assert_eq!(s.in_flight(), 1);
        assert!(s.has_unacked());
    }

    #[test]
    fn window_full_blocks_send() {
        let mut s = GbnSender::new(0, 2);
        let p1 = make_pkt(0, 5);
        let p2 = make_pkt(5, 5);
        s.record_sent(p1);
        s.next_seq = 5;
        s.record_sent(p2);
        s.next_seq = 10;

        assert!(!s.can_send());
        assert_eq!(s.in_flight(), 2);
    }

    #[test]
    fn ack_slides_window_by_one() {
        let mut s = GbnSender::new(0, 4);
        let p = s.build_data_packet(vec![0u8; 10], 0, 8192);
        s.record_sent(p);

        let r = s.on_ack(10);
        assert_eq!(r.acked_count, 1);
        assert_eq!(s.send_base, 10);
        assert!(!s.has_unacked());
    }

    #[test]
    fn cumulative_ack_slides_multiple() {
        let mut s = GbnSender::new(0, 4);
        for _ in 0..3 {
            let pkt = s.build_data_packet(vec![0u8; 5], 0, 8192);
            s.record_sent(pkt);
        }
        assert_eq!(s.next_seq, 15);

        let r = s.on_ack(15);
        assert_eq!(r.acked_count, 3);
        assert_eq!(s.send_base, 15);
        assert!(!s.has_unacked());
    }

    #[test]
    fn duplicate_ack_returns_zero() {
        let mut s = GbnSender::new(0, 4);
        let p = s.build_data_packet(vec![0u8; 5], 0, 8192);
        s.record_sent(p);

        let first = s.on_ack(5);
        assert_eq!(first.acked_count, 1);

        let dup = s.on_ack(5);
        assert_eq!(dup.acked_count, 0);
        assert!(dup.rtt_sample.is_none());
    }

    #[test]
    fn spurious_ack_beyond_next_seq_ignored() {
        let mut s = GbnSender::new(0, 4);
        let p = s.build_data_packet(vec![0u8; 5], 0, 8192);
        s.record_sent(p);

        let r = s.on_ack(1000);
        assert_eq!(r.acked_count, 0);
        assert_eq!(s.send_base, 0);
    }

    #[test]
    fn partial_cumulative_ack() {
        let mut s = GbnSender::new(0, 4);
        for _ in 0..3 {
            let pkt = s.build_data_packet(vec![0u8; 5], 0, 8192);
            s.record_sent(pkt);
        }
        let r = s.on_ack(10);
        assert_eq!(r.acked_count, 2);
        assert_eq!(s.send_base, 10);
        assert_eq!(s.in_flight(), 1);
    }

    #[test]
    fn on_retransmit_increments_tx_count() {
        let mut s = GbnSender::new(0, 4);
        let p = s.build_data_packet(vec![0u8; 5], 0, 8192);
        s.record_sent(p);

        assert_eq!(s.window.front().unwrap().tx_count, 1);
        s.on_retransmit();
        assert_eq!(s.window.front().unwrap().tx_count, 2);
    }

    #[test]
    fn seq_wrap_around() {
        let start = u32::MAX - 5;
        let mut s = GbnSender::new(start, 4);
        let p = s.build_data_packet(vec![0u8; 10], 0, 8192);
        s.record_sent(p);

        let expected_ack = start.wrapping_add(10);
        let r = s.on_ack(expected_ack);
        assert_eq!(r.acked_count, 1);
        assert_eq!(s.send_base, expected_ack);
    }

    // ── Karn's algorithm ────────────────────────────────────────────────────

    #[test]
    fn clean_segment_yields_rtt_sample() {
        let mut s = GbnSender::new(0, 1);
        let p = s.build_data_packet(vec![0u8; 5], 0, 8192);
        s.record_sent(p);

        // tx_count == 1 (never retransmitted) → sample must be present.
        let r = s.on_ack(5);
        assert_eq!(r.acked_count, 1);
        assert!(
            r.rtt_sample.is_some(),
            "tx_count==1 segment must produce an RTT sample"
        );
        // Sample should be tiny (measured in this test process).
        assert!(
            r.rtt_sample.unwrap() < Duration::from_millis(100),
            "sample should be near-zero in a unit test"
        );
    }

    #[test]
    fn retransmitted_segment_yields_no_rtt_sample() {
        let mut s = GbnSender::new(0, 1);
        let p = s.build_data_packet(vec![0u8; 5], 0, 8192);
        s.record_sent(p);

        // Simulate one retransmit: tx_count becomes 2.
        s.on_retransmit();
        assert_eq!(s.window.front().unwrap().tx_count, 2);

        // Karn's algorithm: ACK for a retransmitted segment → no RTT sample.
        let r = s.on_ack(5);
        assert_eq!(r.acked_count, 1);
        assert!(
            r.rtt_sample.is_none(),
            "tx_count>1 segment must NOT produce an RTT sample (Karn's algorithm)"
        );
    }

    #[test]
    fn rtt_sample_taken_from_oldest_only() {
        // With 3 segments in the window, a cumulative ACK for all three
        // should yield the sample from segment 0 only (the oldest).
        let mut s = GbnSender::new(0, 4);
        for _ in 0..3 {
            let pkt = s.build_data_packet(vec![0u8; 4], 0, 8192);
            s.record_sent(pkt);
        }

        // Retransmit only the middle segment by manually bumping its tx_count.
        s.window[1].tx_count = 2;

        // Cumulative ACK for all three: oldest (index 0) has tx_count==1 → sample present.
        let r = s.on_ack(12);
        assert_eq!(r.acked_count, 3);
        assert!(
            r.rtt_sample.is_some(),
            "oldest segment tx_count==1 → sample expected"
        );
    }

    #[test]
    fn karn_oldest_retransmitted_no_sample_even_if_later_clean() {
        // Even when later segments are clean, if the OLDEST was retransmitted
        // the sample must be None.
        let mut s = GbnSender::new(0, 4);
        for _ in 0..3 {
            let pkt = s.build_data_packet(vec![0u8; 4], 0, 8192);
            s.record_sent(pkt);
        }

        // Mark the OLDEST segment as retransmitted.
        s.window[0].tx_count = 2;

        let r = s.on_ack(12);
        assert_eq!(r.acked_count, 3);
        assert!(
            r.rtt_sample.is_none(),
            "oldest segment retransmitted → no RTT sample (Karn's algorithm)"
        );
    }
}
