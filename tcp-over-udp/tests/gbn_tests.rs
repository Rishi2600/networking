//! Integration tests for the Go-Back-N sliding-window layer.
//!
//! Each test spins up two in-process GBN endpoints talking over the loopback
//! interface.  Both sides are spawned as separate tokio tasks so they can make
//! progress concurrently without blocking each other.

use tcp_over_udp::{
    connection::ConnError,
    gbn_connection::GbnConnection,
    socket::Socket,
};

/// Bind a socket to an OS-assigned port on loopback and return it together
/// with its resolved local address.
async fn ephemeral() -> Socket {
    let addr = "127.0.0.1:0".parse().unwrap();
    Socket::bind(addr).await.expect("bind failed")
}

// ---------------------------------------------------------------------------
// Test 1: basic ping-pong via GBN (window = 1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gbn_basic_send_recv() {
    let server_sock = ephemeral().await;
    let server_addr = server_sock.local_addr;

    let server = tokio::spawn(async move {
        let mut conn = GbnConnection::accept(server_sock, 1)
            .await
            .expect("accept");

        let data = conn.recv().await.expect("server recv");
        assert_eq!(data, b"Ping!");

        conn.send(b"Pong!").await.expect("server send");
        conn.close().await.expect("server close");
    });

    let client = tokio::spawn(async move {
        let sock = ephemeral().await;
        let mut conn = GbnConnection::connect(sock, server_addr, 1)
            .await
            .expect("connect");

        conn.send(b"Ping!").await.expect("client send");

        let reply = conn.recv().await.expect("client recv");
        assert_eq!(reply, b"Pong!");

        conn.close().await.expect("client close");
    });

    let (sr, cr) = tokio::join!(server, client);
    sr.unwrap();
    cr.unwrap();
}

// ---------------------------------------------------------------------------
// Test 2: pipelined sends with window > 1
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gbn_pipeline_window4() {
    const WINDOW: usize = 4;
    const MSG_COUNT: usize = 10;

    let server_sock = ephemeral().await;
    let server_addr = server_sock.local_addr;

    let server = tokio::spawn(async move {
        let mut conn = GbnConnection::accept(server_sock, WINDOW)
            .await
            .expect("accept");

        let mut received: Vec<Vec<u8>> = Vec::new();
        loop {
            match conn.recv().await {
                Ok(data) => received.push(data),
                Err(ConnError::Eof) => break,
                Err(e) => panic!("server recv error: {e}"),
            }
            if received.len() == MSG_COUNT {
                break;
            }
        }
        conn.close().await.expect("server close");
        received
    });

    let client = tokio::spawn(async move {
        let sock = ephemeral().await;
        let mut conn = GbnConnection::connect(sock, server_addr, WINDOW)
            .await
            .expect("connect");

        for i in 0..MSG_COUNT {
            let msg = format!("msg-{i:02}");
            conn.send(msg.as_bytes()).await.expect("send");
        }
        conn.flush().await.expect("flush");
        conn.close().await.expect("client close");
    });

    let (sr, cr) = tokio::join!(server, client);
    let received = sr.unwrap();
    cr.unwrap();

    assert_eq!(received.len(), MSG_COUNT);
    for (i, chunk) in received.iter().enumerate() {
        let expected = format!("msg-{i:02}");
        assert_eq!(chunk, expected.as_bytes(), "message {i} corrupted");
    }
}

// ---------------------------------------------------------------------------
// Test 3: from_connection — upgrade stop-and-wait to GBN mid-stream
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gbn_from_connection() {
    use tcp_over_udp::connection::Connection;

    let server_sock = ephemeral().await;
    let server_addr = server_sock.local_addr;

    let server = tokio::spawn(async move {
        // Complete handshake with the existing Connection API, then upgrade.
        let conn = Connection::accept(server_sock).await.expect("accept");
        let mut gbn = GbnConnection::from_connection(conn, 4);

        let data = gbn.recv().await.expect("recv");
        assert_eq!(data, b"hello from gbn");

        gbn.send(b"ack from gbn").await.expect("send");
        gbn.close().await.expect("close");
    });

    let client = tokio::spawn(async move {
        let sock = ephemeral().await;
        let conn = Connection::connect(sock, server_addr).await.expect("connect");
        let mut gbn = GbnConnection::from_connection(conn, 4);

        gbn.send(b"hello from gbn").await.expect("send");

        let reply = gbn.recv().await.expect("recv");
        assert_eq!(reply, b"ack from gbn");

        gbn.close().await.expect("close");
    });

    let (sr, cr) = tokio::join!(server, client);
    sr.unwrap();
    cr.unwrap();
}

// ---------------------------------------------------------------------------
// Test 4: concurrent session via run()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gbn_concurrent_session() {
    const WINDOW: usize = 4;
    const MSG_COUNT: usize = 6;

    let server_sock = ephemeral().await;
    let server_addr = server_sock.local_addr;

    // Server side: sequential recv, echoes back.
    let server = tokio::spawn(async move {
        let mut conn = GbnConnection::accept(server_sock, WINDOW)
            .await
            .expect("accept");
        for _ in 0..MSG_COUNT {
            match conn.recv().await {
                Ok(data) => conn.send(&data).await.expect("echo"),
                Err(ConnError::Eof) => break,
                Err(e) => panic!("server: {e}"),
            }
        }
        conn.close().await.expect("close");
    });

    // Client side: use the concurrent session API.
    let client = tokio::spawn(async move {
        let sock = ephemeral().await;
        let conn = GbnConnection::connect(sock, server_addr, WINDOW)
            .await
            .expect("connect");

        let mut session = conn.run();

        // Send all messages through the channel (non-blocking).
        for i in 0..MSG_COUNT {
            let msg = format!("item-{i}");
            session.send(msg.into_bytes()).await.expect("session send");
        }

        // Collect echoes.
        let mut replies = Vec::new();
        for _ in 0..MSG_COUNT {
            match session.recv().await {
                Ok(data) => replies.push(data),
                Err(ConnError::Eof) => break,
                Err(e) => panic!("client session recv: {e}"),
            }
        }

        session.close().await;
        replies
    });

    let (sr, cr) = tokio::join!(server, client);
    sr.unwrap();
    let replies = cr.unwrap();

    assert_eq!(replies.len(), MSG_COUNT);
    for (i, r) in replies.iter().enumerate() {
        let expected = format!("item-{i}");
        assert_eq!(r, expected.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Test 5: GBN sender unit-level — window boundary
// ---------------------------------------------------------------------------

#[test]
fn test_gbn_sender_window_boundary() {
    use tcp_over_udp::gbn_sender::GbnSender;

    let mut s = GbnSender::new(0, 3);

    // Fill the window.
    for _ in 0..3u32 {
        let pkt = s.build_data_packet(vec![0u8; 4], 0, 8192);
        s.record_sent(pkt);
    }
    assert!(!s.can_send(), "window should be full");
    assert_eq!(s.in_flight(), 3);

    // ACK the first two.
    let r = s.on_ack(8); // two 4-byte segments
    assert_eq!(r.acked_count, 2);
    assert!(s.can_send(), "one slot should have opened");
    assert_eq!(s.in_flight(), 1);

    // ACK the last one.
    let r = s.on_ack(12);
    assert_eq!(r.acked_count, 1);
    assert!(!s.has_unacked());
}

// ---------------------------------------------------------------------------
// Test 6: GBN receiver unit-level — discard out-of-order
// ---------------------------------------------------------------------------

#[test]
fn test_gbn_receiver_discard_ooo() {
    use tcp_over_udp::gbn_receiver::GbnReceiver;

    let mut r = GbnReceiver::new(0);

    // seq=5 arrives before seq=0 — must be discarded.
    assert!(!r.on_segment(5, b"future"));
    assert_eq!(r.ack_number(), 0, "rcv_nxt must not advance on OOO segment");

    // seq=0 arrives in order — must be accepted.
    assert!(r.on_segment(0, b"hello"));
    assert_eq!(r.ack_number(), 5);

    // seq=5 again — now in order — accepted.
    assert!(r.on_segment(5, b"future"));
    assert_eq!(r.ack_number(), 11);
}

// ---------------------------------------------------------------------------
// Test 7: flush waits for all in-flight ACKs
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_gbn_flush_delivers_all() {
    const WINDOW: usize = 8;
    const MSG_COUNT: usize = 8;

    let server_sock = ephemeral().await;
    let server_addr = server_sock.local_addr;

    let server = tokio::spawn(async move {
        let mut conn = GbnConnection::accept(server_sock, WINDOW)
            .await
            .expect("accept");
        let mut total = 0usize;
        loop {
            match conn.recv().await {
                Ok(data) => total += data.len(),
                Err(ConnError::Eof) => break,
                Err(e) => panic!("server: {e}"),
            }
            if total >= MSG_COUNT * 4 {
                break;
            }
        }
        conn.close().await.ok();
        total
    });

    let client = tokio::spawn(async move {
        let sock = ephemeral().await;
        let mut conn = GbnConnection::connect(sock, server_addr, WINDOW)
            .await
            .expect("connect");

        for _ in 0..MSG_COUNT {
            conn.send(b"data").await.expect("send");
        }
        // flush() must block until every segment is ACKed.
        conn.flush().await.expect("flush");

        // After flush the sender window must be empty.
        assert!(!conn.sender.has_unacked(), "window not empty after flush");

        conn.close().await.expect("close");
    });

    let (sr, cr) = tokio::join!(server, client);
    let total = sr.unwrap();
    cr.unwrap();
    assert_eq!(total, MSG_COUNT * 4);
}

// ---------------------------------------------------------------------------
// Test 8: RTT estimator adapts — RTO falls below initial 1 s after exchanges
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rtt_adapts_on_loopback() {
    // After a handful of loopback ping-pongs the RTT estimator should observe
    // sub-millisecond RTTs and reduce the RTO well below its 1 s initial value.

    let server_sock = ephemeral().await;
    let server_addr = server_sock.local_addr;

    const ROUNDS: usize = 8;

    let server = tokio::spawn(async move {
        let mut conn = GbnConnection::accept(server_sock, 4)
            .await
            .expect("accept");
        for _ in 0..ROUNDS {
            match conn.recv().await {
                Ok(data) => conn.send(&data).await.expect("echo"),
                Err(e) => panic!("server recv: {e}"),
            }
        }
        conn.close().await.ok();
    });

    let client = tokio::spawn(async move {
        let sock = ephemeral().await;
        let mut conn = GbnConnection::connect(sock, server_addr, 4)
            .await
            .expect("connect");

        for i in 0..ROUNDS {
            let msg = format!("rtt-probe-{i}");
            conn.send(msg.as_bytes()).await.expect("send");
            conn.recv().await.expect("recv");
        }

        // After ROUNDS loopback echoes the RTO must be significantly below 1 s.
        let rto = conn.rtt.rto();
        assert!(
            rto < std::time::Duration::from_millis(500),
            "RTO should have adapted below 500 ms after loopback; got {rto:?}"
        );
        // SRTT must be present and tiny.
        let srtt = conn.rtt.srtt().expect("SRTT must be set after exchanges");
        assert!(
            srtt < std::time::Duration::from_millis(100),
            "SRTT should be near-zero on loopback; got {srtt:?}"
        );

        conn.close().await.ok();
    });

    let (sr, cr) = tokio::join!(server, client);
    sr.unwrap();
    cr.unwrap();
}

// ---------------------------------------------------------------------------
// Test 9: Karn's algorithm — retransmit does not poison SRTT
// ---------------------------------------------------------------------------

#[test]
fn test_karn_retransmit_no_sample() {
    use tcp_over_udp::gbn_sender::GbnSender;

    let mut s = GbnSender::new(0, 2);

    // Send two segments.
    let p1 = s.build_data_packet(vec![1u8; 8], 0, 8192);
    s.record_sent(p1);
    let p2 = s.build_data_packet(vec![2u8; 8], 0, 8192);
    s.record_sent(p2);

    // Simulate a timeout: both are retransmitted → tx_count becomes 2.
    s.on_retransmit();
    assert_eq!(s.window_entries().next().unwrap().tx_count, 2);

    // ACK for both: oldest was retransmitted → no RTT sample (Karn's algorithm).
    let r = s.on_ack(16);
    assert_eq!(r.acked_count, 2);
    assert!(
        r.rtt_sample.is_none(),
        "retransmitted oldest segment must suppress RTT sample"
    );
}

// ---------------------------------------------------------------------------
// Test 10: RTT estimator unit — RTO adapts after simulated delay
// ---------------------------------------------------------------------------

#[test]
fn test_rtt_estimator_adapts_to_delay() {
    use std::time::Duration;
    use tcp_over_udp::rtt::RttEstimator;

    let mut rtt = RttEstimator::new();

    // Feed 10 identical 50 ms samples.
    for _ in 0..10 {
        rtt.record_sample(Duration::from_millis(50));
    }

    let srtt = rtt.srtt().expect("SRTT must be set");
    // After 10 samples of 50 ms the SRTT should be close to 50 ms.
    assert!(
        srtt.as_millis().abs_diff(50) <= 5,
        "SRTT should ≈ 50 ms, got {srtt:?}"
    );

    // RTO = SRTT + 4·RTTVAR; with near-zero RTTVAR it should be close to SRTT.
    let rto = rtt.rto();
    assert!(
        rto >= srtt,
        "RTO must be ≥ SRTT, got rto={rto:?} srtt={srtt:?}"
    );
    assert!(
        rto < Duration::from_millis(500),
        "RTO should be well below 500 ms after stable 50 ms samples; got {rto:?}"
    );

    // Feed a sudden spike — RTTVAR should widen and RTO should increase.
    rtt.record_sample(Duration::from_millis(300));
    let rto_after_spike = rtt.rto();
    assert!(
        rto_after_spike > rto,
        "RTO must increase after a RTT spike; before={rto:?} after={rto_after_spike:?}"
    );
}
