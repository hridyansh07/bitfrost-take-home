//! cargo test -p ingester --test ordering_realtime  -- --nocapture 2>&1
//!
//! Realtime demonstration of the Algo.md pipeline: two threaded venue streams replay the
//! fixture files, each event is stamped with its genuine receive time (the Instant it comes
//! off the channel-as-socket), and `Ingester::fill_caught` pushes the survivors straight onto
//! the sequencer lanes. The emitted tape is checked against the Algo.md invariants and written
//! to ordering_algo.json — under `just realtime` that lands in the repo's out/ directory;
//! plain `cargo test` writes to a temp directory so default test runs don't touch the repo.
mod common;

use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ingester::{Ingester, Sequencer};
use types::{read_hl, read_pm, RawLine, Registry, Venue};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

fn emit_json(name: &str, entries: Vec<serde_json::Value>) -> PathBuf {
    let out_dir = std::env::var_os("BITFROST_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("bitfrost-out"));
    std::fs::create_dir_all(&out_dir).unwrap();
    let path = out_dir.join(name);
    let body = serde_json::to_string_pretty(&serde_json::Value::Array(entries)).unwrap();
    std::fs::write(&path, body).unwrap();
    path.canonicalize().unwrap()
}

/// recv_ts is a real epoch timestamp: wall-clock millis captured once at start
/// advanced by the shared monotonic Instant. Ordering stays Instant-driven,
fn spawn_venue_stream(
    lines: Vec<RawLine>,
    epoch_base_ms: u64,
    start: Instant,
    seed: u64,
) -> mpsc::Receiver<(u64, RawLine)> {
    let lines = common::sorted_by_seq(lines); // producers deliver in venue_seq order
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut rng = seed;
        for line in lines {
            thread::sleep(Duration::from_micros(common::splitmix64(&mut rng) % 150));
            let recv_ts = epoch_base_ms + start.elapsed().as_millis() as u64;
            sender.send((recv_ts, line)).unwrap();
        }
    });
    receiver
}

#[test]
fn realtime_recv_ts_stamping_upholds_algo_invariants() {
    let fixture_dir = fixture_dir();
    let hl = read_hl(&fixture_dir.join("hl.ndjson")).unwrap();
    let pm = read_pm(&fixture_dir.join("pm.ndjson")).unwrap();

    let epoch_base_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let start = Instant::now();
    let hl_stream = spawn_venue_stream(hl, epoch_base_ms, start, 7);
    let pm_stream = spawn_venue_stream(pm, epoch_base_ms, start, 99);
    let hl_arrivals: Vec<(u64, RawLine)> = hl_stream.into_iter().collect();
    let pm_arrivals: Vec<(u64, RawLine)> = pm_stream.into_iter().collect();
    let elapsed = start.elapsed();

    // Stamps are epoch-anchored yet still monotonic per venue (no walk-back).
    for arrivals in [&hl_arrivals, &pm_arrivals] {
        assert!(arrivals.windows(2).all(|pair| pair[0].0 <= pair[1].0));
        assert!(arrivals
            .iter()
            .all(|(recv_ts, _)| *recv_ts >= epoch_base_ms));
    }

    // Replay the merged arrival order through the pipeline: stamp → canonicalize → dedup →
    // lane push, with incremental drains along the way.
    let mut ingester = Ingester::new(Registry::standard());
    let mut tape = Vec::new();
    let mut last_stamp = 0u64;
    let mut hl_iter = hl_arrivals.into_iter().peekable();
    let mut pm_iter = pm_arrivals.into_iter().peekable();
    loop {
        let take_hl = match (hl_iter.peek(), pm_iter.peek()) {
            (None, None) => break,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some((hl_ts, _)), Some((pm_ts, _))) => hl_ts <= pm_ts,
        };
        let (recv_ts, line) = if take_hl {
            hl_iter.next()
        } else {
            pm_iter.next()
        }
        .unwrap();
        ingester.fill_caught(recv_ts, line);
        tape.extend(ingester.drain_ready());
        last_stamp = last_stamp.max(recv_ts);
    }

    // I4 liveness: with both streams idle, ticks alone finish the tape — no flush needed.
    ingester.tick(Venue::Hl, last_stamp + 1);
    ingester.tick(Venue::Pm, last_stamp + 1);
    tape.extend(ingester.drain_ready());
    assert_eq!(ingester.sequencer().depth(), 0);
    assert!(ingester.flush().is_empty());

    assert_eq!(tape.len(), ingester.stats().accepted);
    assert_eq!(ingester.stats().out_of_order, 0);

    // I3: global_seq is exactly 1..=n and recv_ts never walks back.
    assert!(tape
        .iter()
        .zip(1u64..)
        .all(|(out, expected)| out.global_seq == expected));
    assert!(tape
        .windows(2)
        .all(|pair| pair[0].recv_ts <= pair[1].recv_ts));

    // I1: each venue's fills come out in venue_seq order.
    for venue in [Venue::Hl, Venue::Pm] {
        let seqs: Vec<u64> = tape
            .iter()
            .filter(|out| out.fill.venue == venue)
            .map(|out| out.fill.seq)
            .collect();
        assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]));
    }

    // I7: equal recv_ts collisions resolve by venue priority (Hl < Pm).
    let cross_venue_ties = tape
        .windows(2)
        .filter(|pair| {
            pair[0].recv_ts == pair[1].recv_ts && pair[0].fill.venue != pair[1].fill.venue
        })
        .inspect(|pair| {
            assert_eq!(pair[0].fill.venue, Venue::Hl);
            assert_eq!(pair[1].fill.venue, Venue::Pm);
        })
        .count();

    // I2/I5: the tape is a pure function of the stamped fills — a bare sequencer fed the same
    // stamps in a completely different push interleaving (venue-major) with a single flush
    // reproduces it exactly, global_seq included.
    let mut replay = Sequencer::new();
    for wanted in [Venue::Hl, Venue::Pm] {
        for out in tape.iter().filter(|out| out.fill.venue == wanted) {
            replay
                .push(out.fill.venue, out.recv_ts, out.fill.clone())
                .unwrap();
        }
    }
    assert_eq!(replay.flush(), tape);

    // Whenever the two streams overlapped in time (they practically always do), the tape must
    // interleave venues rather than group them.
    let last_hl_stamp = tape
        .iter()
        .filter(|out| out.fill.venue == Venue::Hl)
        .map(|out| out.recv_ts)
        .max()
        .unwrap();
    let first_pm_stamp = tape
        .iter()
        .filter(|out| out.fill.venue == Venue::Pm)
        .map(|out| out.recv_ts)
        .min()
        .unwrap();
    if first_pm_stamp < last_hl_stamp {
        let first_pm_index = tape
            .iter()
            .position(|out| out.fill.venue == Venue::Pm)
            .unwrap();
        assert!(
            tape[first_pm_index..]
                .iter()
                .any(|out| out.fill.venue == Venue::Hl),
            "streams overlapped in time but the tape is venue-grouped"
        );
    }

    let algo_entries = tape
        .iter()
        .map(|out| {
            serde_json::json!({
                "global_seq": out.global_seq,
                "recv_ts": out.recv_ts,
                "exchange_ts_ms": out.fill.ts_ms,
                "venue": format!("{:?}", out.fill.venue),
                "event_id": out.fill.event_id,
                "venue_seq": out.fill.seq,
            })
        })
        .collect();
    println!(
        "wrote {}",
        emit_json("ordering_algo.json", algo_entries).display()
    );

    println!("global_seq | recv_ts | venue | event_id");
    for out in tape.iter().take(10) {
        println!(
            "{:>10} | {} | {:<5} | {}",
            out.global_seq,
            out.recv_ts,
            format!("{:?}", out.fill.venue),
            out.fill.event_id
        );
    }
    let distinct_stamps = tape
        .iter()
        .map(|out| out.recv_ts)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    println!(
        "sequenced {} fills in {:?} | epoch base: {} | distinct recv_ts: {} | cross-venue ties broken by venue: {}",
        tape.len(),
        elapsed,
        epoch_base_ms,
        distinct_stamps,
        cross_venue_ties
    );
}
