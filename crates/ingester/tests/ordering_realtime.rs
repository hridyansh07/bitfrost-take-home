/// cargo test -p ingester --test ordering_realtime  -- --nocapture 2>&1
/// 
/// Test current vs proposed algorithm by running the invariants on the current algorithm to devise the 
/// proposed pattern of events uses the fixtures .ndjson files to run the test 
/// Outputs /out/ordering_algo.json. and /out/ordering_current.json with the proposed order
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ingester::Ingester;
use types::{read_hl, read_pm, Fill, RawEvent, RawLine, Registry, Venue};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

fn emit_json(name: &str, entries: Vec<serde_json::Value>) -> PathBuf {
    let out_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../out");
    std::fs::create_dir_all(&out_dir).unwrap();
    let path = out_dir.join(name);
    let body = serde_json::to_string_pretty(&serde_json::Value::Array(entries)).unwrap();
    std::fs::write(&path, body).unwrap();
    path.canonicalize().unwrap()
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SequencedFill {
    global_seq: u64,
    recv_ts: u64,
    fill: Fill,
}

/// Stub of the Algo.md merger: SORT_KEY = (recv_ts, venue_priority, venue_seq).
fn sequence_fills(mut stamped: Vec<(u64, Fill)>) -> Vec<SequencedFill> {
    stamped.sort_by_key(|(recv_ts, fill)| (*recv_ts, fill.venue, fill.seq));
    stamped
        .into_iter()
        .zip(1u64..)
        .map(|((recv_ts, fill), global_seq)| SequencedFill {
            global_seq,
            recv_ts,
            fill,
        })
        .collect()
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

/// recv_ts is a real epoch timestamp: wall-clock millis captured once at start
/// advanced by the shared monotonic Instant. Ordering stays Instant-driven, 
fn spawn_venue_stream(
    mut lines: Vec<RawLine>,
    epoch_base_ms: u64,
    start: Instant,
    seed: u64,
) -> mpsc::Receiver<(u64, RawLine)> {
    lines.sort_by_key(|line| line.parsed.as_ref().map(RawEvent::seq).unwrap_or(u64::MAX));
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let mut rng = seed;
        for line in lines {
            thread::sleep(Duration::from_micros(splitmix64(&mut rng) % 150));
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
        assert!(arrivals.iter().all(|(recv_ts, _)| *recv_ts >= epoch_base_ms));
    }

    let mut ingester = Ingester::new(Registry::standard());
    let mut recv_map: BTreeMap<(Venue, String), u64> = BTreeMap::new();
    for (recv_ts, line) in hl_arrivals.into_iter().chain(pm_arrivals) {
        if let Ok(event) = &line.parsed {
            recv_map
                .entry((line.venue, event.event_id().to_string()))
                .or_insert(recv_ts);
        }
        ingester.ingest_line(line);
    }

    let current = ingester.ordered_fills();
    let stamped: Vec<(u64, Fill)> = current
        .iter()
        .map(|fill| (recv_map[&(fill.venue, fill.event_id.clone())], fill.clone()))
        .collect();
    let sequenced = sequence_fills(stamped.clone());

    assert_eq!(sequenced.len(), ingester.stats().accepted);

    // I3: global_seq is exactly 1..=n and recv_ts never walks back.
    assert!(sequenced
        .iter()
        .zip(1u64..)
        .all(|(out, expected)| out.global_seq == expected));
    assert!(sequenced
        .windows(2)
        .all(|pair| pair[0].recv_ts <= pair[1].recv_ts));

    // I1: each venue's fills come out in venue_seq order.
    for venue in [Venue::Hl, Venue::Pm] {
        let seqs: Vec<u64> = sequenced
            .iter()
            .filter(|out| out.fill.venue == venue)
            .map(|out| out.fill.seq)
            .collect();
        assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]));
    }

    // I7: equal recv_ts collisions resolve by venue priority (Hl < Pm).
    let cross_venue_ties = sequenced
        .windows(2)
        .filter(|pair| pair[0].recv_ts == pair[1].recv_ts && pair[0].fill.venue != pair[1].fill.venue)
        .inspect(|pair| {
            assert_eq!(pair[0].fill.venue, Venue::Hl);
            assert_eq!(pair[1].fill.venue, Venue::Pm);
        })
        .count();

    // I2/I5: replaying the stamped set in any order reproduces the tape.
    let mut replay = stamped;
    let mut rng = 2024u64;
    for i in (1..replay.len()).rev() {
        let j = (splitmix64(&mut rng) % (i as u64 + 1)) as usize;
        replay.swap(i, j);
    }
    assert_eq!(sequence_fills(replay), sequenced);

    // Today's ordered_fills() is venue-grouped: the whole HL block precedes PM.
    assert_eq!(current[0].venue, Venue::Hl);
    let first_pm = current
        .iter()
        .position(|fill| fill.venue == Venue::Pm)
        .unwrap();
    assert!(current[first_pm..].iter().all(|fill| fill.venue == Venue::Pm));

    // Whenever the two streams overlapped in time (they practically always
    // do), the algo order must interleave venues and diverge from the
    // venue-grouped order.
    let current_ids: Vec<&str> = current.iter().map(|fill| fill.event_id.as_str()).collect();
    let new_ids: Vec<&str> = sequenced
        .iter()
        .map(|out| out.fill.event_id.as_str())
        .collect();
    let last_hl_stamp = sequenced
        .iter()
        .filter(|out| out.fill.venue == Venue::Hl)
        .map(|out| out.recv_ts)
        .max()
        .unwrap();
    let first_pm_stamp = sequenced
        .iter()
        .filter(|out| out.fill.venue == Venue::Pm)
        .map(|out| out.recv_ts)
        .min()
        .unwrap();
    if first_pm_stamp <= last_hl_stamp {
        assert_ne!(current_ids, new_ids);
    }

    let current_entries = current
        .iter()
        .enumerate()
        .map(|(position, fill)| {
            serde_json::json!({
                "position": position,
                "recv_ts": recv_map[&(fill.venue, fill.event_id.clone())],
                "exchange_ts_ms": fill.ts_ms,
                "venue": format!("{:?}", fill.venue),
                "event_id": fill.event_id,
                "venue_seq": fill.seq,
            })
        })
        .collect();
    let algo_entries = sequenced
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
        emit_json("ordering_current.json", current_entries).display()
    );
    println!(
        "wrote {}",
        emit_json("ordering_algo.json", algo_entries).display()
    );

    println!("pos | current (venue-grouped) | algo (recv_ts, venue, seq)");
    for i in 0..10 {
        println!("{:>3} | {:<23} | {}", i, current_ids[i], new_ids[i]);
    }
    let distinct_stamps = sequenced
        .iter()
        .map(|out| out.recv_ts)
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    println!(
        "sequenced {} fills in {:?} | epoch base: {} | distinct recv_ts: {} | cross-venue ties broken by venue: {}",
        sequenced.len(),
        elapsed,
        epoch_base_ms,
        distinct_stamps,
        cross_venue_ties
    );
}
