//! Deterministic fixture generation. A fixed seed drives the content; exchange timestamps are
//! anchored at generation time and the anchor is recorded in the manifest, so any fixture set
//! stays byte-reproducible via [`generate_at`]. The `gen_fixtures` bin is a thin wrapper
//! around [`generate`]; the drift check in the test suite uses [`generate_at`] to prove the
//! checked-in `fixtures/` never drift from the generator.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::json;

use crate::boundary::{HlEvent, PmEvent};

/// The knobs. `SEED` drives every random choice (same seed → same events); the line counts
/// size the streams; the planted-defect cadences live inline in the generators below
/// (duplicates every 37th/31st event, out-of-order swaps every 41st/43rd, poison and
/// byzantine events appended at the end of each stream).
const SEED: u64 = 0xB17F_0057_D00D_1234;
const HL_LINES: usize = 500;
const PM_LINES: usize = 500;

/// Every file [`generate`] writes, relative to its output directory.
pub const FILES: [&str; 4] = ["hl.ndjson", "pm.ndjson", "matched.ndjson", "manifest.json"];

/// Write the full fixture set into `dir`, anchoring exchange timestamps at the current
/// wall-clock time. The anchor is recorded as `base_ts` in manifest.json, so the exact bytes
/// can always be reproduced later via [`generate_at`] — that is what the drift check does.
pub fn generate(dir: &Path) -> io::Result<()> {
    let base_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_millis() as u64;
    generate_at(dir, base_ts)
}

/// The deterministic core: the same `base_ts` (with the fixed `SEED` above) produces
/// byte-identical output.
pub fn generate_at(dir: &Path, base_ts: u64) -> io::Result<()> {
    fs::create_dir_all(dir)?;

    let mut rng = Rng::new(SEED);
    let hl = generate_hl(&mut rng, base_ts);
    let pm = generate_pm(&mut rng, base_ts);
    let (matched, sum_fees_micro) = generate_matched(base_ts);

    write_ndjson(&dir.join("hl.ndjson"), &hl)?;
    write_ndjson(&dir.join("pm.ndjson"), &pm)?;
    write_ndjson(&dir.join("matched.ndjson"), &matched)?;

    let manifest = json!({
        "base_ts": base_ts,
        "hl": {
            "lines": hl.len(),
            "poison": [
                { "event_id": "hl-poison-unknown", "reason": "UnknownSymbol" },
                { "event_id": "hl-poison-offtick", "reason": "OffTick" },
                { "event_id": "hl-dup-byz", "detected_at": "ingest" }
            ]
        },
        "pm": {
            "lines": pm.len(),
            "poison": [
                { "event_id": "pm-poison-unknown", "reason": "UnknownSymbol" },
                { "event_id": "pm-poison-range", "reason": "PriceOutOfRange" },
                { "event_id": "pm-dup-byz", "detected_at": "ingest" }
            ]
        },
        "matched": {
            "lines": matched.len(),
            "sum_fees_micro": sum_fees_micro,
            "expected_sum_realized_pnl_micro": 0
        }
    });
    write_json(&dir.join("manifest.json"), &manifest)
}

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}

fn generate_hl(rng: &mut Rng, base_ts: u64) -> Vec<HlEvent> {
    let mut events = Vec::with_capacity(HL_LINES);
    let mut last_seq = 999u64;
    let mut ordinal = 0usize;

    while events.len() < HL_LINES - 4 {
        ordinal += 1;
        let out_of_order = ordinal.is_multiple_of(41);
        last_seq += rng.below(2) + 1;
        let seq = last_seq;
        let symbol = if rng.below(10) < 8 {
            "BTC-PERP"
        } else {
            "ETH-PERP"
        };
        let price_ticks = if symbol == "BTC-PERP" {
            133_000 + rng.below(4_001) as i64
        } else {
            68_000 + rng.below(4_001) as i64
        };
        let lots = rng.below(100) as i64 + 1;
        let event = HlEvent {
            seq,
            event_id: format!("hl-{ordinal:04}"),
            ts: base_ts + ordinal as u64 * 11,
            account: format!("acct-{}", rng.below(3) + 1),
            symbol: symbol.to_string(),
            side: if rng.below(2) == 0 { "buy" } else { "sell" }.to_string(),
            px: ticks_to_px_string(symbol, price_ticks),
            qty: lots_to_qty_string(symbol, lots),
            fee: micro_to_fee_string(100 + rng.below(9_901) as i128),
        };
        events.push(event.clone());
        if out_of_order && events.len() >= 2 {
            let last = events.len() - 1;
            events.swap(last - 1, last);
        }
        if ordinal.is_multiple_of(37) && events.len() < HL_LINES - 4 {
            events.push(event);
        }
    }

    last_seq += 1;
    events.push(HlEvent {
        seq: last_seq,
        event_id: "hl-poison-unknown".to_string(),
        ts: base_ts + 90_001,
        account: "acct-1".to_string(),
        symbol: "DOGE-PERP".to_string(),
        side: "buy".to_string(),
        px: "0.10".to_string(),
        qty: "1.0000".to_string(),
        fee: "0.000100".to_string(),
    });
    last_seq += 1;
    events.push(HlEvent {
        seq: last_seq,
        event_id: "hl-poison-offtick".to_string(),
        ts: base_ts + 90_002,
        account: "acct-2".to_string(),
        symbol: "BTC-PERP".to_string(),
        side: "sell".to_string(),
        px: "67412.30".to_string(),
        qty: "0.0030".to_string(),
        fee: "0.000200".to_string(),
    });
    last_seq += 1;
    let byzantine = HlEvent {
        seq: last_seq,
        event_id: "hl-dup-byz".to_string(),
        ts: base_ts + 90_003,
        account: "acct-3".to_string(),
        symbol: "BTC-PERP".to_string(),
        side: "buy".to_string(),
        px: "67412.50".to_string(),
        qty: "0.0020".to_string(),
        fee: "0.000300".to_string(),
    };
    events.push(byzantine.clone());
    events.push(HlEvent {
        qty: "0.0040".to_string(),
        ..byzantine
    });
    assert_eq!(events.len(), HL_LINES);
    events
}

fn generate_pm(rng: &mut Rng, base_ts: u64) -> Vec<PmEvent> {
    let mut events = Vec::with_capacity(PM_LINES);
    let mut last_seq = 87_999u64;
    let mut ordinal = 0usize;

    while events.len() < PM_LINES - 4 {
        ordinal += 1;
        let out_of_order = ordinal.is_multiple_of(43);
        last_seq += rng.below(2) + 1;
        let sequence = last_seq;
        let market = if rng.below(10) < 8 {
            "FED-CUT-SEP"
        } else {
            "CPI-ABOVE-AUG"
        };
        let price_ticks = 100 + rng.below(9_801) as i64;
        let event = PmEvent {
            sequence,
            id: format!("pm-{ordinal:04}"),
            timestamp_ms: base_ts + ordinal as u64 * 13 + 3,
            user: format!("acct-{}", rng.below(3) + 1),
            market: market.to_string(),
            outcome: if rng.below(10) < 8 { "YES" } else { "NO" }.to_string(),
            action: if rng.below(2) == 0 { "BUY" } else { "SELL" }.to_string(),
            price: price_ticks as f64 / 10_000.0,
            size: rng.below(20) as i64 + 1,
            fee_bps: ((rng.below(3) + 1) * 10) as i64,
        };
        events.push(event.clone());
        if out_of_order && events.len() >= 2 {
            let last = events.len() - 1;
            events.swap(last - 1, last);
        }
        if ordinal.is_multiple_of(31) && events.len() < PM_LINES - 4 {
            events.push(event);
        }
    }

    last_seq += 1;
    events.push(PmEvent {
        sequence: last_seq,
        id: "pm-poison-unknown".to_string(),
        timestamp_ms: base_ts + 91_001,
        user: "acct-1".to_string(),
        market: "UNKNOWN-MKT".to_string(),
        outcome: "YES".to_string(),
        action: "BUY".to_string(),
        price: 0.50,
        size: 3,
        fee_bps: 10,
    });
    last_seq += 1;
    events.push(PmEvent {
        sequence: last_seq,
        id: "pm-poison-range".to_string(),
        timestamp_ms: base_ts + 91_002,
        user: "acct-2".to_string(),
        market: "FED-CUT-SEP".to_string(),
        outcome: "YES".to_string(),
        action: "SELL".to_string(),
        price: 0.005,
        size: 4,
        fee_bps: 20,
    });
    last_seq += 1;
    let byzantine = PmEvent {
        sequence: last_seq,
        id: "pm-dup-byz".to_string(),
        timestamp_ms: base_ts + 91_003,
        user: "acct-3".to_string(),
        market: "CPI-ABOVE-AUG".to_string(),
        outcome: "NO".to_string(),
        action: "BUY".to_string(),
        price: 0.30,
        size: 2,
        fee_bps: 30,
    };
    events.push(byzantine.clone());
    events.push(PmEvent {
        size: 5,
        ..byzantine
    });
    assert_eq!(events.len(), PM_LINES);
    events
}

fn generate_matched(base_ts: u64) -> (Vec<HlEvent>, i128) {
    let mut events = Vec::new();
    let mut seq = 200_000u64;
    let mut sum_fees_micro = 0i128;

    for round in 0..25i64 {
        let open_ticks = 133_000 + round * 4;
        let close_ticks = open_ticks + 2 + round % 5;
        let lots = 10 + round % 21;
        let fills = [
            ("mm-A", "buy", open_ticks),
            ("mm-B", "sell", open_ticks),
            ("mm-A", "sell", close_ticks),
            ("mm-B", "buy", close_ticks),
        ];
        for (leg, (account, side, ticks)) in fills.into_iter().enumerate() {
            let fee_micro = 100 + round as i128 * 4 + leg as i128;
            events.push(HlEvent {
                seq,
                event_id: format!("matched-{round:02}-{leg}"),
                ts: base_ts + seq,
                account: account.to_string(),
                symbol: "BTC-PERP".to_string(),
                side: side.to_string(),
                px: ticks_to_px_string("BTC-PERP", ticks),
                qty: lots_to_qty_string("BTC-PERP", lots),
                fee: micro_to_fee_string(fee_micro),
            });
            seq += 1;
            sum_fees_micro += fee_micro;
        }
    }
    (events, sum_fees_micro)
}

fn ticks_to_px_string(symbol: &str, ticks: i64) -> String {
    if symbol == "BTC-PERP" {
        format_scaled(ticks as i128 * 5, 1)
    } else {
        format_scaled(ticks as i128 * 5, 2)
    }
}

fn lots_to_qty_string(symbol: &str, lots: i64) -> String {
    if symbol == "BTC-PERP" {
        format_scaled(lots as i128, 4)
    } else {
        format_scaled(lots as i128, 3)
    }
}

fn micro_to_fee_string(micro: i128) -> String {
    format_scaled(micro, 6)
}

fn format_scaled(value: i128, decimals: u32) -> String {
    let factor = 10i128.pow(decimals);
    let whole = value / factor;
    let fraction = value % factor;
    let width = decimals as usize;
    format!("{whole}.{fraction:0width$}")
}

fn write_ndjson<T: Serialize>(path: &Path, values: &[T]) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    for value in values {
        serde_json::to_writer(&mut writer, value).map_err(io::Error::other)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let mut writer = BufWriter::new(File::create(path)?);
    serde_json::to_writer_pretty(&mut writer, value).map_err(io::Error::other)?;
    writer.write_all(b"\n")?;
    writer.flush()
}
