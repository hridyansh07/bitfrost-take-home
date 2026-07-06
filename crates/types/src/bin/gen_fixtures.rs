use std::collections::BTreeMap;
use std::error::Error;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::json;
use types::{HlEvent, PmEvent};

const SEED: u64 = 0xB17F_0057_D00D_1234;
const HL_LINES: usize = 500;
const PM_LINES: usize = 500;
const BASE_TS: u64 = 1_720_000_000_000;

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

fn main() -> Result<(), Box<dyn Error>> {
    let output = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("fixtures"));
    fs::create_dir_all(&output)?;

    let mut rng = Rng::new(SEED);
    let hl = generate_hl(&mut rng);
    let pm = generate_pm(&mut rng);
    let (matched, sum_fees_micro) = generate_matched();

    write_ndjson(&output.join("hl.ndjson"), &hl)?;
    write_ndjson(&output.join("pm.ndjson"), &pm)?;
    write_ndjson(&output.join("matched.ndjson"), &matched)?;

    let marks = BTreeMap::from([
        ("BTC-PERP", 67_000.0),
        ("CPI-ABOVE-AUG", 0.30),
        ("ETH-PERP", 3_500.0),
        ("FED-CUT-SEP", 0.62),
    ]);
    write_json(&output.join("marks.json"), &marks)?;

    let manifest = json!({
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
    write_json(&output.join("manifest.json"), &manifest)?;
    Ok(())
}

fn generate_hl(rng: &mut Rng) -> Vec<HlEvent> {
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
            ts: BASE_TS + ordinal as u64 * 11,
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
        ts: BASE_TS + 90_001,
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
        ts: BASE_TS + 90_002,
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
        ts: BASE_TS + 90_003,
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

fn generate_pm(rng: &mut Rng) -> Vec<PmEvent> {
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
            timestamp_ms: BASE_TS + ordinal as u64 * 13 + 3,
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
        timestamp_ms: BASE_TS + 91_001,
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
        timestamp_ms: BASE_TS + 91_002,
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
        timestamp_ms: BASE_TS + 91_003,
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

fn generate_matched() -> (Vec<HlEvent>, i128) {
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
                ts: BASE_TS + 200_000 + seq - 200_000,
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

fn write_ndjson<T: Serialize>(path: &Path, values: &[T]) -> Result<(), Box<dyn Error>> {
    let mut writer = BufWriter::new(File::create(path)?);
    for value in values {
        serde_json::to_writer(&mut writer, value)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<(), Box<dyn Error>> {
    let mut writer = BufWriter::new(File::create(path)?);
    serde_json::to_writer_pretty(&mut writer, value)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}
