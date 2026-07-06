use std::fs;
use std::path::Path;

use serde::de::DeserializeOwned;

use crate::boundary::{HlEvent, PmEvent};
use crate::instrument::Venue;

#[derive(Clone, Debug, PartialEq)]
pub enum RawEvent {
    Hl(HlEvent),
    Pm(PmEvent),
}

pub trait BoundaryEvent: DeserializeOwned {
    const VENUE: Venue;

    fn into_raw(self) -> RawEvent;
}

impl BoundaryEvent for HlEvent {
    const VENUE: Venue = Venue::Hl;

    fn into_raw(self) -> RawEvent {
        RawEvent::Hl(self)
    }
}

impl BoundaryEvent for PmEvent {
    const VENUE: Venue = Venue::Pm;

    fn into_raw(self) -> RawEvent {
        RawEvent::Pm(self)
    }
}

impl RawEvent {
    pub fn venue(&self) -> Venue {
        match self {
            RawEvent::Hl(_) => Venue::Hl,
            RawEvent::Pm(_) => Venue::Pm,
        }
    }

    pub fn ts(&self) -> u64 {
        match self {
            RawEvent::Hl(event) => event.ts,
            RawEvent::Pm(event) => event.timestamp_ms,
        }
    }

    pub fn seq(&self) -> u64 {
        match self {
            RawEvent::Hl(event) => event.seq,
            RawEvent::Pm(event) => event.sequence,
        }
    }

    pub fn event_id(&self) -> &str {
        match self {
            RawEvent::Hl(event) => &event.event_id,
            RawEvent::Pm(event) => &event.id,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RawLine {
    pub venue: Venue,
    pub arrival: usize,
    pub text: String,
    pub parsed: Result<RawEvent, String>,
}

pub fn read_hl(path: &Path) -> std::io::Result<Vec<RawLine>> {
    read_ndjson::<HlEvent>(path)
}

pub fn read_pm(path: &Path) -> std::io::Result<Vec<RawLine>> {
    read_ndjson::<PmEvent>(path)
}

pub fn read_ndjson<T>(path: &Path) -> std::io::Result<Vec<RawLine>>
where
    T: BoundaryEvent,
{
    let input = fs::read_to_string(path)?;
    let mut lines = Vec::new();
    for line in input.lines().filter(|line| !line.trim().is_empty()) {
        let arrival = lines.len();
        let parsed = serde_json::from_str::<T>(line)
            .map(BoundaryEvent::into_raw)
            .map_err(|error| error.to_string());
        lines.push(RawLine {
            venue: T::VENUE,
            arrival,
            text: line.to_string(),
            parsed,
        });
    }
    Ok(lines)
}

pub fn merge(hl: Vec<RawLine>, pm: Vec<RawLine>) -> Vec<RawLine> {
    let mut lines = hl;
    lines.extend(pm);
    lines.sort_by_key(|line| {
        (
            line.parsed.as_ref().map(RawEvent::ts).unwrap_or(u64::MAX),
            venue_rank(line.venue),
            line.arrival,
        )
    });
    lines
}

pub fn shuffle(lines: &mut [RawLine], seed: u64) {
    let mut state = seed;
    for i in (1..lines.len()).rev() {
        let j = (splitmix64(&mut state) % (i as u64 + 1)) as usize;
        lines.swap(i, j);
    }
}

fn venue_rank(venue: Venue) -> u8 {
    match venue {
        Venue::Hl => 0,
        Venue::Pm => 1,
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hl_event(ts: u64) -> HlEvent {
        HlEvent {
            seq: 1,
            event_id: "hl".to_string(),
            ts,
            account: "acct".to_string(),
            symbol: "BTC-PERP".to_string(),
            side: "buy".to_string(),
            px: "1.0".to_string(),
            qty: "0.0001".to_string(),
            fee: "0.0".to_string(),
        }
    }

    fn pm_event(ts: u64) -> PmEvent {
        PmEvent {
            sequence: 1,
            id: "pm".to_string(),
            timestamp_ms: ts,
            user: "acct".to_string(),
            market: "FED-CUT-SEP".to_string(),
            outcome: "YES".to_string(),
            action: "BUY".to_string(),
            price: 0.5,
            size: 1,
            fee_bps: 0,
        }
    }

    #[test]
    fn reader_skips_blank_lines_and_captures_parse_failures() {
        let path = std::env::temp_dir().join(format!(
            "bitfrost-stream-{}-{}.ndjson",
            std::process::id(),
            std::thread::current().name().unwrap_or("reader")
        ));
        let valid = serde_json::to_string(&hl_event(1)).unwrap();
        std::fs::write(&path, format!("\n{valid}\nnot-json\n\n")).unwrap();

        let lines = read_hl(&path).unwrap();
        std::fs::remove_file(path).unwrap();

        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].arrival, 0);
        assert!(lines[0].parsed.is_ok());
        assert_eq!(lines[1].arrival, 1);
        assert_eq!(lines[1].text, "not-json");
        assert!(lines[1].parsed.is_err());
    }

    #[test]
    fn generic_reader_uses_boundary_event_venue_and_conversion() {
        let path = std::env::temp_dir().join(format!(
            "bitfrost-generic-stream-{}-{}.ndjson",
            std::process::id(),
            std::thread::current().name().unwrap_or("reader")
        ));
        let valid = serde_json::to_string(&pm_event(9)).unwrap();
        std::fs::write(&path, valid).unwrap();

        let lines = read_ndjson::<PmEvent>(&path).unwrap();
        std::fs::remove_file(path).unwrap();

        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].venue, Venue::Pm);
        assert!(matches!(lines[0].parsed, Ok(RawEvent::Pm(_))));
    }

    #[test]
    fn merge_uses_venue_then_arrival_for_timestamp_ties() {
        let hl = RawLine {
            venue: Venue::Hl,
            arrival: 1,
            text: String::new(),
            parsed: Ok(RawEvent::Hl(hl_event(10))),
        };
        let pm = RawLine {
            venue: Venue::Pm,
            arrival: 0,
            text: String::new(),
            parsed: Ok(RawEvent::Pm(pm_event(10))),
        };
        let invalid = RawLine {
            venue: Venue::Hl,
            arrival: 2,
            text: "bad".to_string(),
            parsed: Err("bad".to_string()),
        };

        let merged = merge(vec![invalid, hl], vec![pm]);

        assert_eq!(merged[0].venue, Venue::Hl);
        assert_eq!(merged[1].venue, Venue::Pm);
        assert!(merged[2].parsed.is_err());
    }
}
