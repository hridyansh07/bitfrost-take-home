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
        let parsed = serde_json::from_str::<T>(line)
            .map(BoundaryEvent::into_raw)
            .map_err(|error| error.to_string());
        lines.push(RawLine {
            venue: T::VENUE,
            text: line.to_string(),
            parsed,
        });
    }
    Ok(lines)
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
        assert!(lines[0].parsed.is_ok());
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
}
