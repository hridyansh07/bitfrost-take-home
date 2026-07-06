use std::collections::{BTreeMap, BTreeSet};

use position_keeper::PositionStore;
use types::{Canonicalize, Fill, InstrumentSource, RawEvent, RawLine, RejectReason, Venue};

/// Shift to types crate
pub trait FillSink {
    fn fill_caught(&mut self, event: RawEvent) -> IngestOutcome;
}

pub struct Ingester<S> {
    instruments: S,
    seen: BTreeMap<(Venue, String), Fill>,
    park: BTreeMap<(Venue, u64, String), Fill>,
    dead_letters: Vec<DeadLetter>,
    alerts: Vec<Alert>,
    max_seq: BTreeMap<Venue, u64>,
    stats: IngestStats,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestOutcome {
    Accepted,
    Duplicate,
    DeadLettered(DeadLetterReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeadLetterReason {
    Malformed(String),
    Rejected(RejectReason),
    Byzantine,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetter {
    pub venue: Venue,
    pub event_id: String,
    pub reason: DeadLetterReason,
    pub text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Alert {
    pub venue: Venue,
    pub event_id: String,
    pub kind: AlertKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlertKind {
    ByzantineDuplicate,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IngestStats {
    pub accepted: usize,
    pub duplicates: usize,
    pub byzantine: usize,
    pub dead_lettered: usize,
    pub out_of_order: usize,
    pub gaps: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DriveReport {
    pub applied: usize,
    pub apply_errors: Vec<(String, RejectReason)>,
}

impl<S: InstrumentSource> Ingester<S> {
    pub fn new(instruments: S) -> Self {
        Self {
            instruments,
            seen: BTreeMap::new(),
            park: BTreeMap::new(),
            dead_letters: Vec::new(),
            alerts: Vec::new(),
            max_seq: BTreeMap::new(),
            stats: IngestStats::default(),
        }
    }

    /// main.rs should drive this
    pub fn fill_caught(&mut self, event: RawEvent) -> IngestOutcome {
        let venue = event.venue();
        let event_id = event.event_id().to_string();
        let fill = match event.canonicalize(&self.instruments) {
            Ok(fill) => fill,
            Err(reason) => {
                let dead_letter_reason = DeadLetterReason::Rejected(reason);
                self.dead_letters.push(DeadLetter {
                    venue,
                    event_id,
                    reason: dead_letter_reason.clone(),
                    text: None,
                });
                self.stats.dead_lettered += 1;
                return IngestOutcome::DeadLettered(dead_letter_reason);
            }
        };
        let key = (venue, event_id.clone());

        match self.seen.get(&key) {
            None => {
                if self
                    .max_seq
                    .get(&venue)
                    .is_some_and(|max_seq| fill.seq < *max_seq)
                {
                    self.stats.out_of_order += 1;
                }
                self.max_seq
                    .entry(venue)
                    .and_modify(|max_seq| *max_seq = (*max_seq).max(fill.seq))
                    .or_insert(fill.seq);
                self.park.insert((venue, fill.seq, event_id), fill.clone());
                self.seen.insert(key, fill);
                self.stats.accepted += 1;
                IngestOutcome::Accepted
            }
            Some(previous) if previous == &fill => {
                self.stats.duplicates += 1;
                IngestOutcome::Duplicate
            }
            Some(previous) => {
                let previous = previous.clone();
                let kept = previous.clone().min(fill);
                self.alerts.push(Alert {
                    venue,
                    event_id: event_id.clone(),
                    kind: AlertKind::ByzantineDuplicate,
                });
                self.dead_letters.push(DeadLetter {
                    venue,
                    event_id: event_id.clone(),
                    reason: DeadLetterReason::Byzantine,
                    text: None,
                });
                self.stats.byzantine += 1;
                self.stats.dead_lettered += 1;
                if kept != previous {
                    self.park.remove(&(venue, previous.seq, event_id.clone()));
                    self.park
                        .insert((venue, kept.seq, event_id.clone()), kept.clone());
                    self.seen.insert(key, kept);
                }
                IngestOutcome::DeadLettered(DeadLetterReason::Byzantine)
            }
        }
    }

    /// Stub to go over the fixtured values
    pub fn ingest_line(&mut self, line: RawLine) -> IngestOutcome {
        let RawLine {
            venue,
            text,
            parsed,
            ..
        } = line;
        match parsed {
            Ok(event) => {
                let dead_letter_count = self.dead_letters.len();
                let outcome = self.fill_caught(event);
                if self.dead_letters.len() > dead_letter_count {
                    self.dead_letters.last_mut().unwrap().text = Some(text);
                }
                outcome
            }
            Err(error) => {
                let reason = DeadLetterReason::Malformed(error);
                self.dead_letters.push(DeadLetter {
                    venue,
                    event_id: String::new(),
                    reason: reason.clone(),
                    text: Some(text),
                });
                self.stats.dead_lettered += 1;
                IngestOutcome::DeadLettered(reason)
            }
        }
    }

    pub fn ordered_fills(&self) -> Vec<Fill> {
        [Venue::Hl, Venue::Pm]
            .into_iter()
            .flat_map(|venue| self.venue_fills(venue))
            .collect()
    }

    pub fn drive<K: PositionStore>(&self, keeper: &mut K) -> DriveReport {
        let mut report = DriveReport::default();
        for fill in self.ordered_fills() {
            match keeper.apply(&fill) {
                Ok(_) => report.applied += 1,
                Err(reason) => report.apply_errors.push((fill.event_id.clone(), reason)),
            }
        }
        report
    }

    pub fn dead_letters(&self) -> &[DeadLetter] {
        &self.dead_letters
    }

    pub fn alerts(&self) -> &[Alert] {
        &self.alerts
    }

    pub fn stats(&self) -> IngestStats {
        let mut stats = self.stats;
        stats.gaps = self.gap_count();
        stats
    }

    fn venue_fills(&self, venue: Venue) -> Vec<Fill> {
        self.park
            .iter()
            .filter(|((candidate, _, _), _)| *candidate == venue)
            .map(|(_, fill)| fill.clone())
            .collect()
    }

    fn gap_count(&self) -> usize {
        [Venue::Hl, Venue::Pm]
            .into_iter()
            .map(|venue| {
                self.park
                    .keys()
                    .filter_map(|(candidate, seq, _)| (*candidate == venue).then_some(*seq))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>()
                    .windows(2)
                    .map(|pair| pair[1].saturating_sub(pair[0]).saturating_sub(1))
                    .fold(0u64, u64::saturating_add)
            })
            .fold(0u64, u64::saturating_add)
            .try_into()
            .unwrap_or(usize::MAX)
    }
}

impl<S: InstrumentSource> FillSink for Ingester<S> {
    fn fill_caught(&mut self, event: RawEvent) -> IngestOutcome {
        Ingester::fill_caught(self, event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use position_keeper::PositionKeeper;
    use types::{HlEvent, Lots, PmEvent, RawEvent, Registry};

    fn event(event_id: &str, seq: u64, qty: &str) -> RawEvent {
        RawEvent::Hl(HlEvent {
            seq,
            event_id: event_id.to_string(),
            ts: 1,
            account: "account".to_string(),
            symbol: "BTC-PERP".to_string(),
            side: "buy".to_string(),
            px: "100.0".to_string(),
            qty: qty.to_string(),
            fee: "0.0".to_string(),
        })
    }

    fn pm_event(event_id: &str, seq: u64) -> RawEvent {
        RawEvent::Pm(PmEvent {
            sequence: seq,
            id: event_id.to_string(),
            timestamp_ms: 1,
            user: "account".to_string(),
            market: "FED-CUT-SEP".to_string(),
            outcome: "YES".to_string(),
            action: "BUY".to_string(),
            price: 0.5,
            size: 1,
            fee_bps: 0,
        })
    }

    #[test]
    fn applies_exactly_once_by_venue_and_event_id() {
        let mut ingester = Ingester::new(Registry::standard());
        assert_eq!(
            ingester.fill_caught(event("event", 1, "0.002")),
            IngestOutcome::Accepted
        );
        assert_eq!(
            ingester.fill_caught(event("event", 1, "0.002")),
            IngestOutcome::Duplicate
        );
        assert_eq!(ingester.stats().accepted, 1);
        assert_eq!(ingester.stats().duplicates, 1);
        assert_eq!(ingester.park.len(), 1);
    }

    #[test]
    fn selects_same_byzantine_winner_in_both_arrival_orders() {
        fn run(first: &str, second: &str) -> (Vec<Fill>, Vec<DeadLetter>, Vec<Alert>) {
            let mut ingester = Ingester::new(Registry::standard());
            assert_eq!(
                ingester.fill_caught(event("event", 1, first)),
                IngestOutcome::Accepted
            );
            assert_eq!(
                ingester.fill_caught(event("event", 1, second)),
                IngestOutcome::DeadLettered(DeadLetterReason::Byzantine)
            );
            (
                ingester.park.into_values().collect(),
                ingester.dead_letters,
                ingester.alerts,
            )
        }

        let forward = run("0.002", "0.004");
        let reverse = run("0.004", "0.002");
        assert_eq!(forward.0, reverse.0);
        assert_eq!(forward.0[0].qty, Lots(20));
        assert_eq!(forward.1.len(), 1);
        assert_eq!(reverse.1.len(), 1);
        assert_eq!(forward.2.len(), 1);
        assert_eq!(reverse.2.len(), 1);
    }

    #[test]
    fn rejects_unknown_instrument_into_dead_letters() {
        let mut unknown = event("unknown", 1, "0.002");
        let RawEvent::Hl(raw) = &mut unknown else {
            unreachable!()
        };
        raw.symbol = "UNKNOWN".to_string();
        let mut ingester = Ingester::new(Registry::standard());
        assert!(matches!(
            ingester.fill_caught(unknown),
            IngestOutcome::DeadLettered(DeadLetterReason::Rejected(
                RejectReason::UnknownSymbol(symbol)
            )) if symbol == "UNKNOWN"
        ));
        assert!(ingester.park.is_empty());
        assert_eq!(ingester.dead_letters.len(), 1);
    }

    #[test]
    fn preserves_malformed_line_text() {
        let mut ingester = Ingester::new(Registry::standard());
        let outcome = ingester.ingest_line(RawLine {
            venue: Venue::Hl,
            arrival: 0,
            text: "not-json".to_string(),
            parsed: Err("expected value".to_string()),
        });
        assert_eq!(
            outcome,
            IngestOutcome::DeadLettered(DeadLetterReason::Malformed("expected value".to_string()))
        );
        assert_eq!(ingester.dead_letters[0].text.as_deref(), Some("not-json"));
    }

    #[test]
    fn backfills_raw_text_for_normalization_dead_letter() {
        let mut raw = event("unknown", 1, "0.002");
        let RawEvent::Hl(event) = &mut raw else {
            unreachable!()
        };
        event.symbol = "UNKNOWN".to_string();
        let mut ingester = Ingester::new(Registry::standard());
        ingester.ingest_line(RawLine {
            venue: Venue::Hl,
            arrival: 0,
            text: "source".to_string(),
            parsed: Ok(raw),
        });
        assert_eq!(ingester.dead_letters[0].text.as_deref(), Some("source"));
    }

    #[test]
    fn counts_out_of_order_arrivals_and_sequence_gaps() {
        let mut ingester = Ingester::new(Registry::standard());
        ingester.fill_caught(event("third", 5, "0.002"));
        ingester.fill_caught(event("first", 1, "0.002"));
        ingester.fill_caught(event("second", 3, "0.002"));
        assert_eq!(ingester.stats().out_of_order, 2);
        assert_eq!(ingester.stats().gaps, 2);
    }

    #[test]
    fn orders_each_venue_by_seq_and_tolerates_gaps() {
        let mut ingester = Ingester::new(Registry::standard());
        ingester.fill_caught(event("hl-later", 3, "0.002"));
        ingester.fill_caught(pm_event("pm-second", 11));
        ingester.fill_caught(event("hl-first", 1, "0.002"));
        ingester.fill_caught(pm_event("pm-first", 10));

        let fills = ingester.ordered_fills();
        assert_eq!(
            fills
                .iter()
                .map(|fill| fill.event_id.as_str())
                .collect::<Vec<_>>(),
            ["hl-first", "hl-later", "pm-first", "pm-second"]
        );
    }

    #[test]
    fn ordering_is_independent_of_arrival_order() {
        fn run(events: impl IntoIterator<Item = RawEvent>) -> Vec<Fill> {
            let mut ingester = Ingester::new(Registry::standard());
            for event in events {
                ingester.fill_caught(event);
            }
            ingester.ordered_fills()
        }

        let ascending = run([
            event("first", 1, "0.002"),
            event("second", 2, "0.002"),
            event("fourth", 4, "0.002"),
        ]);
        let descending = run([
            event("fourth", 4, "0.002"),
            event("second", 2, "0.002"),
            event("first", 1, "0.002"),
        ]);
        assert_eq!(ascending, descending);
        assert_eq!(ascending.len(), 3);
    }

    #[test]
    fn drives_ready_fills_into_the_position_store() {
        let mut ingester = Ingester::new(Registry::standard());
        ingester.fill_caught(event("first", 1, "0.002"));
        ingester.fill_caught(event("second", 2, "0.003"));
        let mut keeper = PositionKeeper::new(Registry::standard());

        let report = ingester.drive(&mut keeper);

        assert_eq!(report.applied, 2);
        assert!(report.apply_errors.is_empty());
        assert_eq!(
            keeper.positions().values().next().unwrap().net_qty,
            Lots(50)
        );
    }
}
