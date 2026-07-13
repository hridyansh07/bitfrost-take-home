//! The Algo.md ingest task: the caller stamps `recv_ts` the instant a raw message is received,
//! then `fill_caught` canonicalizes, dedups, and pushes the surviving fill straight onto the
//! sequencer's lane.

mod sequencer;

use std::collections::BTreeMap;

use types::{
    Alert, AlertKind, Canonicalize, DeadLetter, DeadLetterReason, Fill, IngestOutcome, IngestStats,
    InstrumentSource, RawEvent, RawLine, SequencedFill, Venue,
};

pub use sequencer::Sequencer;

/// The first observed copy for an event id. Retransmits of that copy dedup; different later
/// copies intentionally raise fresh byzantine alerts while the first correct copy remains canonical.
struct Seen {
    event: RawEvent,
    /// `Some` when the copy canonicalized cleanly; `None` for rejected payloads.
    fill: Option<Fill>,
}

enum Identity {
    New,
    Retransmit,
    Conflict,
}

pub struct Ingester<S> {
    instruments: S,
    sequencer: Sequencer,
    seen: BTreeMap<(Venue, String), Seen>,
    max_seq: BTreeMap<Venue, u64>,
    dead_letters: Vec<DeadLetter>,
    alerts: Vec<Alert>,
    stats: IngestStats,
}

impl<S: InstrumentSource> Ingester<S> {
    pub fn new(instruments: S) -> Self {
        Self {
            instruments,
            sequencer: Sequencer::new(),
            seen: BTreeMap::new(),
            max_seq: BTreeMap::new(),
            dead_letters: Vec::new(),
            alerts: Vec::new(),
            stats: IngestStats::default(),
        }
    }

    /// The ingest task body. `recv_ts` is stamped by the caller at receive time (the harness
    /// stamps the arrival `Instant`; fixture tests use a logical clock) — everything after the
    /// stamp is deterministic, so the emitted tape is a pure function of the stamped arrivals.
    pub fn fill_caught(&mut self, recv_ts: u64, line: RawLine) -> IngestOutcome {
        let RawLine {
            venue: line_venue,
            text,
            parsed,
            ..
        } = line;
        let event = match parsed {
            Ok(event) => event,
            Err(error) => {
                // Bad arrivals signal movement on venue still advance ticks
                self.sequencer.tick(line_venue, recv_ts);
                let reason = DeadLetterReason::Malformed(error);
                return self.dead_letter(line_venue, String::new(), reason, text);
            }
        };
        // The payload owns its venue; the transport wrapper's label is not trusted for
        // dedup keys or lane selection.
        let venue = event.venue();
        self.sequencer.tick(venue, recv_ts);
        let event_id = event.event_id().to_string();
        let fill = event.clone().canonicalize(&self.instruments);
        let key = (venue, event_id.clone());

        // Identity is checked BEFORE rejection, so a same-id conflict can neither hide
        // behind a canonicalization failure nor impersonate a previously rejected id.
        let identity = match self.seen.get(&key) {
            None => Identity::New,
            Some(previous) => {
                let same = match (&previous.fill, &fill) {
                    // Both copies are valid: compare canonical fills ideally built using
                    // Equal traits on cannonical structs not a reference equality
                    (Some(previous_fill), Ok(fill)) => previous_fill == fill,
                    // Either copy is invalid: fall back to raw payload identity.
                    _ => previous.event == event,
                };
                if same {
                    Identity::Retransmit
                } else {
                    Identity::Conflict
                }
            }
        };

        match identity {
            Identity::Retransmit => {
                self.stats.duplicates += 1;
                IngestOutcome::Duplicate
            }
            Identity::Conflict => {
                // The first observed copy is treated as canonical
                self.alerts.push(Alert {
                    venue,
                    event_id: event_id.clone(),
                    kind: AlertKind::ByzantineDuplicate,
                });
                self.stats.byzantine += 1;
                self.dead_letter(venue, event_id, DeadLetterReason::Byzantine, text)
            }
            Identity::New => match fill {
                Err(reason) => {
                    self.seen.insert(key, Seen { event, fill: None });
                    self.dead_letter(venue, event_id, DeadLetterReason::Rejected(reason), text)
                }
                Ok(fill) => match self.sequencer.push(venue, recv_ts, fill.clone()) {
                    Ok(()) => {
                        if let Some(last) = self.max_seq.get(&venue) {
                            self.stats.gaps += (fill.seq - last - 1) as usize;
                        }
                        self.max_seq.insert(venue, fill.seq);
                        self.seen.insert(
                            key,
                            Seen {
                                event,
                                fill: Some(fill),
                            },
                        );
                        self.stats.accepted += 1;
                        IngestOutcome::Accepted
                    }
                    Err(error) => {
                        // In-order delivery is a producer guarantee; the violation is
                        // quarantined, and remembered so retransmits dedup instead of
                        // double-counting.
                        self.seen.insert(
                            key,
                            Seen {
                                event,
                                fill: Some(fill),
                            },
                        );
                        self.stats.out_of_order += 1;
                        self.dead_letter(venue, event_id, DeadLetterReason::OutOfOrder(error), text)
                    }
                },
            },
        }
    }

    /// Idle venue advances its frontier so it never stalls other lanes
    pub fn tick(&mut self, venue: Venue, now_ts: u64) {
        self.sequencer.tick(venue, now_ts);
    }

    /// Canonical tape entries proven safe by the lane frontiers, in global order.
    pub fn drain_ready(&mut self) -> Vec<SequencedFill> {
        self.sequencer.drain_ready()
    }

    /// End-of-stream: drain the remaining buffered fills completely
    pub fn flush(&mut self) -> Vec<SequencedFill> {
        self.sequencer.flush()
    }

    pub fn sequencer(&self) -> &Sequencer {
        &self.sequencer
    }

    pub fn dead_letters(&self) -> &[DeadLetter] {
        &self.dead_letters
    }

    pub fn alerts(&self) -> &[Alert] {
        &self.alerts
    }

    pub fn stats(&self) -> IngestStats {
        self.stats
    }

    fn dead_letter(
        &mut self,
        venue: Venue,
        event_id: String,
        reason: DeadLetterReason,
        text: String,
    ) -> IngestOutcome {
        self.stats.dead_lettered += 1;
        let outcome = IngestOutcome::DeadLettered(reason.clone());
        self.dead_letters.push(DeadLetter {
            venue,
            event_id,
            reason,
            text: Some(text),
        });
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{HlEvent, Lots, PmEvent, RawEvent, Registry, RejectReason, SequencerError};

    fn line(event: RawEvent) -> RawLine {
        RawLine {
            venue: event.venue(),
            text: "raw".to_string(),
            parsed: Ok(event),
        }
    }

    fn event(event_id: &str, seq: u64, qty: &str) -> RawLine {
        line(RawEvent::Hl(HlEvent {
            seq,
            event_id: event_id.to_string(),
            ts: 1,
            account: "account".to_string(),
            symbol: "BTC-PERP".to_string(),
            side: "buy".to_string(),
            px: "100.0".to_string(),
            qty: qty.to_string(),
            fee: "0.0".to_string(),
        }))
    }

    fn pm_event(event_id: &str, seq: u64) -> RawLine {
        line(RawEvent::Pm(PmEvent {
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
        }))
    }

    #[test]
    fn applies_exactly_once_by_venue_and_event_id() {
        let mut ingester = Ingester::new(Registry::standard());
        assert_eq!(
            ingester.fill_caught(1, event("event", 1, "0.002")),
            IngestOutcome::Accepted
        );
        assert_eq!(
            ingester.fill_caught(2, event("event", 1, "0.002")),
            IngestOutcome::Duplicate
        );
        assert_eq!(ingester.stats().accepted, 1);
        assert_eq!(ingester.stats().duplicates, 1);
        assert_eq!(ingester.flush().len(), 1);
    }

    #[test]
    fn quarantines_byzantine_duplicates_and_keeps_the_first_accepted_copy() {
        fn run(first: &str, second: &str) -> (Vec<SequencedFill>, Lots) {
            let mut ingester = Ingester::new(Registry::standard());
            assert_eq!(
                ingester.fill_caught(1, event("event", 1, first)),
                IngestOutcome::Accepted
            );
            assert_eq!(
                ingester.fill_caught(2, event("event", 1, second)),
                IngestOutcome::DeadLettered(DeadLetterReason::Byzantine)
            );
            assert_eq!(ingester.alerts().len(), 1);
            assert_eq!(ingester.dead_letters().len(), 1);
            let tape = ingester.flush();
            let qty = tape[0].fill.qty;
            (tape, qty)
        }

        // The tape is immutable (I5): whichever payload was accepted first stays on it, and
        // the conflicting copy is dead-lettered — deterministic per stamped arrival tape.
        assert_eq!(run("0.002", "0.004").1, Lots(20));
        assert_eq!(run("0.004", "0.002").1, Lots(40));
    }

    #[test]
    fn repeated_byzantine_conflicts_raise_repeated_alerts() {
        let mut ingester = Ingester::new(Registry::standard());
        assert_eq!(
            ingester.fill_caught(1, event("event", 1, "0.002")),
            IngestOutcome::Accepted
        );
        for recv_ts in [2, 3] {
            assert_eq!(
                ingester.fill_caught(recv_ts, event("event", 1, "0.004")),
                IngestOutcome::DeadLettered(DeadLetterReason::Byzantine)
            );
        }

        assert_eq!(ingester.alerts().len(), 2);
        assert_eq!(ingester.dead_letters().len(), 2);
        assert_eq!(ingester.stats().byzantine, 2);
        let tape = ingester.flush();
        assert_eq!(tape.len(), 1);
        assert_eq!(tape[0].fill.qty, Lots(20));
    }

    #[test]
    fn rejects_unknown_instrument_into_dead_letters() {
        let mut unknown = event("unknown", 1, "0.002");
        let Ok(RawEvent::Hl(raw)) = &mut unknown.parsed else {
            unreachable!()
        };
        raw.symbol = "UNKNOWN".to_string();
        let mut ingester = Ingester::new(Registry::standard());
        assert!(matches!(
            ingester.fill_caught(1, unknown),
            IngestOutcome::DeadLettered(DeadLetterReason::Rejected(
                RejectReason::UnknownSymbol(symbol)
            )) if symbol == "UNKNOWN"
        ));
        assert!(ingester.flush().is_empty());
        assert_eq!(ingester.dead_letters().len(), 1);
        assert_eq!(ingester.dead_letters()[0].text.as_deref(), Some("raw"));
    }

    #[test]
    fn preserves_malformed_line_text() {
        let mut ingester = Ingester::new(Registry::standard());
        let outcome = ingester.fill_caught(
            1,
            RawLine {
                venue: Venue::Hl,
                text: "not-json".to_string(),
                parsed: Err("expected value".to_string()),
            },
        );
        assert_eq!(
            outcome,
            IngestOutcome::DeadLettered(DeadLetterReason::Malformed("expected value".to_string()))
        );
        assert_eq!(ingester.dead_letters()[0].text.as_deref(), Some("not-json"));
    }

    #[test]
    fn quarantines_out_of_order_producers_and_counts_gaps() {
        let mut ingester = Ingester::new(Registry::standard());
        assert_eq!(
            ingester.fill_caught(1, event("first", 1, "0.002")),
            IngestOutcome::Accepted
        );
        assert_eq!(
            ingester.fill_caught(2, event("third", 3, "0.002")),
            IngestOutcome::Accepted
        );
        assert_eq!(
            ingester.fill_caught(3, event("seventh", 7, "0.002")),
            IngestOutcome::Accepted
        );
        // A walk-back violates the in-order producer assumption → quarantined, not applied.
        assert!(matches!(
            ingester.fill_caught(4, event("stale", 2, "0.002")),
            IngestOutcome::DeadLettered(DeadLetterReason::OutOfOrder(
                SequencerError::SeqWalkback {
                    last_seq: 7,
                    seq: 2,
                    ..
                }
            ))
        ));
        assert_eq!(ingester.stats().out_of_order, 1);
        assert_eq!(ingester.stats().gaps, 4); // (1→3) skips one, (3→7) skips three
        assert_eq!(ingester.stats().accepted, 3);
        assert_eq!(ingester.flush().len(), 3);
    }

    #[test]
    fn tape_orders_by_stamp_then_venue_priority() {
        let mut ingester = Ingester::new(Registry::standard());
        ingester.fill_caught(1, pm_event("pm-first", 10));
        ingester.fill_caught(1, event("hl-first", 1, "0.002"));
        ingester.fill_caught(2, event("hl-later", 3, "0.002"));
        ingester.fill_caught(3, pm_event("pm-second", 11));

        let tape = ingester.flush();
        assert_eq!(
            tape.iter()
                .map(|out| out.fill.event_id.as_str())
                .collect::<Vec<_>>(),
            // Equal stamps resolve by venue priority (Hl < Pm); the rest by recv_ts.
            ["hl-first", "pm-first", "hl-later", "pm-second"]
        );
        assert_eq!(
            tape.iter().map(|out| out.global_seq).collect::<Vec<_>>(),
            [1, 2, 3, 4]
        );
    }

    #[test]
    fn drains_incrementally_as_frontiers_advance() {
        let mut ingester = Ingester::new(Registry::standard());
        ingester.fill_caught(10, event("hl-1", 1, "0.002"));
        assert!(ingester.drain_ready().is_empty());
        // Pm reaching the stamp frees the higher-priority Hl head.
        ingester.tick(Venue::Pm, 10);
        assert_eq!(ingester.drain_ready().len(), 1);
        assert_eq!(ingester.sequencer().depth(), 0);
    }

    #[test]
    fn flags_byzantine_when_the_conflicting_copy_is_invalid() {
        let mut ingester = Ingester::new(Registry::standard());
        assert_eq!(
            ingester.fill_caught(1, event("event", 1, "0.002")),
            IngestOutcome::Accepted
        );
        // Same id comes back with an off-tick price: the conflict must be flagged as
        // byzantine, not laundered into a plain rejection.
        let mut off_tick = event("event", 1, "0.002");
        let Ok(RawEvent::Hl(raw)) = &mut off_tick.parsed else {
            unreachable!()
        };
        raw.px = "100.3".to_string();
        assert_eq!(
            ingester.fill_caught(2, off_tick),
            IngestOutcome::DeadLettered(DeadLetterReason::Byzantine)
        );
        assert_eq!(ingester.alerts().len(), 1);
        assert_eq!(ingester.stats().byzantine, 1);
        // The valid first copy is still the one on the tape.
        assert_eq!(ingester.flush().len(), 1);
    }

    #[test]
    fn flags_byzantine_when_the_first_copy_was_rejected() {
        let mut ingester = Ingester::new(Registry::standard());
        let mut poison = event("event", 1, "0.002");
        let Ok(RawEvent::Hl(raw)) = &mut poison.parsed else {
            unreachable!()
        };
        raw.symbol = "UNKNOWN".to_string();
        assert!(matches!(
            ingester.fill_caught(1, poison),
            IngestOutcome::DeadLettered(DeadLetterReason::Rejected(_))
        ));
        // A venue re-issuing the id with valid-but-different data must not slip in silently.
        assert_eq!(
            ingester.fill_caught(2, event("event", 1, "0.002")),
            IngestOutcome::DeadLettered(DeadLetterReason::Byzantine)
        );
        assert_eq!(ingester.alerts().len(), 1);
        assert!(ingester.flush().is_empty());
    }

    #[test]
    fn dedups_retransmits_of_rejected_and_quarantined_events() {
        let mut ingester = Ingester::new(Registry::standard());
        ingester.fill_caught(1, event("first", 5, "0.002"));
        // Quarantined for walking the sequence back…
        assert!(matches!(
            ingester.fill_caught(2, event("stale", 3, "0.002")),
            IngestOutcome::DeadLettered(DeadLetterReason::OutOfOrder(_))
        ));
        // …so an identical retransmit is a duplicate, not a second dead letter.
        assert_eq!(
            ingester.fill_caught(3, event("stale", 3, "0.002")),
            IngestOutcome::Duplicate
        );
        assert_eq!(ingester.dead_letters().len(), 1);
        assert_eq!(ingester.stats().out_of_order, 1);
        assert_eq!(ingester.stats().duplicates, 1);
    }

    #[test]
    fn derives_venue_from_the_parsed_event_not_the_wrapper() {
        let mut ingester = Ingester::new(Registry::standard());
        let mut mislabeled = pm_event("pm-event", 10);
        mislabeled.venue = Venue::Hl; // the transport wrapper lies
        assert_eq!(ingester.fill_caught(1, mislabeled), IngestOutcome::Accepted);
        let tape = ingester.flush();
        assert_eq!(tape[0].fill.venue, Venue::Pm);
    }

    #[test]
    fn bad_arrivals_still_advance_the_frontier() {
        let mut ingester = Ingester::new(Registry::standard());
        ingester.fill_caught(10, event("hl-1", 1, "0.002"));
        assert!(ingester.drain_ready().is_empty()); // Pm frontier is still 0
                                                    // A poison Pm arrival is still proof that Pm has reached ts 10 — it must free the
                                                    // buffered Hl head rather than stall it until flush.
        let mut poison = pm_event("pm-poison", 7);
        let Ok(RawEvent::Pm(raw)) = &mut poison.parsed else {
            unreachable!()
        };
        raw.market = "UNKNOWN".to_string();
        assert!(matches!(
            ingester.fill_caught(10, poison),
            IngestOutcome::DeadLettered(DeadLetterReason::Rejected(_))
        ));
        assert_eq!(ingester.drain_ready().len(), 1);
    }
}
