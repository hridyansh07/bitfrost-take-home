//! The Algo.md merger, made real: per-venue ring buffers, a vector of `recv_ts` frontiers, and a
//! single-writer `global_seq`.
//!
//! One deliberate refinement over the older scalar-watermark pseudocode (`head.recv_ts <=
//! min(frontier)`): a venue may still legally push an event AT its current frontier, so the emit
//! gate must be per-lane and asymmetric. A head stamped `t` is emittable once every
//! *higher-priority* lane's frontier is strictly past `t` (that lane could still produce a tie
//! at `t`, which would sort first) and every *lower-priority* lane's frontier is at least `t`
//! (its ties sort after the head anyway). The head's own lane never gates it — same-venue ties
//! are already FIFO-ordered by `venue_seq`. A plain strict global watermark would instead
//! deadlock the freshest venue behind its own frontier.
use std::collections::VecDeque;

use types::{Fill, SequencedFill, SequencerError, Venue};

/// Allocation hint only — lanes grow past this if a burst outruns it. A hard bound with
/// backpressure is an extension point not implemented here
const INITIAL_LANE_CAPACITY: usize = 1024;

#[derive(Debug)]
struct Lane {
    buffer: VecDeque<(u64, Fill)>,
    /// Latest stamp this venue is known to have reached
    frontier: u64,
    /// Producer-order guard for venue_seq.
    last_seq: Option<u64>,
}

impl Lane {
    fn new() -> Lane {
        Lane {
            buffer: VecDeque::with_capacity(INITIAL_LANE_CAPACITY),
            frontier: 0,
            last_seq: None,
        }
    }
}

pub struct Sequencer {
    lanes: [Lane; 2],
    next_global_seq: u64,
    /// Set by `flush`: the stream has ended, and the published tape must never grow
    /// afterward, so a sealed sequencer refuses further pushes.
    sealed: bool,
}

impl Default for Sequencer {
    fn default() -> Sequencer {
        Sequencer::new()
    }
}

impl Sequencer {
    pub fn new() -> Sequencer {
        Sequencer {
            lanes: [Lane::new(), Lane::new()],
            next_global_seq: 1,
            sealed: false,
        }
    }

    /// Stamp an in-order venue event onto its lane. O(1).
    pub fn push(&mut self, venue: Venue, recv_ts: u64, fill: Fill) -> Result<(), SequencerError> {
        if self.sealed {
            return Err(SequencerError::Sealed { venue });
        }
        if fill.venue != venue {
            return Err(SequencerError::VenueMismatch {
                lane: venue,
                fill: fill.venue,
            });
        }
        let lane = &mut self.lanes[venue as usize];
        // New Fill can never be before the current highest fill
        if recv_ts < lane.frontier {
            return Err(SequencerError::RecvTsWalkback {
                venue,
                frontier: lane.frontier,
                recv_ts,
            });
        }
        if let Some(last_seq) = lane.last_seq {
            // New Fill can never be behind the exchanges newest seq
            if fill.seq <= last_seq {
                return Err(SequencerError::SeqWalkback {
                    venue,
                    last_seq,
                    seq: fill.seq,
                });
            }
        }
        lane.last_seq = Some(fill.seq);
        lane.frontier = recv_ts;
        lane.buffer.push_back((recv_ts, fill));
        Ok(())
    }

    /// Idle venue advances its frontier on the `T_IDLE` heartbeat so a silent stream never
    /// freezes the merge.
    pub fn tick(&mut self, venue: Venue, now_ts: u64) {
        let lane = &mut self.lanes[venue as usize];
        lane.frontier = lane.frontier.max(now_ts);
    }

    /// Emit every event proven safe by the lane frontiers, in `SORT_KEY` order, assigning
    /// `global_seq`. O(K) per emitted event (head scan + frontier check), O(1) pop.
    pub fn drain_ready(&mut self) -> Vec<SequencedFill> {
        self.drain(true)
    }

    /// End-of-stream: drain everything regardless of frontiers and seal the sequencer (fixture
    /// streams are finite; a live shutdown would call this after quiescing the producers).
    pub fn flush(&mut self) -> Vec<SequencedFill> {
        self.sealed = true;
        self.drain(false)
    }

    /// Events currently buffered across all lanes.
    pub fn depth(&self) -> usize {
        self.lanes.iter().map(|lane| lane.buffer.len()).sum()
    }

    /// No event that could still arrive on another lane may sort before a head stamped
    /// `recv_ts`: higher-priority lanes must be strictly past it (an equal stamp there would
    /// win the tie), lower-priority lanes only need to have reached it.
    fn emittable(&self, lane_index: usize, recv_ts: u64) -> bool {
        self.lanes.iter().enumerate().all(|(other, lane)| {
            if other < lane_index {
                lane.frontier > recv_ts
            } else {
                other == lane_index || lane.frontier >= recv_ts
            }
        })
    }

    /// Ideally driven by whatever producers wants to consume the fills
    /// Opens up more optimizations and better reading API for downstream servers
    /// Intentionally sub-optimal here
    fn drain(&mut self, gated: bool) -> Vec<SequencedFill> {
        let mut out = Vec::new();
        loop {
            // O(K) head scan in lane (venue-priority) order; the strict `<` keeps the earlier
            // venue on recv_ts ties, and each lane is already in venue_seq order, so this
            // realizes SORT_KEY = (recv_ts, venue, venue_seq).
            let mut best: Option<(usize, u64)> = None;
            for (index, lane) in self.lanes.iter().enumerate() {
                if let Some((recv_ts, _)) = lane.buffer.front() {
                    if best.is_none_or(|(_, best_ts)| *recv_ts < best_ts) {
                        best = Some((index, *recv_ts));
                    }
                }
            }
            let Some((index, recv_ts)) = best else {
                break;
            };
            if gated && !self.emittable(index, recv_ts) {
                break;
            }
            let (recv_ts, fill) = self.lanes[index].buffer.pop_front().unwrap();
            let global_seq = self.next_global_seq;
            self.next_global_seq += 1;
            out.push(SequencedFill {
                global_seq,
                recv_ts,
                fill,
            });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{InstrumentId, Kind, Lots, Micro, Side, Ticks};

    fn fill(venue: Venue, seq: u64) -> Fill {
        let (symbol, kind) = match venue {
            Venue::Hl => ("BTC-PERP", Kind::Perp),
            Venue::Pm => ("FED-CUT-SEP", Kind::Binary),
        };
        Fill {
            venue,
            event_id: format!("{venue:?}-{seq}"),
            seq,
            ts_ms: 1,
            account: "acct".to_string(),
            instrument: InstrumentId(symbol.to_string()),
            symbol: symbol.to_string(),
            kind,
            side: Side::Buy,
            price: Ticks(100),
            qty: Lots(1),
            fee: Micro(0),
        }
    }

    fn ids(tape: &[SequencedFill]) -> Vec<&str> {
        tape.iter().map(|out| out.fill.event_id.as_str()).collect()
    }

    #[test]
    fn rejects_recv_ts_behind_the_frontier() {
        let mut sequencer = Sequencer::new();
        sequencer.push(Venue::Hl, 10, fill(Venue::Hl, 1)).unwrap();
        assert_eq!(
            sequencer.push(Venue::Hl, 5, fill(Venue::Hl, 2)),
            Err(SequencerError::RecvTsWalkback {
                venue: Venue::Hl,
                frontier: 10,
                recv_ts: 5,
            })
        );
        // Equal stamps are expected (clock granularity); only walk-backs are refused.
        sequencer.push(Venue::Hl, 10, fill(Venue::Hl, 2)).unwrap();
        // A tick also commits the frontier.
        sequencer.tick(Venue::Hl, 20);
        assert!(matches!(
            sequencer.push(Venue::Hl, 15, fill(Venue::Hl, 3)),
            Err(SequencerError::RecvTsWalkback { frontier: 20, .. })
        ));
    }

    #[test]
    fn rejects_venue_seq_walkback_but_tolerates_gaps() {
        let mut sequencer = Sequencer::new();
        sequencer.push(Venue::Hl, 10, fill(Venue::Hl, 5)).unwrap();
        assert_eq!(
            sequencer.push(Venue::Hl, 11, fill(Venue::Hl, 5)),
            Err(SequencerError::SeqWalkback {
                venue: Venue::Hl,
                last_seq: 5,
                seq: 5,
            })
        );
        assert!(matches!(
            sequencer.push(Venue::Hl, 12, fill(Venue::Hl, 4)),
            Err(SequencerError::SeqWalkback { .. })
        ));
        sequencer.push(Venue::Hl, 12, fill(Venue::Hl, 9)).unwrap();
    }

    #[test]
    fn rejects_fill_pushed_to_the_wrong_venue_lane() {
        let mut sequencer = Sequencer::new();
        assert_eq!(
            sequencer.push(Venue::Hl, 10, fill(Venue::Pm, 1)),
            Err(SequencerError::VenueMismatch {
                lane: Venue::Hl,
                fill: Venue::Pm,
            })
        );
        assert_eq!(sequencer.depth(), 0);
    }

    #[test]
    fn gates_a_head_until_no_earlier_arrival_can_form_elsewhere() {
        let mut sequencer = Sequencer::new();
        sequencer.push(Venue::Hl, 10, fill(Venue::Hl, 1)).unwrap();
        // Pm's frontier is still 0: it could yet deliver a stamp below 10.
        assert!(sequencer.drain_ready().is_empty());
        // Pm reaching 10 exactly frees the Hl head: a Pm tie at 10 sorts after Hl anyway.
        sequencer.tick(Venue::Pm, 10);
        assert_eq!(ids(&sequencer.drain_ready()), ["Hl-1"]);

        // The mirror direction is strict: an Hl tie at 10 would sort BEFORE a Pm head at 10,
        // so the Pm head waits until Hl's frontier strictly passes its stamp.
        sequencer.push(Venue::Pm, 10, fill(Venue::Pm, 1)).unwrap();
        assert!(sequencer.drain_ready().is_empty());
        sequencer.tick(Venue::Hl, 10); // stale tick — frontier already there
        assert!(sequencer.drain_ready().is_empty());
        sequencer.tick(Venue::Hl, 11);
        assert_eq!(ids(&sequencer.drain_ready()), ["Pm-1"]);
        assert_eq!(sequencer.depth(), 0);
    }

    #[test]
    fn cross_venue_ties_resolve_by_venue_priority_then_seq() {
        let mut sequencer = Sequencer::new();
        sequencer.push(Venue::Pm, 10, fill(Venue::Pm, 1)).unwrap();
        sequencer.push(Venue::Hl, 10, fill(Venue::Hl, 7)).unwrap();
        sequencer.tick(Venue::Hl, 11);
        sequencer.tick(Venue::Pm, 11);
        let tape = sequencer.drain_ready();
        assert_eq!(ids(&tape), ["Hl-7", "Pm-1"]);
        assert_eq!(tape[0].recv_ts, tape[1].recv_ts);
    }

    #[test]
    fn global_seq_is_contiguous_across_drains_and_recv_ts_never_walks_back() {
        let mut sequencer = Sequencer::new();
        let mut tape = Vec::new();
        sequencer.push(Venue::Hl, 1, fill(Venue::Hl, 1)).unwrap();
        sequencer.push(Venue::Pm, 2, fill(Venue::Pm, 10)).unwrap();
        tape.extend(sequencer.drain_ready());
        sequencer.push(Venue::Hl, 4, fill(Venue::Hl, 3)).unwrap();
        sequencer.push(Venue::Pm, 5, fill(Venue::Pm, 12)).unwrap();
        tape.extend(sequencer.drain_ready());
        tape.extend(sequencer.flush());

        assert_eq!(tape.len(), 4);
        // I3: global_seq is exactly 1..=n and recv_ts is non-decreasing.
        assert!(tape
            .iter()
            .zip(1u64..)
            .all(|(out, expected)| out.global_seq == expected));
        assert!(tape
            .windows(2)
            .all(|pair| pair[0].recv_ts <= pair[1].recv_ts));
        // I1: each venue's fills appear in venue_seq order.
        for venue in [Venue::Hl, Venue::Pm] {
            let seqs: Vec<u64> = tape
                .iter()
                .filter(|out| out.fill.venue == venue)
                .map(|out| out.fill.seq)
                .collect();
            assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]));
        }
    }

    #[test]
    fn output_is_a_pure_function_of_the_stamped_pushes() {
        let stamped = [
            (Venue::Hl, 1, 1),
            (Venue::Pm, 1, 10),
            (Venue::Hl, 3, 2),
            (Venue::Pm, 4, 11),
            (Venue::Hl, 4, 5),
        ];

        // Interleaved pushes with eager drains…
        let mut eager = Sequencer::new();
        let mut eager_tape = Vec::new();
        for (venue, recv_ts, seq) in stamped {
            eager.push(venue, recv_ts, fill(venue, seq)).unwrap();
            eager_tape.extend(eager.drain_ready());
        }
        eager_tape.extend(eager.flush());

        // …versus venue-major pushes and a single flush: the tape depends only on the stamps.
        let mut lazy = Sequencer::new();
        for wanted in [Venue::Hl, Venue::Pm] {
            for (venue, recv_ts, seq) in stamped {
                if venue == wanted {
                    lazy.push(venue, recv_ts, fill(venue, seq)).unwrap();
                }
            }
        }
        assert_eq!(lazy.flush(), eager_tape);
        assert_eq!(eager_tape.len(), stamped.len());
    }

    #[test]
    fn rejects_pushes_after_flush_seals_the_stream() {
        let mut sequencer = Sequencer::new();
        sequencer.push(Venue::Pm, 10, fill(Venue::Pm, 1)).unwrap();
        assert_eq!(sequencer.flush().len(), 1);
        // A post-flush push at an earlier stamp would put a walked-back recv_ts on the
        // published tape (I3/I5) — the sealed sequencer refuses it outright.
        assert_eq!(
            sequencer.push(Venue::Hl, 3, fill(Venue::Hl, 1)),
            Err(SequencerError::Sealed { venue: Venue::Hl })
        );
        assert!(sequencer.drain_ready().is_empty());
    }

    #[test]
    fn flush_drains_everything_in_sort_key_order() {
        let mut sequencer = Sequencer::new();
        sequencer.push(Venue::Hl, 5, fill(Venue::Hl, 1)).unwrap();
        sequencer.push(Venue::Hl, 6, fill(Venue::Hl, 2)).unwrap();
        sequencer.push(Venue::Pm, 5, fill(Venue::Pm, 1)).unwrap();
        assert_eq!(sequencer.depth(), 3);
        // No ticks, so nothing is watermark-safe — flush ends the stream instead.
        let tape = sequencer.flush();
        assert_eq!(ids(&tape), ["Hl-1", "Pm-1", "Hl-2"]);
        assert_eq!(sequencer.depth(), 0);
    }
}
