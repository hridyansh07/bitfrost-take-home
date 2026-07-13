//! Shared harness for the fixture-driven integration tests: per-venue streams are fed in
//! venue_seq order (the in-order producer assumption), stamped with a logical receive clock,
//! and pulled through the ingester's sequencer incrementally.
#![allow(dead_code)]

use std::collections::BTreeMap;

use ingester::Ingester;
use position_keeper::PositionKeeper;
use types::{Position, PositionKey, RawEvent, RawLine, Registry, SequencedFill};

/// Producers deliver in venue_seq order (unparsable lines carry no seq and stream last).
pub fn sorted_by_seq(mut lines: Vec<RawLine>) -> Vec<RawLine> {
    lines.sort_by_key(|line| line.parsed.as_ref().map(RawEvent::seq).unwrap_or(u64::MAX));
    lines
}

/// Seeded cross-venue interleave that PRESERVES each stream's internal order — only the merge
/// pattern (which venue speaks next) varies with the seed, exactly like arrival timing would.
pub fn riffle(hl: Vec<RawLine>, pm: Vec<RawLine>, seed: u64) -> Vec<RawLine> {
    let mut state = seed;
    let mut hl = hl.into_iter().peekable();
    let mut pm = pm.into_iter().peekable();
    let mut out = Vec::new();
    loop {
        let take_hl = match (hl.peek(), pm.peek()) {
            (None, None) => break,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (Some(_), Some(_)) => splitmix64(&mut state) & 1 == 0,
        };
        out.push(if take_hl { hl.next() } else { pm.next() }.unwrap());
    }
    out
}

/// Stamp arrivals with a logical receive clock (1, 2, 3, …) and pull the tape through the
/// pipeline with incremental drains plus an end-of-stream flush.
pub fn run_pipeline(lines: Vec<RawLine>) -> (Ingester<Registry>, Vec<SequencedFill>) {
    let mut ingester = Ingester::new(Registry::standard());
    let mut tape = Vec::new();
    for (index, line) in lines.into_iter().enumerate() {
        ingester.fill_caught(index as u64 + 1, line);
        tape.extend(ingester.drain_ready());
    }
    tape.extend(ingester.flush());
    (ingester, tape)
}

/// Apply the canonical tape to a fresh keeper; fixture tapes must apply cleanly.
pub fn apply(tape: &[SequencedFill]) -> BTreeMap<PositionKey, Position> {
    let mut keeper = PositionKeeper::new(Registry::standard());
    for sequenced in tape {
        keeper
            .apply(&sequenced.fill)
            .unwrap_or_else(|reason| panic!("{}: {reason:?}", sequenced.fill.event_id));
    }
    keeper.into_positions()
}

pub fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}
