//! The fixture-driven integration suite: one shared harness, one fixture load, focused
//! scenarios. The fixture streams (~500 events per venue with planted dups, byzantine copies,
//! poison, gaps, and in-venue disorder) are the shared input; each test slices one guarantee.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use common::{apply, riffle, run_pipeline, sorted_by_seq};
use ingester::Ingester;
use serde_json::Value;
use types::{read_hl, read_pm, AlertKind, DeadLetterReason, RawLine, Registry, RejectReason};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

struct Fixture {
    hl: Vec<RawLine>,
    pm: Vec<RawLine>,
    manifest: Value,
}

/// Loaded and seq-sorted once per test binary; every test clones the streams it needs.
static FIXTURE: LazyLock<Fixture> = LazyLock::new(|| {
    let dir = fixture_dir();
    Fixture {
        hl: sorted_by_seq(read_hl(&dir.join("hl.ndjson")).unwrap()),
        pm: sorted_by_seq(read_pm(&dir.join("pm.ndjson")).unwrap()),
        manifest: serde_json::from_str(&fs::read_to_string(dir.join("manifest.json")).unwrap())
            .unwrap(),
    }
});

/// The checked-in fixtures must be exactly what the generator produces — regenerate with the
/// time anchor recorded in the manifest and compare byte-for-byte, so fixtures and code can
/// never silently drift.
#[test]
fn generator_reproduces_the_checked_in_fixtures() {
    let temp = std::env::temp_dir().join(format!("bitfrost-fixture-drift-{}", std::process::id()));
    let base_ts = FIXTURE.manifest["base_ts"].as_u64().unwrap();
    types::fixtures::generate_at(&temp, base_ts).unwrap();
    for file in types::fixtures::FILES {
        let generated = fs::read(temp.join(file)).unwrap();
        let checked_in = fs::read(fixture_dir().join(file)).unwrap();
        assert_eq!(
            generated, checked_in,
            "fixtures/{file} drifted from the generator"
        );
    }
    fs::remove_dir_all(temp).unwrap();
}

#[test]
fn routes_poison_byzantine_and_duplicates_with_raw_text_preserved() {
    let (ingester, tape) = run_pipeline(riffle(FIXTURE.hl.clone(), FIXTURE.pm.clone(), 7));

    assert_eq!(tape.len(), ingester.stats().accepted);
    assert!(
        ingester.stats().accepted > 900,
        "accepted = {}",
        ingester.stats().accepted
    );
    assert_eq!(ingester.stats().out_of_order, 0);
    assert!(ingester.stats().duplicates > 0);
    assert!(ingester.stats().gaps > 0);

    assert!(rejected(&ingester, "hl-poison-unknown", |reason| {
        matches!(reason, RejectReason::UnknownSymbol(_))
    }));
    assert!(rejected(&ingester, "hl-poison-offtick", |reason| {
        matches!(reason, RejectReason::OffTick { .. })
    }));
    assert!(rejected(&ingester, "pm-poison-unknown", |reason| {
        matches!(reason, RejectReason::UnknownSymbol(_))
    }));
    assert!(rejected(&ingester, "pm-poison-range", |reason| {
        matches!(reason, RejectReason::PriceOutOfRange { .. })
    }));

    assert_eq!(ingester.stats().byzantine, 2);
    assert_eq!(ingester.alerts().len(), 2);
    for id in ["hl-dup-byz", "pm-dup-byz"] {
        assert!(ingester
            .alerts()
            .iter()
            .any(|alert| alert.event_id == id && alert.kind == AlertKind::ByzantineDuplicate));
        assert!(ingester.dead_letters().iter().any(|letter| {
            letter.event_id == id && letter.reason == DeadLetterReason::Byzantine
        }));
    }

    assert_eq!(ingester.dead_letters().len(), 6);
    assert_eq!(ingester.stats().dead_lettered, 6);
    assert!(ingester
        .dead_letters()
        .iter()
        .all(|letter| letter.text.as_ref().is_some_and(|text| !text.is_empty())));
}

/// I2/I5: the canonical tape — global_seq and recv_ts included — is a pure function of the
/// stamped arrival tape.
#[test]
fn replays_identical_arrivals_to_an_identical_tape() {
    let arrivals = riffle(FIXTURE.hl.clone(), FIXTURE.pm.clone(), 7);
    let (_, tape) = run_pipeline(arrivals.clone());
    let (_, replayed) = run_pipeline(arrivals);
    assert_eq!(replayed, tape);
}

/// Different cross-venue interleavings are different tapes (arrival IS the frame of
/// reference), but per-venue order is preserved (I1) and instruments are venue-disjoint, so
/// the final positions must be identical.
#[test]
fn positions_are_independent_of_cross_venue_interleave() {
    let (_, tape) = run_pipeline(riffle(FIXTURE.hl.clone(), FIXTURE.pm.clone(), 7));
    let expected = apply(&tape);
    for seed in [99u64, 2024] {
        let (_, other) = run_pipeline(riffle(FIXTURE.hl.clone(), FIXTURE.pm.clone(), seed));
        assert_eq!(other.len(), tape.len());
        assert_eq!(apply(&other), expected);
    }
}

/// The generator plants in-venue disorder (every 41st/43rd event swapped). The other tests
/// normalize to producer order per the in-order assumption; this one feeds the raw file
/// order and expects every walk-back to be quarantined loudly, never applied.
#[test]
fn raw_fixture_order_quarantines_planted_disorder() {
    let raw_hl = read_hl(&fixture_dir().join("hl.ndjson")).unwrap(); // deliberately NOT sorted
    let (ingester, tape) = run_pipeline(raw_hl);
    assert!(ingester.stats().out_of_order > 0);
    assert!(ingester
        .dead_letters()
        .iter()
        .any(|letter| matches!(letter.reason, DeadLetterReason::OutOfOrder(_))));
    assert_eq!(tape.len(), ingester.stats().accepted);
}

/// Every buy in matched.ndjson has an equal-size sell at the same price across two accounts,
/// so realized PnL must conserve to zero and fees to the manifest's recorded sum.
#[test]
fn matched_stream_conserves_realized_pnl_and_fees() {
    let lines = sorted_by_seq(read_hl(&fixture_dir().join("matched.ndjson")).unwrap());
    let (_, tape) = run_pipeline(lines);
    let positions = apply(&tape);

    let manifest = &FIXTURE.manifest["matched"];
    assert_eq!(tape.len(), manifest["lines"].as_u64().unwrap() as usize);
    assert_eq!(
        positions
            .values()
            .map(|position| position.realized_pnl.0)
            .sum::<i128>(),
        manifest["expected_sum_realized_pnl_micro"]
            .as_i64()
            .unwrap() as i128
    );
    assert_eq!(
        positions
            .values()
            .map(|position| position.fees.0)
            .sum::<i128>(),
        manifest["sum_fees_micro"].as_i64().unwrap() as i128
    );
}

fn rejected(
    ingester: &Ingester<Registry>,
    event_id: &str,
    matches: impl Fn(&RejectReason) -> bool,
) -> bool {
    ingester.dead_letters().iter().any(|letter| {
        letter.event_id == event_id
            && match &letter.reason {
                DeadLetterReason::Rejected(reason) => matches(reason),
                _ => false,
            }
    })
}
