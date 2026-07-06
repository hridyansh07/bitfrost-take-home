use std::path::{Path, PathBuf};

use ingester::{DeadLetterReason, Ingester};
use position_keeper::PositionKeeper;
use types::{merge, read_hl, read_pm, shuffle, RawLine, Registry};

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

fn ingest(lines: Vec<RawLine>) -> Ingester<Registry> {
    let mut ingester = Ingester::new(Registry::standard());
    for line in lines {
        ingester.ingest_line(line);
    }
    ingester
}

fn rejected(ingester: &Ingester<Registry>, event_id: &str) -> bool {
    ingester.dead_letters().iter().any(|dead_letter| {
        dead_letter.event_id == event_id
            && matches!(dead_letter.reason, DeadLetterReason::Rejected(_))
    })
}

fn byzantine(ingester: &Ingester<Registry>, event_id: &str) -> bool {
    ingester.dead_letters().iter().any(|dead_letter| {
        dead_letter.event_id == event_id && dead_letter.reason == DeadLetterReason::Byzantine
    })
}

#[test]
fn drives_full_fixtures_and_is_arrival_order_independent() {
    let dir = fixtures_dir();
    let hl = read_hl(&dir.join("hl.ndjson")).unwrap();
    let pm = read_pm(&dir.join("pm.ndjson")).unwrap();
    let merged = merge(hl, pm);

    let ingester = ingest(merged.clone());

    assert_eq!(ingester.ordered_fills().len(), ingester.stats().accepted);
    assert!(
        ingester.stats().accepted > 900,
        "accepted = {}",
        ingester.stats().accepted
    );

    assert!(rejected(&ingester, "hl-poison-unknown"));
    assert!(rejected(&ingester, "hl-poison-offtick"));
    assert!(rejected(&ingester, "pm-poison-unknown"));
    assert!(rejected(&ingester, "pm-poison-range"));
    assert!(byzantine(&ingester, "hl-dup-byz"));
    assert!(byzantine(&ingester, "pm-dup-byz"));
    assert_eq!(ingester.alerts().len(), 2);

    let mut keeper = PositionKeeper::new(Registry::standard());
    let report = ingester.drive(&mut keeper);
    assert!(report.apply_errors.is_empty());
    assert_eq!(report.applied, ingester.ordered_fills().len());
    let baseline = keeper.into_positions();

    for seed in [7u64, 99, 2024] {
        let mut shuffled = merged.clone();
        shuffle(&mut shuffled, seed);
        let replayed = ingest(shuffled);
        let mut keeper = PositionKeeper::new(Registry::standard());
        replayed.drive(&mut keeper);
        assert_eq!(keeper.into_positions(), baseline);
    }
}
