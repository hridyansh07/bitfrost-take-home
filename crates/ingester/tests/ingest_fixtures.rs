use std::path::{Path, PathBuf};

use ingester::{AlertKind, DeadLetterReason, Ingester};
use types::{read_hl, read_pm, Registry, RejectReason};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

#[test]
fn fixtures_route_poison_and_byzantine_events() {
    let fixture_dir = fixture_dir();
    let mut lines = read_hl(&fixture_dir.join("hl.ndjson")).unwrap();
    lines.extend(read_pm(&fixture_dir.join("pm.ndjson")).unwrap());
    let mut ingester = Ingester::new(Registry::standard());
    for line in lines {
        ingester.ingest_line(line);
    }

    assert_eq!(ingester.alerts().len(), 2);
    assert!(ingester
        .alerts()
        .iter()
        .all(|alert| alert.kind == AlertKind::ByzantineDuplicate));
    assert!(ingester
        .alerts()
        .iter()
        .any(|alert| alert.event_id == "hl-dup-byz"));
    assert!(ingester
        .alerts()
        .iter()
        .any(|alert| alert.event_id == "pm-dup-byz"));

    assert_eq!(ingester.dead_letters().len(), 6);
    assert!(ingester
        .dead_letters()
        .iter()
        .all(|letter| letter.text.as_ref().is_some_and(|text| !text.is_empty())));
    assert!(ingester.dead_letters().iter().any(|letter| {
        letter.event_id == "hl-poison-unknown"
            && matches!(
                letter.reason,
                DeadLetterReason::Rejected(RejectReason::UnknownSymbol(_))
            )
    }));
    assert!(ingester.dead_letters().iter().any(|letter| {
        letter.event_id == "hl-poison-offtick"
            && matches!(
                letter.reason,
                DeadLetterReason::Rejected(RejectReason::OffTick { .. })
            )
    }));
    assert!(ingester.dead_letters().iter().any(|letter| {
        letter.event_id == "pm-poison-unknown"
            && matches!(
                letter.reason,
                DeadLetterReason::Rejected(RejectReason::UnknownSymbol(_))
            )
    }));
    assert!(ingester.dead_letters().iter().any(|letter| {
        letter.event_id == "pm-poison-range"
            && matches!(
                letter.reason,
                DeadLetterReason::Rejected(RejectReason::PriceOutOfRange { .. })
            )
    }));
    assert_eq!(ingester.stats().byzantine, 2);
    assert_eq!(ingester.stats().dead_lettered, 6);
    assert!(ingester.stats().duplicates > 0);
}
