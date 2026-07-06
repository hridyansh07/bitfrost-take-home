use std::fs;
use std::path::{Path, PathBuf};

use ingester::Ingester;
use position_keeper::PositionKeeper;
use serde_json::Value;
use types::{read_hl, Registry};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

#[test]
fn matched_fixture_conserves_realized_pnl_and_fees() {
    let fixture_dir = fixture_dir();
    let manifest: Value =
        serde_json::from_str(&fs::read_to_string(fixture_dir.join("manifest.json")).unwrap())
            .unwrap();
    let lines = read_hl(&fixture_dir.join("matched.ndjson")).unwrap();
    let mut ingester = Ingester::new(Registry::standard());
    for line in lines {
        ingester.ingest_line(line);
    }
    let mut keeper = PositionKeeper::new(Registry::standard());

    let report = ingester.drive(&mut keeper);
    let realized_pnl = keeper
        .positions()
        .values()
        .map(|position| position.realized_pnl.0)
        .sum::<i128>();
    let fees = keeper
        .positions()
        .values()
        .map(|position| position.fees.0)
        .sum::<i128>();

    assert_eq!(
        report.applied,
        manifest["matched"]["lines"].as_u64().unwrap() as usize
    );
    assert!(report.apply_errors.is_empty());
    assert_eq!(
        realized_pnl,
        manifest["matched"]["expected_sum_realized_pnl_micro"]
            .as_i64()
            .unwrap() as i128
    );
    assert_eq!(
        fees,
        manifest["matched"]["sum_fees_micro"].as_i64().unwrap() as i128
    );
}
