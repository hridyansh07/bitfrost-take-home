use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use ingester::Ingester;
use position_keeper::PositionKeeper;
use types::{merge, read_hl, read_pm, shuffle, Position, PositionKey, RawLine, Registry};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

fn run(lines: Vec<RawLine>) -> BTreeMap<PositionKey, Position> {
    let mut ingester = Ingester::new(Registry::standard());
    for line in lines {
        ingester.ingest_line(line);
    }
    let mut keeper = PositionKeeper::new(Registry::standard());
    let report = ingester.drive(&mut keeper);
    assert!(report.apply_errors.is_empty());
    keeper.into_positions()
}

#[test]
fn final_positions_are_independent_of_arrival_order() {
    let fixture_dir = fixture_dir();
    let hl = read_hl(&fixture_dir.join("hl.ndjson")).unwrap();
    let pm = read_pm(&fixture_dir.join("pm.ndjson")).unwrap();
    let merged = merge(hl, pm);
    let expected = run(merged.clone());
    let mut shuffled_seven = merged.clone();
    let mut shuffled_ninety_nine = merged;
    shuffle(&mut shuffled_seven, 7);
    shuffle(&mut shuffled_ninety_nine, 99);
    assert_eq!(run(shuffled_seven), expected);
    assert_eq!(run(shuffled_ninety_nine), expected);
}
