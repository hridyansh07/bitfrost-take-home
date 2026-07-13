use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use types::{read_hl, read_pm, Canonicalize, Fill, RawEvent, RawLine, Registry, RejectReason};

#[test]
fn fixtures_parse_and_canonicalize() {
    let fixture_dir = fixture_dir();
    let manifest: Value = serde_json::from_str(
        &fs::read_to_string(fixture_dir.join("manifest.json"))
            .expect("manifest should be readable"),
    )
    .expect("manifest should be valid JSON");
    let hl = read_hl(&fixture_dir.join("hl.ndjson")).expect("HL fixture should be readable");
    let pm = read_pm(&fixture_dir.join("pm.ndjson")).expect("PM fixture should be readable");

    assert!(hl.iter().all(|line| line.parsed.is_ok()));
    assert!(pm.iter().all(|line| line.parsed.is_ok()));
    assert_eq!(hl.len() as u64, manifest["hl"]["lines"].as_u64().unwrap());
    assert_eq!(pm.len() as u64, manifest["pm"]["lines"].as_u64().unwrap());
    assert_sequence_pathologies(&hl, "hl-dup-byz");
    assert_sequence_pathologies(&pm, "pm-dup-byz");

    let registry = Registry::standard();
    let results = canonical_results(hl.iter().chain(&pm), &registry);
    let mut poison_ids = BTreeSet::new();
    assert_poison(&manifest["hl"]["poison"], &results, &mut poison_ids);
    assert_poison(&manifest["pm"]["poison"], &results, &mut poison_ids);

    for line in hl.iter().chain(&pm) {
        let event = line.parsed.clone().unwrap();
        if !poison_ids.contains(event.event_id()) {
            assert!(canonicalize(event, &registry).is_ok());
        }
    }
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures")
}

fn canonical_results<'a>(
    lines: impl Iterator<Item = &'a RawLine>,
    registry: &Registry,
) -> BTreeMap<String, Result<Fill, RejectReason>> {
    lines
        .map(|line| {
            let event = line.parsed.clone().unwrap();
            let event_id = event.event_id().to_string();
            (event_id, canonicalize(event, registry))
        })
        .collect()
}

fn canonicalize(event: RawEvent, registry: &Registry) -> Result<Fill, RejectReason> {
    event.canonicalize(registry)
}

fn assert_poison(
    poison: &Value,
    results: &BTreeMap<String, Result<Fill, RejectReason>>,
    poison_ids: &mut BTreeSet<String>,
) {
    for entry in poison.as_array().unwrap() {
        let event_id = entry["event_id"].as_str().unwrap();
        poison_ids.insert(event_id.to_string());
        let result = results.get(event_id).unwrap();
        if entry.get("detected_at").is_some() {
            assert!(result.is_ok(), "{event_id}");
            continue;
        }
        match (entry["reason"].as_str().unwrap(), result) {
            ("UnknownSymbol", Err(RejectReason::UnknownSymbol(_)))
            | ("OffTick", Err(RejectReason::OffTick { .. }))
            | ("PriceOutOfRange", Err(RejectReason::PriceOutOfRange { .. })) => {}
            (reason, result) => panic!("{event_id} expected {reason}, got {result:?}"),
        }
    }
}

fn assert_sequence_pathologies(lines: &[RawLine], byzantine_id: &str) {
    let events = lines
        .iter()
        .map(|line| line.parsed.clone().unwrap())
        .collect::<Vec<_>>();
    let mut seq_owners = BTreeMap::new();
    let mut by_id = BTreeMap::<String, Vec<RawEvent>>::new();

    for event in &events {
        if let Some(owner) = seq_owners.insert(event.seq(), event.event_id().to_string()) {
            assert_eq!(owner, event.event_id());
        }
        by_id
            .entry(event.event_id().to_string())
            .or_default()
            .push(event.clone());
    }

    assert!(events.windows(2).any(|pair| pair[0].seq() > pair[1].seq()));
    let unique_sequences = seq_owners.keys().copied().collect::<Vec<_>>();
    assert!(unique_sequences
        .windows(2)
        .any(|pair| pair[1] > pair[0] + 1));
    assert!(by_id
        .values()
        .any(|duplicates| duplicates.len() > 1
            && duplicates.iter().all(|event| event == &duplicates[0])));

    let byzantine = by_id.get(byzantine_id).unwrap();
    assert_eq!(byzantine.len(), 2);
    assert_ne!(byzantine[0], byzantine[1]);
}
