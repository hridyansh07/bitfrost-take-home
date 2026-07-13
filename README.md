# BitFrost Prime — Cross-Venue Position Keeper

A slice of a clearing engine: two trading venues stream fills at us, and we turn that into one
trustworthy, replayable record of positions and PnL.

---

## 1. The problem

Two venues send trade fills over the wire — a perpetual-futures venue (**HL**) and a binary
prediction-market venue (**PM**). Each has its own message format, its own clock, and its own
sequence numbers. The pipeline has four jobs:

1. **Catch** every raw event, and never lose one silently.
2. **Normalize** both wire formats into one canonical `Fill`.
3. **Order** fills from both venues into a single, deterministic global sequence.
4. **Keep** net positions, realized PnL, and fees per account.

The venues' clocks can't be compared, so exchange timestamps must never
decide cross-venue order. Instead, every event is stamped with the moment *we* received it, and
that stamp drives the ordering. The full algorithm — receive-time stamps, per-venue ring
buffers, a frontier-gated merge that hands out one global sequence number — lives in
**[Algo.md](Algo.md)**, together with its invariants and trade-offs. This README covers what
the system does and how to navigate it.

### Run it

```bash
just              # format check, lints, all tests
just test         # unit + fixture-driven integration + realtime harness
just lint         # clippy, default rust-wide lint groups, warnings denied
just fixtures     # regenerate fixtures/ from scratch (fixed seed; timestamps anchored at generation time)
just realtime     # watch the sequencer run on real threads → out/ordering_algo.json
```

Without `just`:

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
cargo run -p types --bin gen_fixtures    # rewrites fixtures/
```

---

## 2. Assumptions, and a walk through the code

### Assumptions

- **No float ever touches capital state.** Prices, quantities, and money are integer newtypes —
  `Ticks(i64)`, `Lots(i64)`, `Micro(i128)` (micro-dollars, 1 USD = 1,000,000 µ$). Floats exist
  in exactly three places, all at the wire boundary: the raw PM price field, the fixture
  generator that emits it, and the single conversion in `convert.rs` that rounds it to ticks
  once. Nothing downstream ever sees a float.
- **Receive time is the frame of reference.** Cross-venue order comes from when an event
  reached us, never from exchange timestamps. Live code stamps the arrival instant; fixture
  tests stamp a logical clock in arrival order. The sequencer only ever sorts by that stamp.
- **Each venue delivers its own events in order.** Websocket streams are considered ordered,
  so a venue-sequence walk-back is treated as a fault.
- **The tape is immutable.** Once a fill has its global sequence number, it is never rewritten.
- **Instruments are known up front.** A registry keyed by `(venue, symbol)` holds tick size,
  lot size, and the scale that keeps notional math exact. Unknown symbols are rejected.

### The crates

```
Cargo.toml                     workspace: types, position_keeper, ingester
justfile                       one-command workflows: just verify / test / fixtures / realtime
fixtures/                      generated NDJSON streams + manifest
  hl.ndjson  pm.ndjson         ~500 events each (HL perps / PM binaries)
  matched.ndjson               100 clean paired fills for the conservation test
  manifest.json                line counts, planted-defect ids, expected sums, base_ts anchor
crates/
  types/                       every shared shape lives here — no behavior
    src/fixed.rs               Ticks / Lots / Micro
    src/instrument.rs          Venue, Kind, InstrumentSpec, Registry
    src/boundary.rs            HlEvent / PmEvent — the raw venue schemas
    src/canonical.rs           Fill (the type that enters the system), Side, RejectReason
    src/convert.rs             Canonicalize: boundary → Fill, with rejection reasons
    src/stream.rs              RawLine / RawEvent, NDJSON readers
    src/ingest.rs              pipeline vocabulary: IngestOutcome, DeadLetter, Alert, stats
    src/sequencer.rs           tape shapes: SequencedFill, SequencerError
    src/position.rs            Position, PositionDelta, PositionKey
    src/fixtures.rs            seeded fixture generation (drift-checked by the tests)
    src/bin/gen_fixtures.rs    thin CLI wrapper around types::fixtures::generate
  ingester/                    the pipeline: stamp → normalize → dedup → sequence
    src/lib.rs                 Ingester: fill_caught(recv_ts, line), dead letters, tape drains
    src/sequencer.rs           the Algo.md merger: ring buffers, frontiers, global_seq
    tests/common/mod.rs        shared test harness (seq-ordered streams, seeded riffle, clock)
    tests/pipeline.rs          the fixture-driven suite (see section 3)
    tests/ordering_realtime.rs the algorithm running on real threads with real timestamps
  position_keeper/             a consumer of the tape: apply(fill) → position, PnL, fees
    src/lib.rs                 PositionKeeper, cost-basis accounting, half-even rounding
```

### The path of one event

```
raw NDJSON line
  │  stamped with recv_ts by whoever received it
  ▼
Ingester::fill_caught(recv_ts, line)
  │  parse failure            → dead letter
  │  unknown symbol, bad tick → dead letter with the reason
  │  exact duplicate          → dropped
  │  same id, different data  → dead letter + alert; first copy considered canonical
  │  (every arrival, good or bad, advances its venue's frontier — bad traffic never stalls
  │   the other lane)
  ▼
Sequencer  (Algo.md)
  │  per-venue ring buffer, merged by (recv_ts, venue, venue_seq)
  ▼
the canonical tape — every fill carries a strictly increasing global_seq
  │  drain_ready() / flush()
  ▼
consumers, e.g. PositionKeeper::apply → net qty, realized PnL, fees
```

Frontiers are not sort keys. They are per-venue proof that a receiver has advanced far enough
in our receive-time clock. If a lower-priority venue has a buffered head at `recv_ts = 1005`
while a higher-priority venue has only proven progress through `1000`, the sequencer waits:
the higher-priority receiver could still produce an event stamped `1001..=1005`, and that
event would sort before the lower-priority head. In production the same `T_IDLE` cadence should
keep these frontiers close; the gate exists for the short skew window where one receiver has
advanced and another has not ticked yet. The behavior is to stall briefly, not to misorder.

The keeper uses **cost-basis** accounting: adding to a position accumulates cost, reducing one
realizes PnL on the closed portion (banker's rounding), and crossing through zero closes then
reopens.

**Special Mention (Binary Tokens)**: binaries model **NO as −YES**. A NO fill folds into the YES leg at
price `$1 − p` with the side flipped.

Instruments in `Registry::standard()`:

| Symbol          | Venue | Kind   | Tick   | Lot    |
| --------------- | ----- | ------ | ------ | ------ |
| `BTC-PERP`      | HL    | Perp   | 0.5    | 0.0001 |
| `ETH-PERP`      | HL    | Perp   | 0.05   | 0.001  |
| `FED-CUT-SEP`   | PM    | Binary | 0.0001 | 1      |
| `CPI-ABOVE-AUG` | PM    | Binary | 0.0001 | 1      |

---

## 3. What the tests prove

The fixtures are not clean data. The generator deliberately plants the failure modes a real
feed produces, and the test suite checks that each one is handled.

### Generating and reshaping the fixtures

`just fixtures` rewrites `fixtures/` through `types::fixtures::generate`. The knobs sit at the
top of `crates/types/src/fixtures.rs`: `SEED` drives every random choice (same seed, same
events), `HL_LINES` / `PM_LINES` size the streams, and the defect cadences are inline in the
generators — every 37th/31st event is duplicated, every 41st/43rd is swapped out of order, and
the poison and byzantine events are appended at the end of each stream. Exchange timestamps
are anchored at the moment of generation, and that anchor is recorded as `base_ts` in
`manifest.json`, so a fixture set is always reproducible even though the clock moves. If you
change a knob, rerun `just fixtures` and update the counts the pipeline suite pins (six dead
letters, two alerts, and so on).

### Edge cases planted in the fixtures

| Edge case | What the pipeline does |
| --- | --- |
| Exact duplicate (same id, same payload) | Dropped and counted; state mutates once |
| Byzantine duplicate (same id, *different* payload) | Dead letter + alert — even when one of the copies is invalid; the first observed copy stays canonical |
| Poison (unknown symbol, off-tick price, impossible probability) | Rejected into the dead-letter channel with a reason |
| Gap in venue sequence numbers | Tolerated and counted; one venue's gap never stalls the other |
| Out-of-order delivery within a venue | Quarantined as a producer fault (a pipeline test feeds the raw, unsorted fixture order to prove it) |
| Malformed line (unparsable JSON) | Dead letter with the raw text preserved (unit-tested — the fixture files themselves contain only parseable lines) |

### The guarantees the suite checks

All in `crates/ingester/tests/pipeline.rs` unless noted:

- **The fixtures can't drift.** The suite regenerates the fixture set — using the time anchor
  recorded in `manifest.json` — into a temp directory and asserts it is byte-identical to the
  checked-in `fixtures/`. Test data and generator always agree.
- **Same input, same tape.** Feeding the identical stamped arrivals twice produces the exact
  same canonical tape.
- **Arrival interleave doesn't change positions.** Different cross-venue interleavings are
  different tapes (arrival *is* the frame of reference), but per-venue order is preserved and
  instruments don't span venues, so final positions come out identical.
- **Money conserves.** `matched.ndjson` pairs every buy with an equal-size sell at the same
  price, so summed realized PnL must be exactly zero and fees must match the manifest.
- **The ordering invariants hold** (unit tests in `ingester/src/sequencer.rs`): stamps and
  venue sequences can't walk back, a flushed sequencer refuses further pushes, a fill is only
  released once no earlier arrival can still appear, ties break by venue priority, and an
  idle venue advances via explicit ticks. `tests/ordering_realtime.rs` then re-checks it all
  with genuine wall-clock stamps from threaded venue streams (the merge itself is single-writer
  by design).
- **No float in capital state** — holds by construction (the three boundary sites in the
  assumptions above).

### Known limitations / next steps

- Real venue adapters (live websockets), an append-only event log
- Replay Invariants using epoch based handling for missed fills
- Gapped Fill Sequence Handling using refetches or special fill states
- The T_IDLE heartbeat driver from Algo.md is not built; frontier liveness comes from
  arrivals and explicit `tick` calls.
- Lane buffers are unbounded (`INITIAL_LANE_CAPACITY` is an allocation hint); a hard bound
  with backpressure is an Algo.md extension point.
- The no-float guarantee is enforced by review discipline, not an automated lint.
- `Fill` carries both `instrument` and `symbol` (the keeper keys on `instrument`; `symbol` is
  kept for the venue-side lookup).
