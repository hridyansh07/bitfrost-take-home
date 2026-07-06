# BitFrost Prime — Cross-Venue Position Keeper

A slice of a clearing engine. It ingests trade-fill events from two venues — a perpetual-futures
venue (**HL**) and a binary prediction-market venue (**PM**) — normalizes them into one canonical
domain model, and maintains net positions, realized PnL, and fees per account.

## The one rule everything is built around

> **No floating-point value may touch capital state.** All prices and quantities in the domain core
> are fixed-point integers. Floats appear only at the adapter boundary, converted once, with explicit
> rounding.

Every capital-bearing value is an integer newtype:

| Concept  | Type          | Unit                                    |
| -------- | ------------- | --------------------------------------- |
| Price    | `Ticks(i64)`  | integer number of instrument ticks      |
| Quantity | `Lots(i64)`   | signed integer lots (net position)      |
| Money    | `Micro(i128)` | micro-dollars (1 USD = 1_000_000 µ$)    |

Notional stays exact because each instrument carries a precomputed scale:

```
notional_micro   = price_ticks × qty_lots × micro_per_tick_lot
micro_per_tick_lot = tick_micro × lot_qty_e9 / 1_000_000_000
```

Floats (`f64`) exist in exactly three places, all at the boundary: `PmEvent.price` (the raw wire
value), `gen_fixtures.rs` (emitting that wire value), and `convert.rs::price_to_ticks` (the single
round-to-tick conversion). Nothing downstream ever sees a float.

## Repository layout

```
Cargo.toml                     workspace: types, position_keeper, ingester
fixtures/                      generated NDJSON streams + marks + manifest
  hl.ndjson  pm.ndjson         ~500 events each (HL perps / PM binaries)
  matched.ndjson               100 clean paired fills for the conservation test
  marks.json  manifest.json    mark prices + known-good answers for tests
crates/
  types/                       the domain core (+ the boundary and the reader)
    src/fixed.rs               Ticks / Lots / Micro
    src/instrument.rs          Venue, Kind, InstrumentSpec, Registry, InstrumentSource (venue-aware lookup)
    src/boundary.rs            HlEvent / PmEvent — raw venue schema (serde lives here)
    src/canonical.rs           Fill (the type that enters the system), Side, RejectReason
    src/convert.rs             Canonicalize trait: boundary → Fill, off-tick/off-lot/range rejection
    src/stream.rs              RawEvent / RawLine, read_hl / read_pm, deterministic merge / shuffle
    src/position.rs            Position, PositionDelta, PositionKey
    src/bin/gen_fixtures.rs    seeded generator that writes fixtures/*
  position_keeper/             Part 2: apply(fill) → net position, realized PnL, fees
    src/lib.rs                 PositionKeeper<S>, PositionStore, cost-basis transition, half-even rounding
  ingester/                    Part 1 + the driver: catch → normalize → dedup → reorder → apply
    src/lib.rs                 Ingester<S>, FillSink::fill_caught, dead-letter channel, drive(keeper)
    tests/fixtures.rs          full-fixture coverage + replay/shuffle determinism
```

## How data flows

```
NDJSON line
  │  read_hl / read_pm  (serde, one RawLine per line, parse errors captured not thrown)
  ▼
RawEvent (HlEvent | PmEvent)              ── the type we RECEIVE at the boundary
  │  Ingester::fill_caught / ingest_line
  ▼
Canonicalize  ── strings/float → fixed-point; unknown symbol / off-tick / off-lot / out-of-range → dead letter
  ▼
Fill                                      ── the canonical type that ENTERS the system
  │  idempotency on (venue, event_id)
  │    · exact duplicate            → dropped (exactly-once)
  │    · same key, diff payload     → dead letter + alert, deterministic min(Fill) kept
  │  reorder buffer keyed (venue, seq, event_id): apply in seq order, gaps tolerated
  ▼
ordered_fills()  ──drive──►  PositionKeeper::apply
                               ▼
                       net qty · average entry (cost-basis) · realized PnL · fees
```

The boundary↔canonical split is deliberate: the raw schema shape (decimal strings, a float price)
never becomes internal state. Conversion is trait-based (`Canonicalize`), so the mapping for a venue
lives in exactly one place.

### Delivery pathologies handled

The fixtures plant real feed defects; the ingester deals with each:

- **Duplicates** (same `event_id`, identical payload) → dropped, state mutated once.
- **Byzantine duplicates** (same `event_id`, *different* payload) → dead-lettered + an alert raised;
  the surviving copy is `min(Fill)`, so it is **byte-identical regardless of arrival order**.
- **Out-of-order** delivery → buffered and applied in `seq` order.
- **Gaps** in `seq` → tolerated (applied in order, missing seqs skipped); one venue's gap never
  stalls the other, since each venue reorders independently.
- **Poison** (unknown symbol, off-tick price, out-of-range probability) → rejected into the
  dead-letter channel with a reason; never silently dropped, never allowed to mutate state.

## Instruments

`Registry::standard()` (lookups are keyed by `(venue, symbol)`):

| Symbol          | Venue | Kind   | Tick    | Lot     | µ$ / tick·lot |
| --------------- | ----- | ------ | ------- | ------- | ------------- |
| `BTC-PERP`      | HL    | Perp   | 0.5     | 0.0001  | 50            |
| `ETH-PERP`      | HL    | Perp   | 0.05    | 0.001   | 50            |
| `FED-CUT-SEP`   | PM    | Binary | 0.0001  | 1       | 100           |
| `CPI-ABOVE-AUG` | PM    | Binary | 0.0001  | 1       | 100           |

PM binaries model **NO as −YES**: a NO fill folds into the YES leg with price `$1 − p` and the side
flipped, so the keeper only ever reasons about one leg per market.

## Position accounting (Part 2)

`PositionKeeper::apply(&Fill)` is atomic (no partial mutation on error) and uses **cost-basis**
accounting rather than a rounded average price, which is what makes conservation exact:

- Same-direction fill → add to `open_cost` and `net_qty`.
- Reducing fill → realize PnL on the closed portion; basis removed proportionally with
  banker's rounding (`div_round_half_even`).
- Crossing through zero → close-then-open, realizing PnL only on the closed leg.
- `avg_entry_price` is a *derived, rounded view* of the exact cost basis.

## Build, test, run

```bash
cargo build --workspace
cargo test  --workspace          # 58 tests: unit + fixture-driven integration
cargo clippy --workspace

# Regenerate the fixtures deterministically (fixed seed → byte-identical output):
cargo run -p types --bin gen_fixtures            # writes fixtures/
```

## Invariants demonstrated (Part 4)

- **Replay / shuffle determinism** — `crates/ingester/tests/fixtures.rs` ingests the full HL+PM
  streams, drives the keeper, and asserts the final positions are identical across the file order
  and three shuffled arrival orders (compared via `Position: Eq`).
- **Conservation** — `position_keeper` replays `matched.ndjson` (every buy has an equal-size sell at
  the same price across two accounts) and asserts `Σ realized PnL = 0` and `Σ fees = 14950`
  (the value recorded in `manifest.json`).
- **No float in capital state** — holds by construction (see the three boundary sites above).

## Scope

- **Part 1 — Ingestion & Normalization:** done (`types` + `ingester`).
- **Part 2 — Position Keeper:** done (`position_keeper`).
- **Part 4 — Invariants & Determinism:** determinism + conservation covered as above.
- **Part 3 — Scenario Margin:** intentionally **not implemented** (time-boxed out).

### Known limitations / next steps

- No `main.rs` harness printing a SHA-256 digest of canonical state; determinism is currently proven
  by direct `Position` equality instead of a byte digest.
- The no-float guarantee is enforced by discipline, not an automated lint/test.
- `Fill` carries both `instrument: InstrumentId` and `symbol`; the keeper only keys on `symbol`.
