# Development Plan & Decision Log

Spec-support document: the original development plan as approved, the verbatim prompts that
produced and then reshaped it, and a map from plan to final implementation. Where the shipped
code differs from the plan, the difference was a deliberate, recorded decision — not drift.

**How to read this:** every block quoted under a “Prompt” heading is the developer's message
to the agent, reproduced verbatim (typos included — they are the authenticity signal). One
decision (D2) deliberately has no prompt behind it: it came from a failing test during
implementation, and is recorded as such.

---

## Part I — Origin and the approved plan

### The prompts that led to the plan

> **Prompt (repo restructure, later reverted):** “Hey claude this take home has been extended
> to a second case study that we also have to implement lets change the repository structure
> to have this one inside a specific folder and then we can add the second folder later on?”

> **Prompt (gap review against the new spec):** “I think if you see the current state of the
> position keeper we are a ways away from what is expected now which is very close to a
> complete pipeline on how this is supposed to work out. I think this is the first order of
> buisness why don't you go over this and report on what should be the best place to start
> the refactor from?”

> **Prompt (the plan's actual mandate):** “No wait up lets make a couple of changes. First
> lets add the algo added sequencer to our current code and ensure that our fixture based
> approach does work with ring buffers and the global_seq that we are attaching to everything.
> Then we can talk about the other things also for now lets move everything back to the
> original version I think we might not need to do the second case study so the folder based
> approach is not really required. Lets make a plan for the first part and you can do the
> second one through a subagent?”

Two pieces of feedback were folded into the plan during its review, before approval:

> **Prompt (scope trims):** “You can drop the original sequencing logic and write the global
> sequencer as part of the ingester maybe in a sequencer crate underneath the current
> ingetser. Most of the current tests can remain as they are the fixture tests that run the
> sequencer against the generated events is the pipeline that we might have to change the most
> otherwise everything should remain as it is. Drive function in itself is also not that
> required since we are driving the code through the tests right? but we can maybe let that be
> for now and circle back when we are cleaning up the repo before final submission”

> **Prompt (data structures — see D1):** “Hmm are BTrees the correct choice for buffers here?
> Both frontier and Last_seq can be HashMap instead of BTreeMap also the major reason for
> using ring buffers is that we don't spend time / complexity on the push/pop from the buffer
> itself can you check the algo and rethink this part?”

One question was put back to the developer during planning — how simulation mode should
synthesize `recv_ts` stamps for the static fixtures. The chosen answer, **“Exchange-ts
schedule”** (interleave venue streams by exchange timestamp as a *simulated arrival clock*,
never as a sort key), was written into the plan below — and later overturned wholesale by D3.
That reversal is documented rather than hidden: it is the plan's single biggest divergence.

### The approved plan (verbatim)

> # Implement the Algo.md sequencer + revert repo restructure
>
> ## Context
>
> The take-home's Case Study 1 requires a deterministic, single-writer canonical ordering across
> venues — exactly what `Algo.md` designs (per-venue ring buffers, `recv_ts` stamping, watermark,
> K-way merge, `global_seq`) and what `crates/ingester/tests/ordering_realtime.rs` already
> validates against a stub. Today the production path (`Ingester::ordered_fills()`) is
> venue-grouped (all HL, then all PM), which the code's own comments call indefensible. This plan
> promotes Algo.md from doc + test stub into real pipeline code, driven by the existing NDJSON
> fixtures, with `global_seq` attached to every fill that reaches the position keeper.
>
> Separately: the second case study is likely being dropped, so the `position-keeper/` folder
> restructure gets reverted — everything moves back to the repo root. This runs FIRST (via a
> subagent) so all paths below are root-relative.
>
> ## Part 0 — Revert repo structure (delegate to a subagent)
>
> `git mv` everything in `position-keeper/` back to the repo root; restore `.gitignore`; leave
> `CaseStudy.md` untouched; verify `git status` returns to the pre-restructure state and
> `cargo check --workspace` passes. Nothing gets committed.
>
> ## Part 1 — Sequencer implementation
>
> ### New module: `crates/ingester/src/sequencer.rs`
>
> Promote the stub from `ordering_realtime.rs` into a real component:
>
> ```rust
> pub struct SequencedFill { pub global_seq: u64, pub recv_ts: u64, pub fill: Fill }
>
> struct Lane {
>     buffer: VecDeque<(u64 /*recv_ts*/, Fill)>, // ring buffer: push_back/pop_front O(1) amortized,
>                                                // sorted by construction (single in-order producer)
>     frontier: u64,                             // last stamp seen (advanced by push or tick)
>     last_seq: u64,                             // producer-order enforcement (venue_seq guard)
> }
>
> pub struct Sequencer {
>     lanes: [Lane; 2],       // indexed by venue discriminant — no map at all: Venue is a closed
>                             // enum, so lookup is O(1) array indexing (no hashing, no tree walk)
>     next_global_seq: u64,
>     max_depth: usize,       // high-water mark (future queue-depth metric hook)
> }
> ```
>
> Data-structure rationale (matches Algo.md's cost model — O(1) push, O(K) head scan per emit,
> O(1) pop): the per-venue lane holds its own `frontier`/`last_seq` scalars instead of side maps;
> `VecDeque` is Rust's growable ring buffer, pre-allocated via `with_capacity`. Watermark =
> `min` over lane frontiers (O(K) array scan); the min-by-`SORT_KEY` head scan iterates lanes in
> index order with strict `<`, which yields the venue-priority tie-break for free and has no map
> iteration order to reason about.
>
> API (all synchronous — determinism-testable; threads live only in the realtime test harness):
> - `push(venue, recv_ts, fill) -> Result<(), SequencerError>` — rejects `recv_ts` below the
>   venue frontier (Algo.md I6) and non-increasing `venue_seq` per venue (producer assumption,
>   I1; gaps allowed). Advances the frontier to `recv_ts`.
> - `tick(venue, now_ts)` — frontier advance for an idle venue (the `T_IDLE` heartbeat, I4): a
>   silent venue must never stall the watermark.
> - `drain_ready() -> Vec<SequencedFill>` — watermark = min over frontiers; repeatedly pop the
>   min-by-`SORT_KEY` head across buffer heads (`SORT_KEY = (recv_ts, venue, venue_seq)`) and
>   assign `global_seq += 1`. **Correctness tweak vs. Algo.md pseudocode:** emit only while
>   `head.recv_ts < watermark` (strict), not `<=` — a venue may still produce an event at
>   `recv_ts == frontier`, and emitting a tied event early would break I2/I7.
> - `flush() -> Vec<SequencedFill>` — end-of-stream: drain everything regardless of watermark.
>
> ### Ingester integration
>
> The Ingester stays the normalize/dedup/byzantine/park stage; the Sequencer sits after park.
> Delete `ordered_fills()`; new `sequenced_fills()` builds a deterministic sim schedule and
> streams it through the Sequencer. `drive(keeper)` kept for now but rewired onto the tape,
> flagged `TODO(cleanup before submission)`.
>
> ### Simulation clock (user decision: exchange-ts-derived schedule)
>
> In fixture/sim mode, synthesize arrival: interleave the two venue streams by exchange
> timestamp, then stamp `recv_ts = 1, 2, 3, …` in schedule order. Exchange ts drives only the
> simulated arrival clock; the sequencer's sort key is `recv_ts` alone.
>
> ### Tests (principle: most tests stay untouched; fixture-pipeline tests change the most)
>
> New sequencer unit tests covering I1–I7; `fixtures.rs` asserts whole-tape equality across
> shuffled arrival orders; `ordering_realtime.rs` drives the real Sequencer; other suites
> unchanged.
>
> ### Docs
>
> README data-flow update; Algo.md invariants marked implemented/tested with the watermark
> refinement noted.
>
> ## Explicitly out of scope
>
> Real venue adapters (Binance/Hyperliquid/Polymarket), WebSocket layer, append-only event log,
> Prometheus/Grafana, canonical-event redesign. The sequencer's published output is the natural
> future hook for the append-only log.

---

## Part II — Decision log (verbatim prompts → what changed)

### D1. Data structures: no maps in the hot path *(plan review)*

> **Prompt:** “Hmm are BTrees the correct choice for buffers here? Both frontier and Last_seq
> can be HashMap instead of BTreeMap also the major reason for using ring buffers is that we
> don't spend time / complexity on the push/pop from the buffer itself can you check the algo
> and rethink this part?”

Outcome went further than the prompt's HashMap suggestion: since `Venue` is a closed enum,
*no map at all* — a fixed `[Lane; 2]` array indexed by venue discriminant, per-lane scalar
`frontier`/`last_seq`, `VecDeque` ring buffers. Scanning the array in index order *is* the
venue-priority tie-break.

### D2. Emit gate correction *(implementation discovery — no prompt)*

Caught by a failing unit test, not by direction. The plan's "strict global watermark" was
still wrong: the global min includes the emitting lane's own frontier (which equals its
newest stamp), so strict `<` deadlocks the freshest venue behind itself — while Algo.md's
original `<=` can emit a lower-priority tie before a higher-priority venue's equal stamp
arrives. The correct gate is **per-lane and asymmetric**: a head stamped `t` emits once every
higher-priority lane is strictly past `t` and every lower-priority lane has at least reached
`t`. Documented in `Algo.md` ("Implementation refinement") and `sequencer.rs`.

### D3. The streaming pivot: code embodies the algorithm *(major redirection)*

> **Prompt:** “Hmm okay but I think we are doing something else that what should be done here.
> This is too convoluted. I think we are making a mistake on multiple places first of all
> ingester's lib does not need the driver and all ingest_lines written into it? Why are types
> defineid on the lib.rs files when we have a seprate create defined to do this makes no
> sense. The logic is not to replay fills that we already have with recv_ts added to them on a
> later date. The integration tests already attaches a good enough random timestamps we can
> just make the recv_ts as the Instant that the event is received. The logic is to show that
> the algorithm works not stub the code into working with everything changed to match the code
> rather than the algo?”

Two scoping questions were put back and answered explicitly: **“Ingester owns Sequencer”**
(stamp-at-receive wiring lives in the pipeline, not the harness) and **“Algo assumption wins”**
(drop the park reorder buffer; in-order producers are enforced, walk-backs quarantined).

Outcome: `fill_caught(recv_ts, line)` became the single entry point — the caller stamps
`recv_ts` at the moment of receipt and accepted fills go onto the lanes right then. The plan's
post-hoc exchange-ts sim schedule, `sequenced_fills()`, `drive()`, and `ingest_line()` were all
deleted. Shared types moved to the `types` crate. Consequences accepted knowingly: byzantine
winner became first-observed (immutable tape, I5) instead of `min(Fill)`, and determinism was
reframed — the canonical tape is a pure function of the *stamped arrival tape*, not of
arbitrary arrival shuffles.

### D4. Lean surface *(follow-up redirection)*

> **Prompt (second half; first half is D5):** “Second can we also shift the sequencer types to
> the types crate and only keep the implementation details there lets also lose everything
> that we don't use today like the max_depth which just seems to add moree lines of code
> without giving us anything on the algo today.”

Outcome: `SequencedFill`/`SequencerError` moved to `types/src/sequencer.rs`; `max_depth`
removed; the production-dead `merge`/`shuffle` helpers (which embodied the forbidden
exchange-ts ordering) deleted. Standing rule from here on: anything unused today is noise.

### D5. Test harness consolidation *(proposed on request, then approved)*

> **Prompt:** “A simple misconeption here that I think can make this code even cleaner the
> current integration tests assume the entirety of the code to run as if someone can consume
> this. Instead of writing these many tests that mostly repeat the same structure can we not
> make a harness of some kind for this? Which ensures that the fixtures are generated and used
> for the tests since the number of events there should be enough to handle almost every
> scenario instead of blatantly repeating everything what do you think just a suggestion want
> to know your thoughts before moving forward on that.”

> **Prompt (go-ahead, plus the justfile):** “Yes lets build that as well. Another change after
> would be to write just based commands to run all tests and create the fixtures from scratch
> which should be a small enough task but make everything easier to run and handle for any
> external viewer”

> **Prompt (linter):** “Great lets also add a linter to the just file thorugh clippy? make the
> lint rust wide defaults for now”

Outcome: four near-duplicate integration files collapsed into `tests/pipeline.rs` over a
shared `tests/common/mod.rs` harness; fixture generation moved into `types::fixtures::generate`
enabling the **byte-drift check** (regenerate, compare byte-for-byte with the checked-in
`fixtures/`); a `justfile` with `verify` (fmt-check + clippy `-D warnings` + full suite) as
the single documented verification command, plus `test` / `lint` / `fixtures` / `realtime`.

### D6. README restructure *(docs redirection)*

> **Prompt:** “Great the next part is to tighten up the readmes to follow structures that are
> easy to handle for any new viewer. Most importantly lets break the readmes into 3 sections.
> 1. Introduction to the problem and defer the algo to the algo.md file. + all necessary
> commands to run and test the structure. 2. Major assumptions and a brief walkthrough of the
> code and structure of the directory. 3. The core assumptions and the edge cases that we are
> testing use simple language for this don't try to overcomplicate the readme it should read
> naturally and be a good guide through the entire code.”

Outcome: README rewritten into exactly those three sections, plain language, with all
algorithm mechanics deferred to `Algo.md`.

### D7. Fixture generation parameters + live time anchor

> **Prompt:** “Lets also add a small para about fixture generation and changing the parameters
> to influce how the fixtures can be generated? Also for the BASE_TS can we use the current
> timestamp whenever generating?”

Outcome: the generator's knobs (seed, stream sizes, defect cadences) documented in the README;
`BASE_TS` became the wall-clock moment of generation, **recorded as `base_ts` in
`manifest.json`** so the drift check regenerates with the recorded anchor — fresh timestamps
on every `just fixtures`, reproducibility preserved.

### D8. Independent review and hardening pass

> **Prompt (launch review):** “Do you think we are missing anything
> maybe launch a new subagent to review the current code and see if anything looks
> out of order or if there are glaring bugs that we are falling flat into also take a look
> into anything that does not conform to our invariants. And you can look for some edge cases
> that we might have missed if any.”

> **Prompt (combine with the external review):** “Okay can you also go through the
> Review_findings.md that I got done though an independent subagent and combine all the
> findings that you have and that are written in the .md file feel free to verify them as
> well”

> **Prompt (execute):** “Yes lets fix the list and move forward”

Outcome: two independent reviews combined, cross-verified, and fully fixed — 14 findings, see
`REVIEW_FINDINGS.md` for each with its resolution and regression test. Highlights: byzantine
identity checked *before* canonicalization (the one real bug); every arrival — even poison —
advances its venue's frontier (liveness); `flush()` seals the sequencer; quarantined
retransmits dedup; venue derived from the parsed payload, not the transport wrapper; a
docs-versus-reality pass; a dead-API sweep. Seven regression tests added (64 → 71).

---

## Part III — Plan → implementation map

| Plan item | Shipped as |
| --- | --- |
| Part 0: revert repo restructure | Done via subagent; tree returned to root layout |
| `sequencer.rs` with lanes/frontiers/`global_seq` | `crates/ingester/src/sequencer.rs` — as planned, plus the D2 asymmetric emit gate and a D8 `sealed` flag on `flush` |
| Types in the plan's code block | Moved to `crates/types/src/sequencer.rs` (D4); `max_depth` removed (D4); `last_seq` is `Option<u64>` |
| `sequenced_fills()` + exchange-ts sim schedule | **Replaced** (D3) by stamp-at-receive: `Ingester::fill_caught(recv_ts, line)` pushes at arrival; no post-hoc schedule exists |
| Park feeding the sequencer | **Dropped** (D3): in-order producers enforced; walk-backs quarantined as `OutOfOrder` dead letters |
| `drive(keeper)` kept with TODO | **Deleted** (D3): consumers apply the tape directly |
| Whole-tape equality across shuffles | **Reframed** (D3): identical stamped arrivals → identical tape (`pipeline.rs`); different interleaves → identical *positions*; raw fixture order → planted disorder quarantined |
| Sequencer unit tests I1–I7 | In `sequencer.rs` tests + `ordering_realtime.rs` (wall-clock stamps from threaded streams) + D8 regressions in `ingester/src/lib.rs` tests |
| Docs updates | README rewritten into problem/walkthrough/tests sections (D6); Algo.md carries typo fixes, the D2 refinement, and honest invariant statuses |
| Out-of-scope list | Unchanged — adapters, event log, Prometheus/Grafana remain future work; the sealed tape is the event-log attachment point |

Verification at every stage: `just verify` (fmt --check, clippy `-D warnings`, full test
suite — 71 tests at the time of writing).
