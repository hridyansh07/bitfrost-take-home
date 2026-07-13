# Deterministic Single-Server Multi-Stream Ordering Algorithm

## Problem:

Ingesting Multiple Websocket Streams generates events that are hard to organize.
3rd Party clock-skews or delay in ordering can leave us vulnerable to skews in account
management. Fills that have been registered if consumed in incorrect order can lead to stale account info. Manage incoming events and devise a way to handle events in a manner which is replayable and deterministic for all incoming event data sets.

## Proposed Solution:

Frame of Reference: We need a single common variable to be able to rank and order incoming events. This reference is strictly when the event is received by the server. It ignores exchange timestamps for local guarantees on when the event was seen by the server.

Assumption: we assume no network faults hence no epoch/gap handling for events that are missed and batch pushed on reconnection. All references are made from when the server receives/acks the event. No venue can give events that are stale in reference to an already received event i.e. events cannot walk back.

Algorithm: Incoming events are stamped with a local reference time which is when the event is received by our server. Events are appended to a per-venue FIFO buffer; single producer + monotonic increasing timestamp means each buffer is sorted by construction. A merger thread performs a K-way min-merge over the K buffer heads. This thread is also responsible for attaching a monotonic increasing global_id which can be used to order arbitrary events moving forward. Essentially ordering everything.

Events are ordered in decreasing priority by:

    Local Reference Time | Venue | venue_seq


Trade Offs:

- Deterministic guarantees for ordering are bought by latency on the merging thread
- Event bundles are not gracefully handled by this algorithm
- Latency increases drastically if same timestamp handling is made more complex
- Buffer Bounded limit needs to be tested for durability with event bundles and publisher latency

## Key Variables
```
    recv_ts             // Local Reference time stamped by server
    T_IDLE              // Bounded worst case for trimming and pushing out events
```


## Data structures

```
Event {
    venue          // A | B
    venue_seq      // exchange-assigned per-stream sequence number
    exchange_ts    // exchange timestamp (METADATA ONLY, never a sort key)
    payload        // fill data
    recv_ts        // stamped by us at catch, monotonic clock
}

OutputEvent = Event + { global_seq }        // strictly monotonic, merge-assigned

VENUE_PRIORITY = ENUM { A: 0, B: 1 }

//   equal recv_ts, different venues  -> venue_priority decides
//   equal recv_ts, same venue        -> venue_seq decides
// Equal recv_ts is EXPECTED dependent on reference time clock granuality and event rate
SORT_KEY       = (recv_ts, VENUE_PRIORITY[venue], venue_seq)


T_IDLE         = idle tick period        // bounds worst-case residence

MAX_STALENESS  = bound by |current_ts - exchange_ts| // Should handle transport delays
```

## Ingest task (one per venue, pinned core)

```
loop:
    raw_event = ws.recv_with_timeout(T_IDLE)

    if timeout:                             // No new events advance the latest timestamp
        state.frontier.store(monotonic_now())
        continue

    t = monotonic_now()                      // Stamp before parsing


    event = canonicalize(raw_event)
    event.recv_ts = t

    // staleness guard
    if |current_ts() - event.exchange_ts| > MAX_STALENESS:
        event.stale = true      // Stale Event Flag Only. Deferred Decision

    // dedup
    if seen.get(event.venue_seq)
        drop

    // Ideally use for gap detection in events currently not fixed hence commented out
    // state.last_seq = max(state.last_seq, event.venue_seq)

    // Ideally buffer bounds never hit but event bundles are unpredictable
    // and this push fails due to lack of memory
    // refer #Extension Points on ideas of how to deal with this

    buffer.push(event)

    state.frontier.store(t) // Advance watermark
```

## Merger task (single thread, owns output order)

```
global_seq = store.load(global_seq) // Persisted Storage

loop:
    // Invariant: no venue j produces an event where recv_ts < frontier[j].
    // frontier is a vector, not one scalar watermark; equal recv_ts ties depend on venue priority.
    frontier = [state[v].frontier.load() for v in venues]

    loop:
        head = min_by(SORT_KEY, ring[v].peek() for v in venues)
        // Buffer is sorted with a single producer so global minimum is always at
        // one of the K heads where K is the number of venues
        // Viability check is O(K), independent of buffer depth.

        if no head:
            break

        i = VENUE_PRIORITY[head.venue]
        t = head.recv_ts

        safe = true
        for each venue j != head.venue:
            if VENUE_PRIORITY[j] < i:
                safe &= frontier[j] > t     // a tie from j would sort before head
            else:
                safe &= frontier[j] >= t    // a tie from j would sort after head

        if safe:
            e = pop(head.buffer)               // O(1)
            global_seq += 1
            e.global_seq = global_seq.       // Global deterministic replay
            publish(e)
        else:
            // The current global head is blocked, so no later head can be safe either.
            break
```

### Implementation refinement (discovered while building `crates/ingester/src/sequencer.rs`)

The older `head.recv_ts <= min(frontier)` gate over a global-min watermark is subtly wrong in both
directions once equal stamps are allowed (and they are EXPECTED — see SORT_KEY):

- **`<=` emits too early.** A venue may still legally produce an event AT its current frontier.
  Emitting a lower-priority head at a tied stamp can publish it ahead of a higher-priority
  venue's tie that hasn't arrived yet, breaking I2/I7.
- **A strict `<` over the global min holds too long.** The min includes the head's OWN lane
  frontier, which equals the head's stamp right after its push — the freshest venue would
  deadlock behind itself until its own next event.

The correct gate is per-lane and asymmetric. A head stamped `t` on lane `i` is emittable iff for
every other lane `j`:

```
frontier_j >  t   if j has higher venue priority than i   (j could still tie at t and win)
frontier_j >= t   if j has lower  venue priority than i   (j's ties sort after the head anyway)
```

For K venues stored in priority order, the same check can be written as:

```
min(frontier[0..i])   >  t   // all higher-priority venues
min(frontier[i+1..K]) >= t   // all lower-priority venues
```

Empty ranges are considered true. The current two-venue implementation does this by directly
checking every lane; a larger K implementation can maintain prefix/suffix minima or a segment
tree if the O(K) certificate becomes expensive.

The head's own lane never gates it: same-venue ties are already FIFO-ordered by venue_seq.

## Structural Guarantees

```
Buffer Occupancy       Fixed by arrival_rate * T_IDLE (test and enforce)
Structural latency   ~ inter-arrival gap of the slower stream
                       (bounded above by T_IDLE when a venue is silent)
Per-event merge cost   O(1) for K=2; O(log K) head selection + O(K) frontier
                       certificate for K venues. The O(K) certificate can be
                       optimized with prefix/suffix frontier minima or a segment tree.
```

## Invariants (implemented in `crates/ingester/src/sequencer.rs`)

Tested by the sequencer unit tests and re-checked with genuine wall-clock stamps from threaded
venue streams in `crates/ingester/tests/ordering_realtime.rs` (the merge itself is
single-writer by design). The fixture suite (`tests/pipeline.rs`) asserts that identical
stamped arrivals reproduce the identical canonical tape, and that final positions are
independent of the cross-venue arrival interleave — different interleavings are deliberately
different tapes, because arrival is the frame of reference.

```
I1  Per-stream order:   output order of venue v's events == venue_seq order        [TESTED]
I2  Determinism:        event order is a pure function of buffer and SORT_KEY      [TESTED]
I3  Monotonic output:   global_seq strictly increasing; recv_ts non decreasing;
                        a flushed (sealed) sequencer refuses further pushes        [TESTED]
I4  Liveness:           every arrival — accepted or quarantined — advances its
                        venue's frontier, and manual tick() covers silent venues;
                        the T_IDLE heartbeat *driver* from the pseudocode above
                        is not built                                    [TESTED via ticks]
I5  Immutability:       published history never rewritten; completely replayable   [TESTED]
I6  Watermark safety:   no event is ever pushed with
                        recv_ts < its venue's frontier at push time                [ENFORCED]
I7  Key totality:       SORT_KEY is unique across all events; recv_ts never
                        produces ambiguity                                         [TESTED]
I8 No silent drops:     staleness NEVER drops an event, only flags it;
                        the only drop path is dedup;
                        (staleness flagging itself is still unimplemented —
                        the sequencer's only refusal paths are the I6/I1 push
                        guards, which error loudly rather than drop)
```

## Extension points

Extended Ideas to make this algorithm better for different scenarios.

```
- Redundant A/B sockets per venue: Arbiter between sockets and buffer;
  dedup key (venue, venue_seq), first arrival wins; frontier = max of
  the pair; Reduce friction of event bundles
- Buffer Memory Limit: Difficult to be hit by bundles alone since max buffer size
  will generally be greater max events per frame of websocket. Yet still possible
  in case of multiple exchanges passing large event bundles together. DECISION
  take backpressure on TCP/IP and miss fills or push pressure to downstream servers with
  a recovering state. Can be heavily denied by generous buffer size limits and efficient
  lookups and pops.
- K venues: min_by over K heads -> min-heap keyed by SORT_KEY.
- Event Bundle Handling: Gap detection with regular handling;
  detect replayed events on reconnect by advancing watermark and comparing last
  acknowledged seq_ids; recv_ts is framed as when the server saw
  the fills giving the replayed events an older recv_ts breaks I3.
  Assign them similar recv_ts assuming events are received together
  and sort by venue_id
- Event Bundle + Epoch: Gap Detection with epoch replayability; handle disconnects as
  epochs. Use a reference time for disconnect and reconnection timestamps.
  Dedup by venue_seq_id assign recv_ts as increasing numbers using venue_seq_id as
  ordering key. Breaks I3 for closer timestamps to when the event was created
  on venue.
```
