# Deterministic Single-Server Multi-Stream Ordering Algorithm

## Problem:

Ingesting Multiple Websocket Streams generates events that are hard to organize.
3rd Party clock-skews or delay in ordering can leave us vulnerable to skews in account
management. Fills that have been registered if consumed in incorrect order can lead to stale account info. Manage incoming events and devise a way to handle events in a manner which is replayable and deterministic for all incoming event data sets. 

## Proposed Solution: 

Frame of Reference: We need a single common variable to be able to rank and order incoming events. This refrence is stricly when the event is received by the server. It ignores exchange timestamps for local gurantees on when the event was seen by the server. 

Assumption: we assume no network faults hence no epoch/gap handling for events that are missed and batch pushed on reconnection. All references are made from when the server receives/acks the event. No venue can give events that are stale in reference to an already recieved event i.e. events cannot walk back.  

Algorithm: Incoming events are stamped with a local reference time which is when the event is received by our server. Events are appended to a per-venue FIFO buffer; single producer + monotonic increasing timestamp means each buffer is sorted by construction. A merger thread performs a K-way min-merge over the K buffer heads. This thread is also responsible for attaching a monotonic increasing global_id which can be used to order arbitary events moving forward. Essentially ordering everything. 

Events are ordered in decreasing priority by: 

    Local Reference Time | Venue | venue_seq


Trade Offs:

- Deterministic gurantees for ordering are bought by latency on the merging thread
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
// Equal recv_ts is EXPECTED dependent on refrence time clock granuality and event rate
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
    

    event = cannonicalize(raw_event)
    event.recv_ts = t

    // staleness guard 
    if |current_ts() - event.exchange_ts| > MAX_STALENESS:
        event.stale = true      // Stale Event Flag Only. Deffered Decision 

    // dedup 
    if seen.get(event.venue_seq)   
        drop                      

    // Ideally use for gap detection in events currently not fixed hence commented out 
    <!-- state.last_seq = max(state.last_seq, event.venue_seq)  -->

    // Ideally buffer bounds never hit but event bundles are unpredicatable 
    // and this push fails due to lack of memory 
    // refer #Extension Points on ideas of how to deal with this

    buffer.push(event)     

    state.frontier.store(t) // Advance watermark 
```

## Merger task (single thread, owns output order)

```
global_seq = store.load(global_seq) // Persisted Storage 

loop:
    // Invariant no stream prodcues an event where recv_ts < watermark 
    watermark = min(state.frontier_A.load(), state.frontier_B.load())

    loop:
        head = min_by(SORT_KEY, ring_A.peek(), ring_B.peek())
        // Buffer is sorted with a single producer so global minimum is always at 
        // one of the K heads where K is the number of venues
        // Viability check is O(K) TOTAL, independent of buffer depth.

        if head exists and head.recv_ts <= watermark:
            e = pop(head.buffer)               // O(1)
            global_seq += 1
            e.global_seq = global_seq.       // Global deterministic replay
            publish(e)                      
        else:
            // head > watermark proves NOTHING in any ring is emittable:
            break
```

## Structural Gurantees

```
Buffer Occupancy       Fixed by arrival_rate * T_IDLE (test and enforce) 
Structural latency   ~ inter-arrival gap of the slower stream
                       (bounded above by T_IDLE when a venue is silent)
Per-event merge cost   O(1) for K=2; O(log K) via ring-buffers for K venues  
```

## Invariants (Pending Test)

```
I1  Per-stream order:   output order of venue v's events == venue_seq order
I2  Determinism:        event order is a pure function of buffer and SORT_KEY
I3  Monotonic output:   global_seq strictly increasing; recv_ts non decreasing
I4  Liveness:           frontier advances every T_IDLE even on a silent
                        stream; a dead venue never freezes the watermark
                        beyond T_IDLE 
I5  Immutability:       published history never rewritten; completely replayable
I6  Watermark safety:   no event is ever pushed with
                        recv_ts < its venue's frontier at push time
I7  Key totality:       SORT_KEY is unique across all events; recv_ts never 
                        produces ambiguity
I8 No silent drops:     staleness NEVER drops an event, only flags it;
                        the only drop path is dedup; 
                        (Not enforced since stale event handling is not clear )
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
- Event Bundle + Epoch: Gap Detection with epoch replaybility; handle disconnects as 
  epochs. Use a reference time for disconnect and reconnection timestamps. 
  Dedup by venue_seq_id assign recv_ts as increasing numbers using venue_seq_id as 
  ordering key. Breaks I3 for closer timestamps to when the event was created
  on venue.  
```