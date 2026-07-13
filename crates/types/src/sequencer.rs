//! The sequencer's shared shapes — the canonical tape entry and the producer-discipline
//! errors. The algorithm itself lives in `ingester/src/sequencer.rs`.

use crate::canonical::Fill;
use crate::instrument::Venue;

/// A [`Fill`] with its canonical position on the single-writer tape.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequencedFill {
    pub global_seq: u64,
    pub recv_ts: u64,
    pub fill: Fill,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SequencerError {
    /// I5: `flush` ended the stream; a sealed sequencer refuses new pushes so the published
    /// tape can never grow a walked-back stamp.
    Sealed { venue: Venue },
    /// The caller tried to push a fill onto a lane that does not match the fill's venue.
    VenueMismatch { lane: Venue, fill: Venue },
    /// I6: a producer tried to stamp an event behind its own frontier.
    RecvTsWalkback {
        venue: Venue,
        frontier: u64,
        recv_ts: u64,
    },
    /// I1 precondition: a producer delivered venue_seq out of order (gaps are fine, walk-backs
    /// and repeats are not — dedup upstream owns repeats).
    SeqWalkback {
        venue: Venue,
        last_seq: u64,
        seq: u64,
    },
}
