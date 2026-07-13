//! The ingest-pipeline vocabulary: outcomes, dead letters, alerts, stats, and the canonical
//! tape entry. Behavior lives in the `ingester` crate; the shared shapes live here.

use crate::canonical::RejectReason;
use crate::instrument::Venue;
use crate::sequencer::SequencerError;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IngestOutcome {
    Accepted,
    Duplicate,
    DeadLettered(DeadLetterReason),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeadLetterReason {
    Malformed(String),
    Rejected(RejectReason),
    Byzantine,
    /// The producer violated the in-order delivery assumption (venue_seq or recv_ts walked
    /// back); the event is quarantined, never silently dropped.
    OutOfOrder(SequencerError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeadLetter {
    pub venue: Venue,
    pub event_id: String,
    pub reason: DeadLetterReason,
    pub text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Alert {
    pub venue: Venue,
    pub event_id: String,
    pub kind: AlertKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlertKind {
    ByzantineDuplicate,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IngestStats {
    pub accepted: usize,
    pub duplicates: usize,
    pub byzantine: usize,
    pub dead_lettered: usize,
    pub out_of_order: usize,
    pub gaps: usize,
}
