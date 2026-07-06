use crate::fixed::{Lots, Micro, Ticks};
use crate::instrument::{InstrumentId, Kind, Venue};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn flip(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Fill {
    pub venue: Venue,
    pub event_id: String,
    pub seq: u64,
    pub ts_ms: u64,
    pub account: String,
    pub instrument: InstrumentId,
    pub symbol: String,
    pub kind: Kind,
    pub side: Side,
    pub price: Ticks,
    pub qty: Lots,
    pub fee: Micro,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RejectReason {
    UnknownSymbol(String),
    OffTick { symbol: String, raw: String },
    OffLot { symbol: String, raw: String },
    PriceOutOfRange { symbol: String, ticks: i64 },
    MalformedDecimal(String),
    UnknownSide(String),
    UnknownOutcome(String),
    InvalidQuantity(String),
    InvalidFee(String),
    InvalidFill(String),
    InvalidPosition(String),
    ArithmeticOverflow,
}
