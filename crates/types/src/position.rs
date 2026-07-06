use crate::{InstrumentId, Lots, Micro, Ticks};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct PositionKey {
    pub account: String,
    pub instrument: InstrumentId,
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct Position {
    pub net_qty: Lots,
    pub avg_entry_price: Option<Ticks>,
    pub open_cost: Micro,
    pub realized_pnl: Micro,
    pub fees: Micro,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct PositionDelta {
    pub key: PositionKey,
    pub before: Position,
    pub after: Position,
    pub closed_qty: Lots,
    pub opened_qty: Lots,
    pub realized_pnl_delta: Micro,
    pub fee_delta: Micro,
}
