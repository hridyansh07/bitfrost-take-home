use std::collections::BTreeMap;
use types::{
    Fill, InstrumentSource, InstrumentSpec, Kind, Lots, Micro, Position, PositionDelta,
    PositionKey, RejectReason, Side, Ticks,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PositionRounding {
    HalfEven,
}

pub const POSITION_ROUNDING: PositionRounding = PositionRounding::HalfEven;

pub trait PositionStore {
    fn apply(&mut self, fill: &Fill) -> Result<PositionDelta, RejectReason>;
    fn position(&self, key: &PositionKey) -> Option<&Position>;
}

pub struct PositionKeeper<S> {
    instruments: S,
    positions: BTreeMap<PositionKey, Position>,
}

impl<S: InstrumentSource> PositionKeeper<S> {
    pub fn new(instruments: S) -> Self {
        Self {
            instruments,
            positions: BTreeMap::new(),
        }
    }

    pub fn apply(&mut self, fill: &Fill) -> Result<PositionDelta, RejectReason> {
        let spec = self
            .instruments
            .lookup(fill.venue, &fill.symbol)
            .ok_or_else(|| RejectReason::UnknownSymbol(fill.symbol.clone()))?;
        validate_fill(fill, spec)?;

        let key = PositionKey {
            account: fill.account.clone(),
            instrument: fill.instrument.clone(),
        };
        let before = self.positions.get(&key).cloned().unwrap_or_default();
        let delta = transition(key.clone(), before, fill, spec)?;
        self.positions.insert(key, delta.after.clone());
        Ok(delta)
    }

    pub fn position(&self, key: &PositionKey) -> Option<&Position> {
        self.positions.get(key)
    }

    pub fn positions(&self) -> &BTreeMap<PositionKey, Position> {
        &self.positions
    }

    pub fn len(&self) -> usize {
        self.positions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    pub fn into_positions(self) -> BTreeMap<PositionKey, Position> {
        self.positions
    }
}

impl<S: InstrumentSource> PositionStore for PositionKeeper<S> {
    fn apply(&mut self, fill: &Fill) -> Result<PositionDelta, RejectReason> {
        PositionKeeper::apply(self, fill)
    }

    fn position(&self, key: &PositionKey) -> Option<&Position> {
        PositionKeeper::position(self, key)
    }
}

fn transition(
    key: PositionKey,
    before: Position,
    fill: &Fill,
    spec: &InstrumentSpec,
) -> Result<PositionDelta, RejectReason> {
    validate_position(&before)?;

    let old_qty = before.net_qty.0;
    let fill_qty = fill.qty.0;
    let signed_fill_qty = match fill.side {
        Side::Buy => fill_qty,
        Side::Sell => fill_qty
            .checked_neg()
            .ok_or(RejectReason::ArithmeticOverflow)?,
    };

    let (new_qty, new_cost, closed_qty, opened_qty, realized_delta) =
        if old_qty == 0 || old_qty.signum() == signed_fill_qty.signum() {
            let new_qty = old_qty
                .checked_add(signed_fill_qty)
                .ok_or(RejectReason::ArithmeticOverflow)?;
            let added_cost = notional(fill.price, fill_qty, spec.micro_per_tick_lot)?;
            let new_cost = before
                .open_cost
                .0
                .checked_add(added_cost)
                .ok_or(RejectReason::ArithmeticOverflow)?;
            (new_qty, new_cost, 0, fill_qty, 0)
        } else {
            let old_abs = checked_abs(old_qty)?;
            let closed_qty = old_abs.min(fill_qty);
            let exit_notional = notional(fill.price, closed_qty, spec.micro_per_tick_lot)?;
            let allocated_cost = if closed_qty == old_abs {
                before.open_cost.0
            } else {
                let numerator = before
                    .open_cost
                    .0
                    .checked_mul(closed_qty as i128)
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                div_round_half_even(numerator, old_abs as i128)?
            };
            let realized_delta = if old_qty > 0 {
                exit_notional
                    .checked_sub(allocated_cost)
                    .ok_or(RejectReason::ArithmeticOverflow)?
            } else {
                allocated_cost
                    .checked_sub(exit_notional)
                    .ok_or(RejectReason::ArithmeticOverflow)?
            };
            let remaining_fill = fill_qty
                .checked_sub(closed_qty)
                .ok_or(RejectReason::ArithmeticOverflow)?;

            if closed_qty < old_abs {
                let new_qty = old_qty
                    .checked_add(signed_fill_qty)
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                let new_cost = before
                    .open_cost
                    .0
                    .checked_sub(allocated_cost)
                    .ok_or(RejectReason::ArithmeticOverflow)?;
                (new_qty, new_cost, closed_qty, 0, realized_delta)
            } else if remaining_fill == 0 {
                (0, 0, closed_qty, 0, realized_delta)
            } else {
                let new_qty = match fill.side {
                    Side::Buy => remaining_fill,
                    Side::Sell => remaining_fill
                        .checked_neg()
                        .ok_or(RejectReason::ArithmeticOverflow)?,
                };
                let new_cost = notional(fill.price, remaining_fill, spec.micro_per_tick_lot)?;
                (
                    new_qty,
                    new_cost,
                    closed_qty,
                    remaining_fill,
                    realized_delta,
                )
            }
        };

    let avg_entry_price = if new_qty == 0 {
        None
    } else {
        Some(average_entry_price(
            new_cost,
            checked_abs(new_qty)?,
            spec.micro_per_tick_lot,
        )?)
    };
    let realized_pnl = before
        .realized_pnl
        .0
        .checked_add(realized_delta)
        .ok_or(RejectReason::ArithmeticOverflow)?;
    let fees = before
        .fees
        .0
        .checked_add(fill.fee.0)
        .ok_or(RejectReason::ArithmeticOverflow)?;
    let after = Position {
        net_qty: Lots(new_qty),
        avg_entry_price,
        open_cost: Micro(new_cost),
        realized_pnl: Micro(realized_pnl),
        fees: Micro(fees),
    };
    validate_position(&after)?;

    Ok(PositionDelta {
        key,
        before,
        after,
        closed_qty: Lots(closed_qty),
        opened_qty: Lots(opened_qty),
        realized_pnl_delta: Micro(realized_delta),
        fee_delta: fill.fee,
    })
}

fn validate_fill(fill: &Fill, spec: &InstrumentSpec) -> Result<(), RejectReason> {
    if fill.venue != spec.venue || fill.kind != spec.kind || fill.instrument != spec.instrument {
        return Err(RejectReason::InvalidFill(fill.symbol.clone()));
    }
    if fill.price.0 <= 0 || fill.qty.0 <= 0 || fill.fee.0 < 0 {
        return Err(RejectReason::InvalidFill(fill.event_id.clone()));
    }
    if spec.tick_micro <= 0 || spec.lot_qty_e9 <= 0 || spec.micro_per_tick_lot <= 0 {
        return Err(RejectReason::InvalidFill(spec.symbol.clone()));
    }
    if spec.kind == Kind::Binary {
        if 1_000_000i128 % spec.tick_micro != 0 {
            return Err(RejectReason::InvalidFill(spec.symbol.clone()));
        }
        let full = i64::try_from(1_000_000i128 / spec.tick_micro)
            .map_err(|_| RejectReason::ArithmeticOverflow)?;
        let lo = full / 100;
        let hi = full - lo;
        if fill.price.0 < lo || fill.price.0 > hi {
            return Err(RejectReason::PriceOutOfRange {
                symbol: fill.symbol.clone(),
                ticks: fill.price.0,
            });
        }
    }
    Ok(())
}

fn validate_position(position: &Position) -> Result<(), RejectReason> {
    if position.fees.0 < 0 {
        return Err(RejectReason::InvalidPosition("negative fees".to_string()));
    }
    if position.net_qty.0 == 0 {
        if position.open_cost != Micro::ZERO || position.avg_entry_price.is_some() {
            return Err(RejectReason::InvalidPosition(
                "flat position has open cost".to_string(),
            ));
        }
    } else if position.open_cost.0 <= 0 || position.avg_entry_price.is_none_or(|price| price.0 <= 0)
    {
        return Err(RejectReason::InvalidPosition(
            "open position has invalid cost".to_string(),
        ));
    }
    Ok(())
}

fn notional(price: Ticks, qty: i64, micro_per_tick_lot: i128) -> Result<i128, RejectReason> {
    (price.0 as i128)
        .checked_mul(qty as i128)
        .and_then(|value| value.checked_mul(micro_per_tick_lot))
        .ok_or(RejectReason::ArithmeticOverflow)
}

fn average_entry_price(
    open_cost: i128,
    qty: i64,
    micro_per_tick_lot: i128,
) -> Result<Ticks, RejectReason> {
    let denominator = (qty as i128)
        .checked_mul(micro_per_tick_lot)
        .ok_or(RejectReason::ArithmeticOverflow)?;
    let rounded = div_round_half_even(open_cost, denominator)?;
    let ticks = i64::try_from(rounded).map_err(|_| RejectReason::ArithmeticOverflow)?;
    Ok(Ticks(ticks))
}

fn div_round_half_even(numerator: i128, denominator: i128) -> Result<i128, RejectReason> {
    if numerator < 0 || denominator <= 0 {
        return Err(RejectReason::ArithmeticOverflow);
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let doubled = remainder
        .checked_mul(2)
        .ok_or(RejectReason::ArithmeticOverflow)?;
    if doubled < denominator || (doubled == denominator && quotient % 2 == 0) {
        Ok(quotient)
    } else {
        quotient
            .checked_add(1)
            .ok_or(RejectReason::ArithmeticOverflow)
    }
}

fn checked_abs(value: i64) -> Result<i64, RejectReason> {
    value.checked_abs().ok_or(RejectReason::ArithmeticOverflow)
}

#[cfg(test)]
mod tests {
    use super::*;
    use types::{Canonicalize, PmEvent, RawEvent, Registry, Venue};

    fn fill(
        account: &str,
        instrument: (&str, Venue, Kind),
        side: Side,
        price: i64,
        qty: i64,
        fee: i128,
    ) -> Fill {
        let (symbol, venue, kind) = instrument;
        Fill {
            venue,
            event_id: format!("{account}-{symbol}-{price}-{qty}"),
            seq: 1,
            ts_ms: 1,
            account: account.to_string(),
            instrument: types::InstrumentId(symbol.to_string()),
            symbol: symbol.to_string(),
            kind,
            side,
            price: Ticks(price),
            qty: Lots(qty),
            fee: Micro(fee),
        }
    }

    fn btc(account: &str, side: Side, price: i64, qty: i64, fee: i128) -> Fill {
        fill(
            account,
            ("BTC-PERP", types::Venue::Hl, types::Kind::Perp),
            side,
            price,
            qty,
            fee,
        )
    }

    fn key(account: &str, symbol: &str) -> PositionKey {
        PositionKey {
            account: account.to_string(),
            instrument: types::InstrumentId(symbol.to_string()),
        }
    }

    #[test]
    fn opens_and_adds_with_weighted_average() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper.apply(&btc("acct", Side::Buy, 200, 2, 10)).unwrap();
        let delta = keeper.apply(&btc("acct", Side::Buy, 400, 2, 20)).unwrap();

        assert_eq!(delta.closed_qty, Lots(0));
        assert_eq!(delta.opened_qty, Lots(2));
        assert_eq!(delta.realized_pnl_delta, Micro::ZERO);
        assert_eq!(delta.after.net_qty, Lots(4));
        assert_eq!(delta.after.open_cost, Micro(60_000));
        assert_eq!(delta.after.avg_entry_price, Some(Ticks(300)));
        assert_eq!(delta.after.fees, Micro(30));
    }

    #[test]
    fn partially_closes_a_long() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper.apply(&btc("acct", Side::Buy, 200, 10, 0)).unwrap();
        let delta = keeper.apply(&btc("acct", Side::Sell, 300, 4, 0)).unwrap();

        assert_eq!(delta.closed_qty, Lots(4));
        assert_eq!(delta.opened_qty, Lots(0));
        assert_eq!(delta.realized_pnl_delta, Micro(20_000));
        assert_eq!(delta.after.net_qty, Lots(6));
        assert_eq!(delta.after.open_cost, Micro(60_000));
        assert_eq!(delta.after.avg_entry_price, Some(Ticks(200)));
    }

    #[test]
    fn partially_closes_a_short() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper.apply(&btc("acct", Side::Sell, 300, 10, 0)).unwrap();
        let delta = keeper.apply(&btc("acct", Side::Buy, 200, 4, 0)).unwrap();

        assert_eq!(delta.realized_pnl_delta, Micro(20_000));
        assert_eq!(delta.after.net_qty, Lots(-6));
        assert_eq!(delta.after.open_cost, Micro(90_000));
        assert_eq!(delta.after.avg_entry_price, Some(Ticks(300)));
    }

    #[test]
    fn crosses_zero_close_then_open() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper.apply(&btc("acct", Side::Buy, 200, 5, 0)).unwrap();
        let delta = keeper.apply(&btc("acct", Side::Sell, 300, 8, 0)).unwrap();

        assert_eq!(delta.closed_qty, Lots(5));
        assert_eq!(delta.opened_qty, Lots(3));
        assert_eq!(delta.realized_pnl_delta, Micro(25_000));
        assert_eq!(delta.after.net_qty, Lots(-3));
        assert_eq!(delta.after.open_cost, Micro(45_000));
        assert_eq!(delta.after.avg_entry_price, Some(Ticks(300)));
    }

    #[test]
    fn short_crosses_zero_close_then_open() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper.apply(&btc("acct", Side::Sell, 300, 5, 0)).unwrap();
        let delta = keeper.apply(&btc("acct", Side::Buy, 200, 8, 0)).unwrap();

        assert_eq!(delta.closed_qty, Lots(5));
        assert_eq!(delta.opened_qty, Lots(3));
        assert_eq!(delta.realized_pnl_delta, Micro(25_000));
        assert_eq!(delta.after.net_qty, Lots(3));
        assert_eq!(delta.after.open_cost, Micro(30_000));
        assert_eq!(delta.after.avg_entry_price, Some(Ticks(200)));
    }

    #[test]
    fn full_close_clears_cost_and_average() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper.apply(&btc("acct", Side::Buy, 200, 5, 5)).unwrap();
        let delta = keeper.apply(&btc("acct", Side::Sell, 200, 5, 7)).unwrap();

        assert_eq!(delta.after.net_qty, Lots(0));
        assert_eq!(delta.after.open_cost, Micro::ZERO);
        assert_eq!(delta.after.avg_entry_price, None);
        assert_eq!(delta.after.realized_pnl, Micro::ZERO);
        assert_eq!(delta.after.fees, Micro(12));
    }

    #[test]
    fn applies_binary_positions_with_the_same_integer_math() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        let open = fill(
            "acct",
            ("FED-CUT-SEP", types::Venue::Pm, types::Kind::Binary),
            Side::Sell,
            6_350,
            400,
            508_000,
        );
        let delta = keeper.apply(&open).unwrap();

        assert_eq!(delta.after.net_qty, Lots(-400));
        assert_eq!(delta.after.open_cost, Micro(254_000_000));
        assert_eq!(delta.after.avg_entry_price, Some(Ticks(6_350)));
        assert_eq!(delta.after.fees, Micro(508_000));
    }

    #[test]
    fn pm_no_lifecycle_realizes_negative_yes_pnl() {
        let registry = Registry::standard();
        let open = PmEvent {
            sequence: 1,
            id: "open-no".to_string(),
            timestamp_ms: 1,
            user: "acct".to_string(),
            market: "FED-CUT-SEP".to_string(),
            outcome: "NO".to_string(),
            action: "BUY".to_string(),
            price: 0.30,
            size: 100,
            fee_bps: 0,
        }
        .canonicalize(&registry)
        .unwrap();
        let close = PmEvent {
            sequence: 2,
            id: "close-no".to_string(),
            timestamp_ms: 2,
            user: "acct".to_string(),
            market: "FED-CUT-SEP".to_string(),
            outcome: "NO".to_string(),
            action: "SELL".to_string(),
            price: 0.40,
            size: 100,
            fee_bps: 0,
        }
        .canonicalize(&registry)
        .unwrap();
        let mut keeper = PositionKeeper::new(Registry::standard());

        keeper.apply(&open).unwrap();
        let delta = keeper.apply(&close).unwrap();

        assert_eq!(delta.after.net_qty, Lots(0));
        assert_eq!(delta.after.realized_pnl, Micro(10_000_000));
        assert_eq!(delta.after.open_cost, Micro::ZERO);
    }

    #[test]
    fn rejects_binary_price_outside_payout_bounds_atomically() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        let invalid = fill(
            "acct",
            ("FED-CUT-SEP", types::Venue::Pm, types::Kind::Binary),
            Side::Buy,
            10_000,
            1,
            0,
        );

        assert_eq!(
            keeper.apply(&invalid),
            Err(RejectReason::PriceOutOfRange {
                symbol: "FED-CUT-SEP".to_string(),
                ticks: 10_000,
            })
        );
        assert!(keeper.is_empty());
    }

    #[test]
    fn applies_half_even_rounding_to_average_and_partial_basis() {
        let registry = Registry::from_specs([InstrumentSpec {
            instrument: types::InstrumentId("ROUNDING".to_string()),
            symbol: "ROUNDING".to_string(),
            venue: types::Venue::Hl,
            kind: types::Kind::Perp,
            tick_micro: 1,
            lot_qty_e9: 1_000_000_000,
            micro_per_tick_lot: 0,
        }]);
        let mut keeper = PositionKeeper::new(registry);
        let first = fill(
            "acct",
            ("ROUNDING", types::Venue::Hl, types::Kind::Perp),
            Side::Buy,
            1,
            1,
            0,
        );
        let second = fill(
            "acct",
            ("ROUNDING", types::Venue::Hl, types::Kind::Perp),
            Side::Buy,
            2,
            1,
            0,
        );
        let close = fill(
            "acct",
            ("ROUNDING", types::Venue::Hl, types::Kind::Perp),
            Side::Sell,
            2,
            1,
            0,
        );

        keeper.apply(&first).unwrap();
        let added = keeper.apply(&second).unwrap();
        assert_eq!(POSITION_ROUNDING, PositionRounding::HalfEven);
        assert_eq!(added.after.open_cost, Micro(3));
        assert_eq!(added.after.avg_entry_price, Some(Ticks(2)));

        let reduced = keeper.apply(&close).unwrap();
        assert_eq!(reduced.realized_pnl_delta, Micro::ZERO);
        assert_eq!(reduced.after.open_cost, Micro(1));
        assert_eq!(reduced.after.avg_entry_price, Some(Ticks(1)));
    }

    #[test]
    fn rejection_leaves_state_unchanged() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper.apply(&btc("acct", Side::Buy, 200, 5, 0)).unwrap();
        let before = keeper.positions().clone();
        let unknown = fill(
            "acct",
            ("DOGE-PERP", types::Venue::Hl, types::Kind::Perp),
            Side::Buy,
            1,
            1,
            0,
        );

        assert_eq!(
            keeper.apply(&unknown),
            Err(RejectReason::UnknownSymbol("DOGE-PERP".to_string()))
        );
        assert_eq!(keeper.positions(), &before);
    }

    #[test]
    fn overflow_leaves_state_unchanged() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper.apply(&btc("acct", Side::Buy, 200, 5, 0)).unwrap();
        let before = keeper.positions().clone();
        let overflow = btc("acct", Side::Buy, i64::MAX, i64::MAX, 0);

        assert_eq!(
            keeper.apply(&overflow),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(keeper.positions(), &before);
    }

    #[test]
    fn notional_overflow_leaves_empty_state_unchanged() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        let overflow = btc("acct", Side::Buy, i64::MAX, i64::MAX, 0);

        assert_eq!(
            keeper.apply(&overflow),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert!(keeper.is_empty());
    }

    #[test]
    fn fee_overflow_leaves_state_unchanged() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper
            .apply(&btc("acct", Side::Buy, 200, 1, i128::MAX))
            .unwrap();
        let before = keeper.positions().clone();

        assert_eq!(
            keeper.apply(&btc("acct", Side::Buy, 200, 1, 1)),
            Err(RejectReason::ArithmeticOverflow)
        );
        assert_eq!(keeper.positions(), &before);
    }

    #[test]
    fn matched_fixture_conserves_realized_pnl_and_fees() {
        let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures");
        let lines = types::read_hl(&fixtures.join("matched.ndjson")).unwrap();
        let registry = Registry::standard();
        let mut keeper = PositionKeeper::new(Registry::standard());

        for line in lines {
            let RawEvent::Hl(event) = line.parsed.unwrap() else {
                panic!("unexpected venue");
            };
            let fill = event.canonicalize(&registry).unwrap();
            keeper.apply(&fill).unwrap();
        }

        let manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(fixtures.join("manifest.json")).unwrap())
                .unwrap();
        let expected_fees = manifest["matched"]["sum_fees_micro"].as_i64().unwrap() as i128;
        let realized = keeper
            .positions()
            .values()
            .map(|position| position.realized_pnl.0)
            .sum::<i128>();
        let fees = keeper
            .positions()
            .values()
            .map(|position| position.fees.0)
            .sum::<i128>();

        assert_eq!(realized, 0);
        assert_eq!(fees, expected_fees);
        assert!(keeper
            .positions()
            .values()
            .all(|position| position.net_qty == Lots(0)));
    }

    #[test]
    fn positions_iterate_in_canonical_key_order() {
        let mut keeper = PositionKeeper::new(Registry::standard());
        keeper
            .apply(&btc("z-account", Side::Buy, 200, 1, 0))
            .unwrap();
        keeper
            .apply(&btc("a-account", Side::Buy, 200, 1, 0))
            .unwrap();

        let keys = keeper.positions().keys().cloned().collect::<Vec<_>>();
        assert_eq!(
            keys,
            vec![key("a-account", "BTC-PERP"), key("z-account", "BTC-PERP")]
        );
    }

    #[test]
    fn keys_positions_by_canonical_instrument() {
        let registry = Registry::from_specs([InstrumentSpec {
            instrument: types::InstrumentId("CANONICAL-PERP".to_string()),
            symbol: "VENUE-ALIAS".to_string(),
            venue: types::Venue::Hl,
            kind: types::Kind::Perp,
            tick_micro: 500_000,
            lot_qty_e9: 100_000,
            micro_per_tick_lot: 0,
        }]);
        let mut keeper = PositionKeeper::new(registry);
        let mut fill = fill(
            "acct",
            ("VENUE-ALIAS", types::Venue::Hl, types::Kind::Perp),
            Side::Buy,
            200,
            1,
            0,
        );
        fill.instrument = types::InstrumentId("CANONICAL-PERP".to_string());

        keeper.apply(&fill).unwrap();

        assert!(keeper
            .position(&PositionKey {
                account: "acct".to_string(),
                instrument: types::InstrumentId("CANONICAL-PERP".to_string()),
            })
            .is_some());
    }

    #[test]
    fn half_even_rounding_is_deterministic() {
        assert_eq!(div_round_half_even(3, 2), Ok(2));
        assert_eq!(div_round_half_even(5, 2), Ok(2));
        assert_eq!(div_round_half_even(7, 2), Ok(4));
        assert_eq!(div_round_half_even(8, 3), Ok(3));
    }
}
