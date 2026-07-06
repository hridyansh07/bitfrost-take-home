use crate::boundary::{HlEvent, PmEvent};
use crate::canonical::{Fill, RejectReason, Side};
use crate::fixed::{Lots, Micro, Ticks};
use crate::instrument::{InstrumentSource, Venue};
use crate::stream::RawEvent;

pub trait Canonicalize {
    fn canonicalize<S: InstrumentSource>(self, instruments: &S) -> Result<Fill, RejectReason>;
}

impl Canonicalize for RawEvent {
    fn canonicalize<S: InstrumentSource>(self, instruments: &S) -> Result<Fill, RejectReason> {
        match self {
            RawEvent::Hl(event) => event.canonicalize(instruments),
            RawEvent::Pm(event) => event.canonicalize(instruments),
        }
    }
}

pub(crate) fn parse_fixed(s: &str, scale: u32) -> Result<i128, RejectReason> {
    let t = s.trim();
    if t.is_empty() {
        return Err(RejectReason::MalformedDecimal(s.to_string()));
    }

    let (neg, body) = match t.strip_prefix('-') {
        Some(body) => (true, body),
        None => (false, t),
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some(parts) => parts,
        None => (body, ""),
    };

    if int_part.is_empty() && frac_part.is_empty() {
        return Err(RejectReason::MalformedDecimal(s.to_string()));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(RejectReason::MalformedDecimal(s.to_string()));
    }

    let scale = scale as usize;
    let frac_part = if frac_part.len() > scale {
        let (kept, overflow) = frac_part.split_at(scale);
        if !overflow.bytes().all(|b| b == b'0') {
            return Err(RejectReason::MalformedDecimal(s.to_string()));
        }
        kept
    } else {
        frac_part
    };

    let mut value = 0i128;
    for digit in int_part.bytes().chain(frac_part.bytes()) {
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add((digit - b'0') as i128))
            .ok_or_else(|| RejectReason::MalformedDecimal(s.to_string()))?;
    }
    for _ in frac_part.len()..scale {
        value = value
            .checked_mul(10)
            .ok_or_else(|| RejectReason::MalformedDecimal(s.to_string()))?;
    }

    if neg {
        value = value
            .checked_neg()
            .ok_or_else(|| RejectReason::MalformedDecimal(s.to_string()))?;
    }
    Ok(value)
}

pub fn price_to_ticks(price: f64, tick_micro: i128, symbol: &str) -> Result<i64, RejectReason> {
    if !price.is_finite() || price <= 0.0 || tick_micro <= 0 {
        return Err(RejectReason::MalformedDecimal(format!("{price}")));
    }
    let scaled = price * 1_000_000.0 / tick_micro as f64;
    let ticks = scaled.round();
    if !ticks.is_finite() {
        return Err(RejectReason::MalformedDecimal(format!("{price}")));
    }
    let tolerance = f64::EPSILON * scaled.abs().max(1.0) * 8.0;
    if (scaled - ticks).abs() > tolerance {
        return Err(RejectReason::OffTick {
            symbol: symbol.to_string(),
            raw: price.to_string(),
        });
    }
    if ticks >= 9_223_372_036_854_775_808.0 {
        return Err(RejectReason::ArithmeticOverflow);
    }
    Ok(ticks as i64)
}

impl Canonicalize for HlEvent {
    fn canonicalize<S: InstrumentSource>(self, instruments: &S) -> Result<Fill, RejectReason> {
        let spec = instruments
            .lookup(Venue::Hl, &self.symbol)
            .ok_or_else(|| RejectReason::UnknownSymbol(self.symbol.clone()))?;
        if spec.venue != Venue::Hl {
            return Err(RejectReason::UnknownSymbol(self.symbol.clone()));
        }
        if spec.tick_micro <= 0 || spec.lot_qty_e9 <= 0 || spec.micro_per_tick_lot <= 0 {
            return Err(RejectReason::InvalidFill(self.symbol.clone()));
        }
        let side = match self.side.as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            other => return Err(RejectReason::UnknownSide(other.to_string())),
        };

        let px_micro = parse_fixed(&self.px, 6)?;
        if px_micro <= 0 {
            return Err(RejectReason::MalformedDecimal(self.px.clone()));
        }
        if px_micro % spec.tick_micro != 0 {
            return Err(RejectReason::OffTick {
                symbol: self.symbol.clone(),
                raw: self.px.clone(),
            });
        }
        let price = Ticks(
            i64::try_from(px_micro / spec.tick_micro)
                .map_err(|_| RejectReason::ArithmeticOverflow)?,
        );

        let qty_e9 = parse_fixed(&self.qty, 9)?;
        if qty_e9 <= 0 {
            return Err(RejectReason::InvalidQuantity(self.qty.clone()));
        }
        if qty_e9 % spec.lot_qty_e9 != 0 {
            return Err(RejectReason::OffLot {
                symbol: self.symbol.clone(),
                raw: self.qty.clone(),
            });
        }
        let qty = Lots(
            i64::try_from(qty_e9 / spec.lot_qty_e9)
                .map_err(|_| RejectReason::ArithmeticOverflow)?,
        );

        let fee_micro = parse_fixed(&self.fee, 6)?;
        if fee_micro < 0 {
            return Err(RejectReason::MalformedDecimal(self.fee.clone()));
        }
        let kind = spec.kind;
        let instrument = spec.instrument.clone();

        Ok(Fill {
            venue: Venue::Hl,
            event_id: self.event_id,
            seq: self.seq,
            ts_ms: self.ts,
            account: self.account,
            instrument,
            symbol: self.symbol,
            kind,
            side,
            price,
            qty,
            fee: Micro(fee_micro),
        })
    }
}

impl Canonicalize for PmEvent {
    fn canonicalize<S: InstrumentSource>(self, instruments: &S) -> Result<Fill, RejectReason> {
        let spec = instruments
            .lookup(Venue::Pm, &self.market)
            .ok_or_else(|| RejectReason::UnknownSymbol(self.market.clone()))?;
        if spec.venue != Venue::Pm {
            return Err(RejectReason::UnknownSymbol(self.market.clone()));
        }
        if spec.tick_micro <= 0 || spec.lot_qty_e9 <= 0 || spec.micro_per_tick_lot <= 0 {
            return Err(RejectReason::InvalidFill(self.market.clone()));
        }
        let action = match self.action.as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            other => return Err(RejectReason::UnknownSide(other.to_string())),
        };
        if self.size <= 0 {
            return Err(RejectReason::InvalidQuantity(self.size.to_string()));
        }
        if self.fee_bps < 0 {
            return Err(RejectReason::InvalidFee(self.fee_bps.to_string()));
        }

        let raw_ticks = price_to_ticks(self.price, spec.tick_micro, &self.market)?;
        let full = i64::try_from(1_000_000i128 / spec.tick_micro)
            .map_err(|_| RejectReason::ArithmeticOverflow)?;
        let lo = full / 100;
        let hi = full - lo;
        if raw_ticks < lo || raw_ticks > hi {
            return Err(RejectReason::PriceOutOfRange {
                symbol: self.market.clone(),
                ticks: raw_ticks,
            });
        }

        let notional_micro = (raw_ticks as i128)
            .checked_mul(self.size as i128)
            .and_then(|value| value.checked_mul(spec.micro_per_tick_lot))
            .ok_or(RejectReason::ArithmeticOverflow)?;
        let fee_micro = notional_micro
            .checked_mul(self.fee_bps as i128)
            .and_then(|value| value.checked_add(5_000))
            .ok_or(RejectReason::ArithmeticOverflow)?
            / 10_000;
        let (side, price) = match self.outcome.as_str() {
            "YES" => (action, Ticks(raw_ticks)),
            "NO" => (action.flip(), Ticks(full - raw_ticks)),
            other => return Err(RejectReason::UnknownOutcome(other.to_string())),
        };
        let kind = spec.kind;
        let instrument = spec.instrument.clone();

        Ok(Fill {
            venue: Venue::Pm,
            event_id: self.id,
            seq: self.sequence,
            ts_ms: self.timestamp_ms,
            account: self.user,
            instrument,
            symbol: self.market,
            kind,
            side,
            price,
            qty: Lots(self.size),
            fee: Micro(fee_micro),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_fixed, Canonicalize};
    use crate::{
        HlEvent, InstrumentSource, Lots, Micro, PmEvent, Registry, RejectReason, Side, Ticks, Venue,
    };

    fn hl_event(px: &str, qty: &str) -> HlEvent {
        HlEvent {
            seq: 1,
            event_id: "hl-1".to_string(),
            ts: 2,
            account: "account-1".to_string(),
            symbol: "BTC-PERP".to_string(),
            side: "buy".to_string(),
            px: px.to_string(),
            qty: qty.to_string(),
            fee: "0.0202".to_string(),
        }
    }

    fn pm_event(outcome: &str, action: &str, size: i64, fee_bps: i64) -> PmEvent {
        PmEvent {
            sequence: 3,
            id: "pm-1".to_string(),
            timestamp_ms: 4,
            user: "account-2".to_string(),
            market: "FED-CUT-SEP".to_string(),
            outcome: outcome.to_string(),
            action: action.to_string(),
            price: 0.6350,
            size,
            fee_bps,
        }
    }

    #[test]
    fn parses_fixed_decimals() {
        assert_eq!(parse_fixed("1", 6), Ok(1_000_000));
        assert_eq!(parse_fixed("0.5", 6), Ok(500_000));
        assert_eq!(parse_fixed("-0.02", 6), Ok(-20_000));
        assert_eq!(parse_fixed(".5", 6), Ok(500_000));
        assert_eq!(parse_fixed("5.", 6), Ok(5_000_000));
        assert_eq!(
            parse_fixed("abc", 6),
            Err(RejectReason::MalformedDecimal("abc".to_string()))
        );
        assert_eq!(
            parse_fixed("", 6),
            Err(RejectReason::MalformedDecimal("".to_string()))
        );
    }

    #[test]
    fn flips_side_round_trip() {
        assert_eq!(Side::Buy.flip(), Side::Sell);
        assert_eq!(Side::Buy.flip().flip(), Side::Buy);
        assert_eq!(Side::Sell.flip().flip(), Side::Sell);
    }

    #[test]
    fn standard_registry_has_expected_instruments_and_derived_values() {
        let registry = Registry::standard();
        assert_eq!(
            registry
                .lookup(Venue::Hl, "BTC-PERP")
                .unwrap()
                .micro_per_tick_lot,
            50
        );
        assert_eq!(
            registry
                .lookup(Venue::Hl, "ETH-PERP")
                .unwrap()
                .micro_per_tick_lot,
            50
        );
        assert_eq!(
            registry
                .lookup(Venue::Pm, "FED-CUT-SEP")
                .unwrap()
                .micro_per_tick_lot,
            100
        );
        assert_eq!(
            registry
                .lookup(Venue::Pm, "CPI-ABOVE-AUG")
                .unwrap()
                .micro_per_tick_lot,
            100
        );
        assert!(registry.lookup(Venue::Hl, "DOGE-PERP").is_none());
    }

    #[test]
    fn canonicalizes_hl_fill() {
        let fill = hl_event("67412.50", "0.0030")
            .canonicalize(&Registry::standard())
            .unwrap();
        assert_eq!(fill.venue, Venue::Hl);
        assert_eq!(fill.price, Ticks(134_825));
        assert_eq!(fill.qty, Lots(30));
        assert_eq!(fill.fee, Micro(20_200));
        assert_eq!(fill.instrument.0, "BTC-PERP");
    }

    #[test]
    fn rejects_hl_off_tick() {
        assert_eq!(
            hl_event("67412.30", "0.0030").canonicalize(&Registry::standard()),
            Err(RejectReason::OffTick {
                symbol: "BTC-PERP".to_string(),
                raw: "67412.30".to_string(),
            })
        );
    }

    #[test]
    fn rejects_hl_off_lot() {
        assert_eq!(
            hl_event("67412.50", "0.00305").canonicalize(&Registry::standard()),
            Err(RejectReason::OffLot {
                symbol: "BTC-PERP".to_string(),
                raw: "0.00305".to_string(),
            })
        );
    }

    #[test]
    fn canonicalizes_pm_yes_fill_and_fee() {
        let fill = pm_event("YES", "SELL", 400, 20)
            .canonicalize(&Registry::standard())
            .unwrap();
        assert_eq!(fill.price, Ticks(6_350));
        assert_eq!(fill.side, Side::Sell);
        assert_eq!(fill.qty, Lots(400));
        assert_eq!(fill.fee, Micro(508_000));
    }

    #[test]
    fn canonicalizes_pm_no_as_negative_yes() {
        let mut event = pm_event("NO", "BUY", 100, 0);
        event.price = 0.30;
        let fill = event.canonicalize(&Registry::standard()).unwrap();
        assert_eq!(fill.price, Ticks(7_000));
        assert_eq!(fill.side, Side::Sell);
    }

    #[test]
    fn rejects_pm_price_out_of_range() {
        let mut event = pm_event("YES", "BUY", 1, 0);
        event.price = 0.005;
        assert_eq!(
            event.canonicalize(&Registry::standard()),
            Err(RejectReason::PriceOutOfRange {
                symbol: "FED-CUT-SEP".to_string(),
                ticks: 50,
            })
        );
    }

    #[test]
    fn rejects_pm_price_between_ticks() {
        let mut event = pm_event("YES", "BUY", 1, 0);
        event.price = 0.63505;
        assert_eq!(
            event.canonicalize(&Registry::standard()),
            Err(RejectReason::OffTick {
                symbol: "FED-CUT-SEP".to_string(),
                raw: "0.63505".to_string(),
            })
        );
    }

    #[test]
    fn accepts_pm_tick_with_float_representation_noise() {
        let mut event = pm_event("YES", "BUY", 1, 0);
        event.price = 0.6350000000000001;
        let fill = event.canonicalize(&Registry::standard()).unwrap();
        assert_eq!(fill.price, Ticks(6_350));
    }

    #[test]
    fn rejects_hl_price_that_exceeds_tick_storage() {
        assert_eq!(
            hl_event("9223372036854775808.000000", "0.0001").canonicalize(&Registry::standard()),
            Err(RejectReason::ArithmeticOverflow)
        );
    }

    #[test]
    fn rejects_hl_quantity_that_exceeds_lot_storage() {
        assert_eq!(
            hl_event("1.0", "922337203685477.5808").canonicalize(&Registry::standard()),
            Err(RejectReason::ArithmeticOverflow)
        );
    }

    #[test]
    fn rejects_negative_pm_fee_rate() {
        assert_eq!(
            pm_event("YES", "BUY", 1, -1).canonicalize(&Registry::standard()),
            Err(RejectReason::InvalidFee("-1".to_string()))
        );
    }

    #[test]
    fn rejects_pm_fee_overflow() {
        assert_eq!(
            pm_event("YES", "BUY", i64::MAX, i64::MAX).canonicalize(&Registry::standard()),
            Err(RejectReason::ArithmeticOverflow)
        );
    }

    #[test]
    fn rejects_unknown_symbol() {
        let mut event = hl_event("67412.50", "0.0030");
        event.symbol = "DOGE-PERP".to_string();
        assert_eq!(
            event.canonicalize(&Registry::standard()),
            Err(RejectReason::UnknownSymbol("DOGE-PERP".to_string()))
        );
    }
}
