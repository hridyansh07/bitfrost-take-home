use std::collections::BTreeMap;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Venue {
    Hl,
    Pm,
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Kind {
    Perp,
    Binary,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct InstrumentId(pub String);

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct InstrumentSpec {
    pub instrument: InstrumentId,
    pub symbol: String,
    pub venue: Venue,
    pub kind: Kind,
    pub tick_micro: i128,
    pub lot_qty_e9: i128,
    pub micro_per_tick_lot: i128,
}

pub trait InstrumentSource {
    fn lookup(&self, venue: Venue, symbol: &str) -> Option<&InstrumentSpec>;
}

pub struct Registry {
    by_symbol: BTreeMap<(Venue, String), InstrumentSpec>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RegistryError {
    DuplicateSymbol(String),
    InvalidSpec(String),
    ArithmeticOverflow(String),
    InconsistentInstrument(String),
}

/// Wiring this up this way because According to the spec symbols should be known
/// Ideally this can also use the universe and markets public endpoints to get the list of available symbols
///
impl Registry {
    pub fn from_specs(specs: impl IntoIterator<Item = InstrumentSpec>) -> Registry {
        Registry::try_from_specs(specs).expect("invalid instrument registry")
    }

    pub fn try_from_specs(
        specs: impl IntoIterator<Item = InstrumentSpec>,
    ) -> Result<Registry, RegistryError> {
        let mut by_symbol = BTreeMap::new();
        let mut by_instrument = BTreeMap::<InstrumentId, InstrumentSpec>::new();
        for mut spec in specs {
            if spec.tick_micro <= 0 || spec.lot_qty_e9 <= 0 {
                return Err(RegistryError::InvalidSpec(spec.symbol));
            }
            match (spec.venue, spec.kind) {
                (Venue::Hl, Kind::Perp) => {}
                (Venue::Pm, Kind::Binary) => {
                    if spec.lot_qty_e9 != 1_000_000_000
                        || 1_000_000 % spec.tick_micro != 0
                        || (1_000_000 / spec.tick_micro) % 100 != 0
                    {
                        return Err(RegistryError::InvalidSpec(spec.symbol));
                    }
                }
                _ => return Err(RegistryError::InvalidSpec(spec.symbol)),
            }
            let product = spec
                .tick_micro
                .checked_mul(spec.lot_qty_e9)
                .ok_or_else(|| RegistryError::ArithmeticOverflow(spec.symbol.clone()))?;
            if product % 1_000_000_000 != 0 {
                return Err(RegistryError::InvalidSpec(spec.symbol));
            }
            spec.micro_per_tick_lot = product / 1_000_000_000;
            if spec.micro_per_tick_lot <= 0 {
                return Err(RegistryError::InvalidSpec(spec.symbol));
            }
            let key = (spec.venue, spec.symbol.clone());
            if by_symbol.contains_key(&key) {
                return Err(RegistryError::DuplicateSymbol(spec.symbol));
            }
            // Construction-time consistency check only: the same canonical instrument must
            // carry identical economics wherever it appears.
            if let Some(existing) = by_instrument.get(&spec.instrument) {
                if existing.kind != spec.kind
                    || existing.tick_micro != spec.tick_micro
                    || existing.lot_qty_e9 != spec.lot_qty_e9
                    || existing.micro_per_tick_lot != spec.micro_per_tick_lot
                {
                    return Err(RegistryError::InconsistentInstrument(spec.instrument.0));
                }
            } else {
                by_instrument.insert(spec.instrument.clone(), spec.clone());
            }
            by_symbol.insert(key, spec);
        }
        Ok(Registry { by_symbol })
    }

    pub fn standard() -> Registry {
        Registry::from_specs([
            InstrumentSpec {
                instrument: InstrumentId("BTC-PERP".to_string()),
                symbol: "BTC-PERP".to_string(),
                venue: Venue::Hl,
                kind: Kind::Perp,
                tick_micro: 500_000,
                lot_qty_e9: 100_000,
                micro_per_tick_lot: 0,
            },
            InstrumentSpec {
                instrument: InstrumentId("ETH-PERP".to_string()),
                symbol: "ETH-PERP".to_string(),
                venue: Venue::Hl,
                kind: Kind::Perp,
                tick_micro: 50_000,
                lot_qty_e9: 1_000_000,
                micro_per_tick_lot: 0,
            },
            InstrumentSpec {
                instrument: InstrumentId("FED-CUT-SEP".to_string()),
                symbol: "FED-CUT-SEP".to_string(),
                venue: Venue::Pm,
                kind: Kind::Binary,
                tick_micro: 100,
                lot_qty_e9: 1_000_000_000,
                micro_per_tick_lot: 0,
            },
            InstrumentSpec {
                instrument: InstrumentId("CPI-ABOVE-AUG".to_string()),
                symbol: "CPI-ABOVE-AUG".to_string(),
                venue: Venue::Pm,
                kind: Kind::Binary,
                tick_micro: 100,
                lot_qty_e9: 1_000_000_000,
                micro_per_tick_lot: 0,
            },
        ])
    }
}

impl InstrumentSource for Registry {
    fn lookup(&self, venue: Venue, symbol: &str) -> Option<&InstrumentSpec> {
        self.by_symbol.get(&(venue, symbol.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(symbol: &str, tick_micro: i128, lot_qty_e9: i128) -> InstrumentSpec {
        InstrumentSpec {
            instrument: InstrumentId(symbol.to_string()),
            symbol: symbol.to_string(),
            venue: Venue::Hl,
            kind: Kind::Perp,
            tick_micro,
            lot_qty_e9,
            micro_per_tick_lot: 0,
        }
    }

    fn pm_spec(symbol: &str, tick_micro: i128, lot_qty_e9: i128) -> InstrumentSpec {
        InstrumentSpec {
            instrument: InstrumentId(symbol.to_string()),
            symbol: symbol.to_string(),
            venue: Venue::Pm,
            kind: Kind::Binary,
            tick_micro,
            lot_qty_e9,
            micro_per_tick_lot: 0,
        }
    }

    #[test]
    fn rejects_duplicate_symbols() {
        let result = Registry::try_from_specs([
            spec("DUP", 1, 1_000_000_000),
            spec("DUP", 1, 1_000_000_000),
        ]);
        assert!(matches!(
            result,
            Err(RegistryError::DuplicateSymbol(symbol)) if symbol == "DUP"
        ));
    }

    #[test]
    fn rejects_derived_value_overflow() {
        let result = Registry::try_from_specs([spec("OVERFLOW", i128::MAX, 2)]);
        assert!(matches!(
            result,
            Err(RegistryError::ArithmeticOverflow(symbol)) if symbol == "OVERFLOW"
        ));
    }

    #[test]
    fn rejects_fractional_micro_tick_lot_value() {
        let result = Registry::try_from_specs([spec("FRACTIONAL", 1, 1)]);
        assert!(matches!(
            result,
            Err(RegistryError::InvalidSpec(symbol)) if symbol == "FRACTIONAL"
        ));
    }

    #[test]
    fn rejects_venue_kind_mismatch() {
        let mut invalid = pm_spec("MISMATCH", 100, 1_000_000_000);
        invalid.kind = Kind::Perp;
        assert!(matches!(
            Registry::try_from_specs([invalid]),
            Err(RegistryError::InvalidSpec(symbol)) if symbol == "MISMATCH"
        ));
    }

    #[test]
    fn rejects_binary_tick_that_cannot_represent_probability_bounds() {
        assert!(matches!(
            Registry::try_from_specs([pm_spec("BAD-TICK", 300, 1_000_000_000)]),
            Err(RegistryError::InvalidSpec(symbol)) if symbol == "BAD-TICK"
        ));
    }

    #[test]
    fn rejects_binary_lot_other_than_one_contract() {
        assert!(matches!(
            Registry::try_from_specs([pm_spec("BAD-LOT", 100, 2_000_000_000)]),
            Err(RegistryError::InvalidSpec(symbol)) if symbol == "BAD-LOT"
        ));
    }

    #[test]
    fn resolves_same_raw_symbol_by_venue() {
        let mut hl = spec("SHARED", 500_000, 100_000);
        hl.instrument = InstrumentId("SHARED-PERP".to_string());
        let mut pm = pm_spec("SHARED", 100, 1_000_000_000);
        pm.instrument = InstrumentId("SHARED-BINARY".to_string());
        let registry = Registry::try_from_specs([hl, pm]).unwrap();

        assert_eq!(
            registry.lookup(Venue::Hl, "SHARED").unwrap().instrument,
            InstrumentId("SHARED-PERP".to_string())
        );
        assert_eq!(
            registry.lookup(Venue::Pm, "SHARED").unwrap().instrument,
            InstrumentId("SHARED-BINARY".to_string())
        );
    }
}
