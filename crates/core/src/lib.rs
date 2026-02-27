use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct OrderId(pub u64);

impl fmt::Display for OrderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    pub fn opposite(self) -> Self {
        match self {
            Self::Bid => Self::Ask,
            Self::Ask => Self::Bid,
        }
    }

    pub fn as_u8(self) -> u8 {
        match self {
            Self::Bid => 0,
            Self::Ask => 1,
        }
    }

    pub fn from_u8(value: u8) -> Result<Self, CoreError> {
        match value {
            0 => Ok(Self::Bid),
            1 => Ok(Self::Ask),
            _ => Err(CoreError::InvalidSide(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    AddOrder {
        order_id: OrderId,
        side: Side,
        price_ticks: u64,
        qty: u64,
    },
    ModifyOrder {
        order_id: OrderId,
        new_qty: u64,
    },
    CancelOrder {
        order_id: OrderId,
        cancel_qty: u64,
    },
    Trade {
        maker_order_id: OrderId,
        taker_side: Side,
        price_ticks: u64,
        qty: u64,
    },
    BookSnapshotTop {
        bid_px: u64,
        bid_sz: u64,
        ask_px: u64,
        ask_sz: u64,
    },
}

impl Event {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::AddOrder { .. } => "add",
            Self::ModifyOrder { .. } => "modify",
            Self::CancelOrder { .. } => "cancel",
            Self::Trade { .. } => "trade",
            Self::BookSnapshotTop { .. } => "book_snapshot_top",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalEvent {
    pub timestamp_ns: u64,
    pub sequence: u64,
    pub symbol: String,
    pub venue: String,
    pub event: Event,
}

impl CanonicalEvent {
    pub fn new(
        timestamp_ns: u64,
        sequence: u64,
        symbol: impl Into<String>,
        venue: impl Into<String>,
        event: Event,
    ) -> Self {
        Self {
            timestamp_ns,
            sequence,
            symbol: symbol.into(),
            venue: venue.into(),
            event,
        }
    }

    pub fn validate(&self) -> Result<(), CoreError> {
        if self.symbol.is_empty() {
            return Err(CoreError::EmptySymbol);
        }
        if self.venue.is_empty() {
            return Err(CoreError::EmptyVenue);
        }
        match &self.event {
            Event::AddOrder { qty, .. } | Event::Trade { qty, .. } if *qty == 0 => {
                Err(CoreError::ZeroQuantity(self.event.kind()))
            }
            Event::CancelOrder { cancel_qty, .. } if *cancel_qty == 0 => {
                Err(CoreError::ZeroQuantity(self.event.kind()))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRecord {
    pub symbol: String,
    pub order_id: OrderId,
    pub side: Side,
    pub price_ticks: u64,
    pub qty: u64,
    pub entry_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EngineSnapshot {
    pub cursor_sequence: u64,
    pub orders: Vec<OrderRecord>,
}

impl EngineSnapshot {
    pub fn sort_stable(&mut self) {
        self.orders.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then_with(|| left.side.cmp(&right.side))
                .then_with(|| match left.side {
                    Side::Bid => right.price_ticks.cmp(&left.price_ticks),
                    Side::Ask => left.price_ticks.cmp(&right.price_ticks),
                })
                .then_with(|| left.entry_sequence.cmp(&right.entry_sequence))
                .then_with(|| left.order_id.cmp(&right.order_id))
        });
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BookTop {
    pub bid_px: u64,
    pub bid_sz: u64,
    pub ask_px: u64,
    pub ask_sz: u64,
}

impl Ord for BookTop {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.bid_px, self.bid_sz, self.ask_px, self.ask_sz).cmp(&(
            other.bid_px,
            other.bid_sz,
            other.ask_px,
            other.ask_sz,
        ))
    }
}

impl PartialOrd for BookTop {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("symbol must not be empty")]
    EmptySymbol,
    #[error("venue must not be empty")]
    EmptyVenue,
    #[error("{0} quantity must be non-zero")]
    ZeroQuantity(&'static str),
    #[error("invalid side value {0}")]
    InvalidSide(u8),
}

#[cfg(test)]
mod tests {
    use super::{CanonicalEvent, CoreError, Event, OrderId, Side};

    #[test]
    fn validates_required_fields() {
        let event = CanonicalEvent::new(
            1,
            1,
            "XBTUSD",
            "LAB",
            Event::AddOrder {
                order_id: OrderId(10),
                side: Side::Bid,
                price_ticks: 100,
                qty: 2,
            },
        );
        assert_eq!(event.validate(), Ok(()));
    }

    #[test]
    fn rejects_zero_qty_add() {
        let event = CanonicalEvent::new(
            1,
            1,
            "XBTUSD",
            "LAB",
            Event::AddOrder {
                order_id: OrderId(10),
                side: Side::Bid,
                price_ticks: 100,
                qty: 0,
            },
        );
        assert!(matches!(
            event.validate(),
            Err(CoreError::ZeroQuantity("add"))
        ));
    }
}
