use std::collections::{BTreeMap, HashMap, VecDeque};

use exchange_lab_core::{
    BookTop, CanonicalEvent, EngineSnapshot, Event, OrderId, OrderRecord, Side,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::trace;

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("event failed validation: {0}")]
    InvalidEvent(#[from] exchange_lab_core::CoreError),
    #[error("duplicate order id {0}")]
    DuplicateOrder(OrderId),
    #[error("unknown order id {0}")]
    UnknownOrder(OrderId),
    #[error("zero quantity is not allowed for resting orders")]
    ZeroQuantity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LiveOrder {
    symbol: String,
    side: Side,
    price_ticks: u64,
    qty: u64,
    entry_sequence: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SymbolBook {
    bids: BTreeMap<u64, VecDeque<OrderId>>,
    asks: BTreeMap<u64, VecDeque<OrderId>>,
}

#[derive(Debug, Default, Clone)]
pub struct MatchingEngine {
    books: HashMap<String, SymbolBook>,
    orders: HashMap<OrderId, LiveOrder>,
}

impl MatchingEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn process(&mut self, event: &CanonicalEvent) -> Result<Vec<CanonicalEvent>, EngineError> {
        event.validate()?;
        trace!(
            sequence = event.sequence,
            kind = event.event.kind(),
            "processing event"
        );
        let mut emitted = Vec::new();
        match &event.event {
            Event::AddOrder {
                order_id,
                side,
                price_ticks,
                qty,
            } => {
                self.add_order(event, *order_id, *side, *price_ticks, *qty, &mut emitted)?;
                emitted.push(self.snapshot_event(event));
            }
            Event::ModifyOrder { order_id, new_qty } => {
                self.modify_order(event, *order_id, *new_qty)?;
                emitted.push(self.snapshot_event(event));
            }
            Event::CancelOrder {
                order_id,
                cancel_qty,
            } => {
                self.cancel_order(event, *order_id, *cancel_qty)?;
                emitted.push(self.snapshot_event(event));
            }
            Event::Trade { .. } | Event::BookSnapshotTop { .. } => {}
        }
        Ok(emitted)
    }

    pub fn queue_position(&self, symbol: &str, order_id: OrderId) -> Option<usize> {
        let order = self.orders.get(&order_id)?;
        if order.symbol != symbol {
            return None;
        }
        let book = self.books.get(symbol)?;
        let queue = match order.side {
            Side::Bid => book.bids.get(&order.price_ticks)?,
            Side::Ask => book.asks.get(&order.price_ticks)?,
        };
        queue.iter().position(|candidate| *candidate == order_id)
    }

    pub fn top_of_book(&self, symbol: &str) -> BookTop {
        let Some(book) = self.books.get(symbol) else {
            return BookTop::default();
        };

        let (bid_px, bid_sz) = book
            .bids
            .iter()
            .next_back()
            .map_or((0, 0), |(price, queue)| (*price, self.level_size(queue)));
        let (ask_px, ask_sz) = book
            .asks
            .iter()
            .next()
            .map_or((0, 0), |(price, queue)| (*price, self.level_size(queue)));

        BookTop {
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
        }
    }

    pub fn snapshot(&self, cursor_sequence: u64) -> EngineSnapshot {
        let mut snapshot = EngineSnapshot {
            cursor_sequence,
            orders: self
                .orders
                .iter()
                .map(|(order_id, order)| OrderRecord {
                    symbol: order.symbol.clone(),
                    order_id: *order_id,
                    side: order.side,
                    price_ticks: order.price_ticks,
                    qty: order.qty,
                    entry_sequence: order.entry_sequence,
                })
                .collect(),
        };
        snapshot.sort_stable();
        snapshot
    }

    pub fn load_snapshot(snapshot: &EngineSnapshot) -> Result<Self, EngineError> {
        let mut engine = Self::new();
        for record in &snapshot.orders {
            let event = CanonicalEvent::new(
                0,
                record.entry_sequence,
                record.symbol.clone(),
                "SNAPSHOT",
                Event::AddOrder {
                    order_id: record.order_id,
                    side: record.side,
                    price_ticks: record.price_ticks,
                    qty: record.qty,
                },
            );
            engine.process_snapshot_order(&event, record.entry_sequence)?;
        }
        Ok(engine)
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    pub fn total_resting_qty(&self, symbol: &str) -> u64 {
        self.orders
            .values()
            .filter(|order| order.symbol == symbol)
            .map(|order| order.qty)
            .sum()
    }

    fn add_order(
        &mut self,
        event: &CanonicalEvent,
        order_id: OrderId,
        side: Side,
        price_ticks: u64,
        qty: u64,
        emitted: &mut Vec<CanonicalEvent>,
    ) -> Result<(), EngineError> {
        if qty == 0 {
            return Err(EngineError::ZeroQuantity);
        }
        if self.orders.contains_key(&order_id) {
            return Err(EngineError::DuplicateOrder(order_id));
        }

        let mut remaining = qty;
        while remaining > 0 {
            let Some((maker_order_id, maker_price)) =
                self.best_match_candidate(&event.symbol, side, price_ticks)
            else {
                break;
            };

            let Some(trade_qty) = self.fill_against(order_id, side, maker_order_id, remaining)
            else {
                self.remove_order_from_levels(
                    &event.symbol,
                    maker_order_id,
                    maker_price,
                    side.opposite(),
                );
                continue;
            };
            remaining -= trade_qty;
            emitted.push(CanonicalEvent::new(
                event.timestamp_ns,
                event.sequence,
                event.symbol.clone(),
                event.venue.clone(),
                Event::Trade {
                    maker_order_id,
                    taker_side: side,
                    price_ticks: maker_price,
                    qty: trade_qty,
                },
            ));
        }

        if remaining > 0 {
            self.insert_resting_order(
                event.symbol.clone(),
                order_id,
                side,
                price_ticks,
                remaining,
                event.sequence,
            )?;
        }
        Ok(())
    }

    fn process_snapshot_order(
        &mut self,
        event: &CanonicalEvent,
        entry_sequence: u64,
    ) -> Result<(), EngineError> {
        if let Event::AddOrder {
            order_id,
            side,
            price_ticks,
            qty,
        } = &event.event
        {
            self.insert_resting_order(
                event.symbol.clone(),
                *order_id,
                *side,
                *price_ticks,
                *qty,
                entry_sequence,
            )?;
        }
        Ok(())
    }

    fn modify_order(
        &mut self,
        event: &CanonicalEvent,
        order_id: OrderId,
        new_qty: u64,
    ) -> Result<(), EngineError> {
        let Some(existing_order) = self.orders.get(&order_id) else {
            return Err(EngineError::UnknownOrder(order_id));
        };
        if existing_order.symbol != event.symbol {
            return Err(EngineError::UnknownOrder(order_id));
        }
        if new_qty == 0 {
            return self.remove_order(order_id);
        }
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(EngineError::UnknownOrder(order_id))?;
        order.qty = new_qty;
        Ok(())
    }

    fn cancel_order(
        &mut self,
        event: &CanonicalEvent,
        order_id: OrderId,
        cancel_qty: u64,
    ) -> Result<(), EngineError> {
        let removal = {
            let Some(order) = self.orders.get_mut(&order_id) else {
                return Err(EngineError::UnknownOrder(order_id));
            };
            if order.symbol != event.symbol {
                return Err(EngineError::UnknownOrder(order_id));
            }
            if cancel_qty >= order.qty {
                Some((order.symbol.clone(), order.side, order.price_ticks))
            } else {
                order.qty -= cancel_qty;
                None
            }
        };

        if let Some((symbol, side, price_ticks)) = removal {
            let _ = self.orders.remove(&order_id);
            self.remove_order_from_levels(&symbol, order_id, price_ticks, side);
        }
        Ok(())
    }

    fn best_match_candidate(
        &self,
        symbol: &str,
        taker_side: Side,
        limit_price: u64,
    ) -> Option<(OrderId, u64)> {
        let book = self.books.get(symbol)?;
        match taker_side {
            Side::Bid => {
                let (&price, queue) = book.asks.iter().next()?;
                if price > limit_price {
                    None
                } else {
                    queue.front().copied().map(|order_id| (order_id, price))
                }
            }
            Side::Ask => {
                let (&price, queue) = book.bids.iter().next_back()?;
                if price < limit_price {
                    None
                } else {
                    queue.front().copied().map(|order_id| (order_id, price))
                }
            }
        }
    }

    fn fill_against(
        &mut self,
        taker_order_id: OrderId,
        taker_side: Side,
        maker_order_id: OrderId,
        taker_remaining: u64,
    ) -> Option<u64> {
        let (trade_qty, removal) = {
            let maker_order = self.orders.get_mut(&maker_order_id)?;
            if maker_order.side == taker_side || maker_order.qty == 0 {
                return None;
            }
            let trade_qty = maker_order.qty.min(taker_remaining);
            maker_order.qty -= trade_qty;
            let removal = if maker_order.qty == 0 {
                Some((
                    maker_order.symbol.clone(),
                    maker_order.price_ticks,
                    maker_order.side,
                ))
            } else {
                None
            };
            (trade_qty, removal)
        };

        if let Some((symbol, price_ticks, side)) = removal {
            let _ = self.orders.remove(&maker_order_id);
            self.remove_order_from_levels(&symbol, maker_order_id, price_ticks, side);
        }
        trace!(
            taker = taker_order_id.0,
            maker = maker_order_id.0,
            qty = trade_qty,
            "filled order"
        );
        Some(trade_qty)
    }

    fn insert_resting_order(
        &mut self,
        symbol: String,
        order_id: OrderId,
        side: Side,
        price_ticks: u64,
        qty: u64,
        entry_sequence: u64,
    ) -> Result<(), EngineError> {
        if qty == 0 {
            return Err(EngineError::ZeroQuantity);
        }
        let order = LiveOrder {
            symbol: symbol.clone(),
            side,
            price_ticks,
            qty,
            entry_sequence,
        };
        self.orders.insert(order_id, order);
        let book = self.books.entry(symbol).or_default();
        let levels = match side {
            Side::Bid => &mut book.bids,
            Side::Ask => &mut book.asks,
        };
        levels.entry(price_ticks).or_default().push_back(order_id);
        Ok(())
    }

    fn level_size(&self, queue: &VecDeque<OrderId>) -> u64 {
        queue
            .iter()
            .filter_map(|order_id| self.orders.get(order_id))
            .map(|order| order.qty)
            .sum()
    }

    fn remove_order(&mut self, order_id: OrderId) -> Result<(), EngineError> {
        let order = self
            .orders
            .remove(&order_id)
            .ok_or(EngineError::UnknownOrder(order_id))?;
        self.remove_order_from_levels(&order.symbol, order_id, order.price_ticks, order.side);
        Ok(())
    }

    fn remove_order_from_levels(
        &mut self,
        symbol: &str,
        order_id: OrderId,
        price_ticks: u64,
        side: Side,
    ) {
        let mut remove_symbol = false;
        if let Some(book) = self.books.get_mut(symbol) {
            let levels = match side {
                Side::Bid => &mut book.bids,
                Side::Ask => &mut book.asks,
            };
            let mut should_remove_level = false;
            if let Some(queue) = levels.get_mut(&price_ticks) {
                if let Some(position) = queue.iter().position(|candidate| *candidate == order_id) {
                    let _ = queue.remove(position);
                }
                should_remove_level = queue.is_empty();
            }
            if should_remove_level {
                let _ = levels.remove(&price_ticks);
            }
            remove_symbol = book.bids.is_empty() && book.asks.is_empty();
        }
        if remove_symbol {
            self.books.remove(symbol);
        }
    }

    fn snapshot_event(&self, event: &CanonicalEvent) -> CanonicalEvent {
        let top = self.top_of_book(&event.symbol);
        CanonicalEvent::new(
            event.timestamp_ns,
            event.sequence,
            event.symbol.clone(),
            event.venue.clone(),
            Event::BookSnapshotTop {
                bid_px: top.bid_px,
                bid_sz: top.bid_sz,
                ask_px: top.ask_px,
                ask_sz: top.ask_sz,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use exchange_lab_core::{CanonicalEvent, Event, OrderId, Side};
    use proptest::prelude::*;

    use super::MatchingEngine;

    fn add(sequence: u64, order_id: u64, side: Side, price: u64, qty: u64) -> CanonicalEvent {
        CanonicalEvent::new(
            sequence,
            sequence,
            "XBTUSD",
            "LAB",
            Event::AddOrder {
                order_id: OrderId(order_id),
                side,
                price_ticks: price,
                qty,
            },
        )
    }

    fn cancel(sequence: u64, order_id: u64, cancel_qty: u64) -> CanonicalEvent {
        CanonicalEvent::new(
            sequence,
            sequence,
            "XBTUSD",
            "LAB",
            Event::CancelOrder {
                order_id: OrderId(order_id),
                cancel_qty,
            },
        )
    }

    fn modify(sequence: u64, order_id: u64, new_qty: u64) -> CanonicalEvent {
        CanonicalEvent::new(
            sequence,
            sequence,
            "XBTUSD",
            "LAB",
            Event::ModifyOrder {
                order_id: OrderId(order_id),
                new_qty,
            },
        )
    }

    #[test]
    fn enforces_price_time_priority_with_partial_fill() {
        let mut engine = MatchingEngine::new();
        engine
            .process(&add(1, 1, Side::Ask, 101, 3))
            .expect("ask add");
        engine
            .process(&add(2, 2, Side::Ask, 101, 5))
            .expect("ask add");
        let emitted = engine
            .process(&add(3, 3, Side::Bid, 102, 6))
            .expect("cross add");

        let trades: Vec<_> = emitted
            .into_iter()
            .filter(|event| matches!(event.event, Event::Trade { .. }))
            .collect();
        assert_eq!(trades.len(), 2);
        assert!(matches!(
            trades[0].event,
            Event::Trade {
                maker_order_id: OrderId(1),
                qty: 3,
                ..
            }
        ));
        assert!(matches!(
            trades[1].event,
            Event::Trade {
                maker_order_id: OrderId(2),
                qty: 3,
                ..
            }
        ));
        assert_eq!(engine.queue_position("XBTUSD", OrderId(2)), Some(0));
    }

    #[test]
    fn cancel_and_modify_adjust_resting_state() {
        let mut engine = MatchingEngine::new();
        engine
            .process(&add(1, 11, Side::Bid, 100, 10))
            .expect("bid add");
        engine.process(&modify(2, 11, 7)).expect("modify");
        engine.process(&cancel(3, 11, 2)).expect("cancel");
        assert_eq!(engine.total_resting_qty("XBTUSD"), 5);
        assert_eq!(engine.queue_position("XBTUSD", OrderId(11)), Some(0));
    }

    #[test]
    fn snapshot_round_trip_preserves_queue_order() {
        let mut engine = MatchingEngine::new();
        engine
            .process(&add(1, 1, Side::Bid, 100, 2))
            .expect("add 1");
        engine
            .process(&add(2, 2, Side::Bid, 100, 4))
            .expect("add 2");
        let snapshot = engine.snapshot(2);
        let restored = MatchingEngine::load_snapshot(&snapshot).expect("restore");
        assert_eq!(restored.queue_position("XBTUSD", OrderId(1)), Some(0));
        assert_eq!(restored.queue_position("XBTUSD", OrderId(2)), Some(1));
        assert_eq!(restored.total_resting_qty("XBTUSD"), 6);
    }

    proptest! {
        #[test]
        fn random_event_streams_do_not_break_book_invariants(stream in prop::collection::vec((0_u8..3, 1_u64..50, 90_u64..110, 1_u64..20, 1_u64..20), 1..128)) {
            let mut engine = MatchingEngine::new();
            let mut sequence = 1_u64;

            for (kind, order_id, price, qty, alt_qty) in stream {
                let event = match kind {
                    0 => add(sequence, order_id, if sequence % 2 == 0 { Side::Bid } else { Side::Ask }, price, qty),
                    1 => modify(sequence, order_id, alt_qty),
                    _ => cancel(sequence, order_id, alt_qty),
                };
                let _ = engine.process(&event);
                let top = engine.top_of_book("XBTUSD");
                prop_assert!(top.ask_px == 0 || top.bid_px == 0 || top.bid_px <= top.ask_px);
                prop_assert!(engine.snapshot(sequence).orders.iter().all(|order| order.qty > 0));
                sequence += 1;
            }
        }
    }
}
