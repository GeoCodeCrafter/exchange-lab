use std::thread;
use std::time::{Duration, Instant};

use exchange_lab_core::{CanonicalEvent, EngineSnapshot, Event};
use exchange_lab_engine::{EngineError, MatchingEngine};
use exchange_lab_journal::{load_snapshot_and_tail, JournalError, JournalReader};
use rand::{rngs::StdRng, Rng, SeedableRng};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReplayMode {
    MaxSpeed,
    Paced { speed: f64 },
    Step,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FaultInjection {
    pub packet_loss_pct: f64,
    pub reorder_window: usize,
    pub timestamp_skew_ns: i64,
    pub seed: u64,
}

impl Default for FaultInjection {
    fn default() -> Self {
        Self {
            packet_loss_pct: 0.0,
            reorder_window: 0,
            timestamp_skew_ns: 0,
            seed: 7,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantFailure {
    pub sequence: Option<u64>,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayStats {
    pub events_replayed: u64,
    pub invariant_failures: u64,
    pub elapsed_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayResult {
    pub emitted_events: Vec<CanonicalEvent>,
    pub final_snapshot: EngineSnapshot,
    pub failures: Vec<InvariantFailure>,
    pub stats: ReplayStats,
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("engine error: {0}")]
    Engine(#[from] EngineError),
    #[error("journal error: {0}")]
    Journal(#[from] JournalError),
}

pub fn replay_events(
    events: &[CanonicalEvent],
    mode: ReplayMode,
    fault: FaultInjection,
) -> Result<ReplayResult, ReplayError> {
    replay_events_with_stepper(events, mode, fault, |_| {})
}

pub fn replay_events_with_stepper<F>(
    events: &[CanonicalEvent],
    mode: ReplayMode,
    fault: FaultInjection,
    mut stepper: F,
) -> Result<ReplayResult, ReplayError>
where
    F: FnMut(&CanonicalEvent),
{
    run_replay(MatchingEngine::new(), events, mode, fault, &mut stepper)
}

pub fn replay_journal(
    log_path: impl AsRef<std::path::Path>,
    index_path: impl AsRef<std::path::Path>,
    mode: ReplayMode,
    fault: FaultInjection,
) -> Result<ReplayResult, ReplayError> {
    let mut reader = JournalReader::open(log_path, index_path)?;
    let events = reader.read_all()?;
    replay_events(&events, mode, fault)
}

pub fn replay_from_snapshot(
    snapshot_path: impl AsRef<std::path::Path>,
    log_path: impl AsRef<std::path::Path>,
    index_path: impl AsRef<std::path::Path>,
    mode: ReplayMode,
    fault: FaultInjection,
) -> Result<ReplayResult, ReplayError> {
    let (snapshot, tail) = load_snapshot_and_tail(snapshot_path, log_path, index_path)?;
    let engine = MatchingEngine::load_snapshot(&snapshot)?;
    let mut noop = |_event: &CanonicalEvent| {};
    run_replay(engine, &tail, mode, fault, &mut noop)
}

fn run_replay<F>(
    mut engine: MatchingEngine,
    events: &[CanonicalEvent],
    mode: ReplayMode,
    fault: FaultInjection,
    stepper: &mut F,
) -> Result<ReplayResult, ReplayError>
where
    F: FnMut(&CanonicalEvent),
{
    let started = Instant::now();
    let faulted_events = apply_faults(events, fault);
    let mut emitted_events = Vec::new();
    let mut failures = Vec::new();
    let mut last_sequence = None;
    let mut last_timestamp = None;

    for event in faulted_events {
        if let Some(previous_sequence) =
            last_sequence.filter(|previous_sequence| event.sequence <= *previous_sequence)
        {
            failures.push(InvariantFailure {
                sequence: Some(event.sequence),
                message: format!(
                    "non-monotonic sequence: {} after {}",
                    event.sequence, previous_sequence
                ),
            });
        }
        pace(mode, last_timestamp, event.timestamp_ns);
        last_sequence = Some(event.sequence);
        last_timestamp = Some(event.timestamp_ns);
        emitted_events.push(event.clone());

        if matches!(mode, ReplayMode::Step) {
            stepper(&event);
        }

        if matches!(
            event.event,
            Event::AddOrder { .. } | Event::ModifyOrder { .. } | Event::CancelOrder { .. }
        ) {
            let derived = engine.process(&event)?;
            emitted_events.extend(derived);
            failures.extend(check_invariants(&engine, event.sequence));
        }
    }

    let final_snapshot = engine.snapshot(last_sequence.unwrap_or_default());
    let stats = ReplayStats {
        events_replayed: emitted_events.len() as u64,
        invariant_failures: failures.len() as u64,
        elapsed_ns: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
    };

    Ok(ReplayResult {
        emitted_events,
        final_snapshot,
        failures,
        stats,
    })
}

fn apply_faults(events: &[CanonicalEvent], fault: FaultInjection) -> Vec<CanonicalEvent> {
    let mut rng = StdRng::seed_from_u64(fault.seed);
    let mut output = Vec::with_capacity(events.len());
    let mut reorder_buffer = Vec::with_capacity(fault.reorder_window.saturating_add(1));

    for event in events {
        if fault.packet_loss_pct > 0.0 {
            let draw = rng.gen_range(0.0..100.0);
            if draw < fault.packet_loss_pct {
                continue;
            }
        }

        let mut skewed = event.clone();
        skewed.timestamp_ns = apply_timestamp_skew(skewed.timestamp_ns, fault.timestamp_skew_ns);

        if fault.reorder_window == 0 {
            output.push(skewed);
            continue;
        }

        reorder_buffer.push(skewed);
        if reorder_buffer.len() > fault.reorder_window {
            let index = rng.gen_range(0..reorder_buffer.len());
            output.push(reorder_buffer.remove(index));
        }
    }

    while !reorder_buffer.is_empty() {
        let index = rng.gen_range(0..reorder_buffer.len());
        output.push(reorder_buffer.remove(index));
    }

    output
}

fn apply_timestamp_skew(timestamp_ns: u64, skew_ns: i64) -> u64 {
    if skew_ns >= 0 {
        timestamp_ns.saturating_add(skew_ns as u64)
    } else {
        let magnitude = skew_ns.checked_abs().unwrap_or(i64::MAX) as u64;
        timestamp_ns.saturating_sub(magnitude)
    }
}

fn pace(mode: ReplayMode, last_timestamp: Option<u64>, current_timestamp: u64) {
    if let ReplayMode::Paced { speed } = mode {
        if speed > 0.0 {
            if let Some(previous) = last_timestamp {
                let delta = current_timestamp.saturating_sub(previous);
                if delta > 0 {
                    let nanos = (delta as f64 / speed).max(0.0) as u64;
                    if nanos > 0 {
                        thread::sleep(Duration::from_nanos(nanos));
                    }
                }
            }
        }
    }
}

fn check_invariants(engine: &MatchingEngine, sequence: u64) -> Vec<InvariantFailure> {
    let mut failures = Vec::new();
    let snapshot = engine.snapshot(sequence);
    for order in &snapshot.orders {
        if order.qty == 0 {
            failures.push(InvariantFailure {
                sequence: Some(sequence),
                message: format!("resting order {} has zero quantity", order.order_id.0),
            });
        }
    }

    let mut current_symbol = String::new();
    for order in &snapshot.orders {
        if order.symbol != current_symbol {
            current_symbol = order.symbol.clone();
            let top = engine.top_of_book(&current_symbol);
            if top.bid_px > 0 && top.ask_px > 0 && top.bid_px > top.ask_px {
                failures.push(InvariantFailure {
                    sequence: Some(sequence),
                    message: format!(
                        "crossed book for {}: bid {} ask {}",
                        current_symbol, top.bid_px, top.ask_px
                    ),
                });
            }
        }
    }
    failures
}

#[cfg(test)]
mod tests {
    use exchange_lab_core::{CanonicalEvent, Event, OrderId, Side};

    use super::{replay_events, FaultInjection, ReplayMode};

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

    #[test]
    fn replay_is_deterministic_for_same_seed() {
        let events = vec![
            add(1, 1, Side::Bid, 100, 3),
            add(2, 2, Side::Ask, 101, 2),
            add(3, 3, Side::Bid, 102, 1),
        ];
        let fault = FaultInjection {
            packet_loss_pct: 10.0,
            reorder_window: 2,
            timestamp_skew_ns: 5,
            seed: 42,
        };

        let first = replay_events(&events, ReplayMode::MaxSpeed, fault).expect("replay first");
        let second = replay_events(&events, ReplayMode::MaxSpeed, fault).expect("replay second");
        assert_eq!(first.emitted_events, second.emitted_events);
        assert_eq!(first.failures, second.failures);
    }
}
