use std::fs;

use exchange_lab_core::{CanonicalEvent, Event};
use exchange_lab_engine::MatchingEngine;
use exchange_lab_ingest::parse_csv_file;
use exchange_lab_journal::{JournalConfig, JournalReader, JournalWriter};
use exchange_lab_replay::{replay_events, FaultInjection, ReplayMode};
use tempfile::tempdir;

#[test]
fn ingest_engine_journal_replay_flow_is_consistent() {
    let dir = tempdir().expect("tempdir");
    let csv_path = dir.path().join("events.csv");
    let raw_log = dir.path().join("raw.log");
    let raw_idx = dir.path().join("raw.idx");
    let engine_log = dir.path().join("engine.log");
    let engine_idx = dir.path().join("engine.idx");

    fs::write(
        &csv_path,
        concat!(
            "timestamp_ns,sequence,symbol,venue,event_type,order_id,side,price_ticks,qty,new_qty,cancel_qty,maker_order_id,taker_side,bid_px,bid_sz,ask_px,ask_sz\n",
            "1,1,XBTUSD,LAB,AddOrder,1001,Bid,100,4,,,,,,,,\n",
            "2,2,XBTUSD,LAB,AddOrder,1002,Ask,101,3,,,,,,,,\n",
            "3,3,XBTUSD,LAB,AddOrder,1003,Bid,102,2,,,,,,,,\n"
        ),
    )
    .expect("write csv");

    let parsed = parse_csv_file(&csv_path).expect("parse csv");
    write_events(&raw_log, &raw_idx, &parsed.events);

    let mut writer = JournalWriter::create(
        &engine_log,
        &engine_idx,
        JournalConfig {
            index_stride: 1,
            writer_capacity: 8 * 1024,
        },
    )
    .expect("writer");
    let mut engine = MatchingEngine::new();
    let mut next_sequence = 1_u64;

    for event in &parsed.events {
        let normalized = resequence_event(event, next_sequence);
        next_sequence += 1;
        writer.append(&normalized).expect("append input");
        if matches!(
            normalized.event,
            Event::AddOrder { .. } | Event::ModifyOrder { .. } | Event::CancelOrder { .. }
        ) {
            for derived in engine.process(&normalized).expect("engine process") {
                let normalized_derived = resequence_event(&derived, next_sequence);
                next_sequence += 1;
                writer.append(&normalized_derived).expect("append derived");
            }
        }
    }
    writer.flush().expect("flush");

    let mut reader = JournalReader::open(&engine_log, &engine_idx).expect("reader");
    let journaled = reader.read_all().expect("read all");
    let replayed =
        replay_events(&journaled, ReplayMode::MaxSpeed, FaultInjection::default()).expect("replay");

    assert!(replayed.failures.is_empty());
    assert!(replayed.stats.events_replayed as usize >= journaled.len());
    assert!(replayed
        .final_snapshot
        .orders
        .iter()
        .all(|order| order.qty > 0));
}

#[test]
fn deterministic_replay_produces_identical_logs() {
    let dir = tempdir().expect("tempdir");
    let csv_path = dir.path().join("events.csv");
    let first_log = dir.path().join("first.log");
    let first_idx = dir.path().join("first.idx");
    let second_log = dir.path().join("second.log");
    let second_idx = dir.path().join("second.idx");

    fs::write(
        &csv_path,
        concat!(
            "timestamp_ns,sequence,symbol,venue,event_type,order_id,side,price_ticks,qty,new_qty,cancel_qty,maker_order_id,taker_side,bid_px,bid_sz,ask_px,ask_sz\n",
            "1,1,XBTUSD,LAB,AddOrder,1,Bid,100,5,,,,,,,,\n",
            "2,2,XBTUSD,LAB,AddOrder,2,Ask,101,2,,,,,,,,\n",
            "3,3,XBTUSD,LAB,AddOrder,3,Bid,102,1,,,,,,,,\n"
        ),
    )
    .expect("write csv");

    let parsed = parse_csv_file(&csv_path).expect("parse csv");
    let fault = FaultInjection {
        packet_loss_pct: 10.0,
        reorder_window: 2,
        timestamp_skew_ns: 5,
        seed: 99,
    };

    let first = replay_events(&parsed.events, ReplayMode::MaxSpeed, fault).expect("first replay");
    let second = replay_events(&parsed.events, ReplayMode::MaxSpeed, fault).expect("second replay");

    let first_normalized = normalize_event_stream(&first.emitted_events);
    let second_normalized = normalize_event_stream(&second.emitted_events);
    write_events(&first_log, &first_idx, &first_normalized);
    write_events(&second_log, &second_idx, &second_normalized);

    assert_eq!(
        fs::read(&first_log).expect("first log"),
        fs::read(&second_log).expect("second log")
    );
    assert_eq!(
        fs::read(&first_idx).expect("first idx"),
        fs::read(&second_idx).expect("second idx")
    );
}

fn write_events(log: &std::path::Path, index: &std::path::Path, events: &[CanonicalEvent]) {
    let mut writer = JournalWriter::create(
        log,
        index,
        JournalConfig {
            index_stride: 1,
            writer_capacity: 8 * 1024,
        },
    )
    .expect("writer");
    for event in events {
        writer.append(event).expect("append");
    }
    writer.flush().expect("flush");
}

fn normalize_event_stream(events: &[CanonicalEvent]) -> Vec<CanonicalEvent> {
    events
        .iter()
        .enumerate()
        .map(|(index, event)| resequence_event(event, index as u64 + 1))
        .collect()
}

fn resequence_event(event: &CanonicalEvent, sequence: u64) -> CanonicalEvent {
    let mut cloned = event.clone();
    cloned.sequence = sequence;
    cloned
}
