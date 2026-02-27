use std::fs;
use std::io::{self, Read};
use std::path::Path;
use std::time::Instant;

use exchange_lab_core::{CanonicalEvent, Event, OrderId, Side};
use serde::Deserialize;
use thiserror::Error;

const SYMBOL_FIELD_LEN: usize = 8;
const VENUE_FIELD_LEN: usize = 8;
const PAYLOAD_FIELD_LEN: usize = 32;
const BINARY_RECORD_LEN: usize = 8 + 8 + SYMBOL_FIELD_LEN + VENUE_FIELD_LEN + 1 + PAYLOAD_FIELD_LEN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseStats {
    pub events: usize,
    pub elapsed_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedEvents {
    pub events: Vec<CanonicalEvent>,
    pub stats: ParseStats,
}

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("csv error: {0}")]
    Csv(#[from] csv::Error),
    #[error("invalid event type {0}")]
    InvalidEventType(String),
    #[error("missing field {0}")]
    MissingField(&'static str),
    #[error("invalid side value {0}")]
    InvalidSide(String),
    #[error("binary feed length is not a multiple of the record size")]
    InvalidBinaryLength,
    #[error("binary feed truncated")]
    TruncatedBinary,
    #[error("binary feed contains invalid utf-8 text")]
    InvalidText,
    #[error("event failed validation: {0}")]
    InvalidEvent(#[from] exchange_lab_core::CoreError),
}

#[derive(Debug, Deserialize)]
struct CsvRow {
    timestamp_ns: u64,
    sequence: u64,
    symbol: String,
    venue: String,
    event_type: String,
    order_id: Option<u64>,
    side: Option<String>,
    price_ticks: Option<u64>,
    qty: Option<u64>,
    new_qty: Option<u64>,
    cancel_qty: Option<u64>,
    maker_order_id: Option<u64>,
    taker_side: Option<String>,
    bid_px: Option<u64>,
    bid_sz: Option<u64>,
    ask_px: Option<u64>,
    ask_sz: Option<u64>,
}

pub fn parse_csv_file(path: impl AsRef<Path>) -> Result<ParsedEvents, IngestError> {
    let started = Instant::now();
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)?;
    let mut events = Vec::new();
    for row in reader.deserialize() {
        let row: CsvRow = row?;
        let event = map_csv_row(row)?;
        events.push(event);
    }
    Ok(ParsedEvents {
        stats: ParseStats {
            events: events.len(),
            elapsed_ns: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        },
        events,
    })
}

pub fn parse_binary_file_a(path: impl AsRef<Path>) -> Result<ParsedEvents, IngestError> {
    let file = fs::File::open(path)?;
    parse_binary_feed_a(file)
}

pub fn parse_binary_file_b(path: impl AsRef<Path>) -> Result<ParsedEvents, IngestError> {
    let started = Instant::now();
    let bytes = fs::read(path)?;
    let events = parse_binary_events_b(&bytes)?;
    Ok(ParsedEvents {
        stats: ParseStats {
            events: events.len(),
            elapsed_ns: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        },
        events,
    })
}

pub fn parse_binary_feed_a(mut reader: impl Read) -> Result<ParsedEvents, IngestError> {
    let started = Instant::now();
    let mut buffer = [0_u8; BINARY_RECORD_LEN];
    let mut events = Vec::new();
    while let Some(()) = read_exact_or_eof(&mut reader, &mut buffer)? {
        events.push(parse_record(&buffer)?);
    }
    Ok(ParsedEvents {
        stats: ParseStats {
            events: events.len(),
            elapsed_ns: started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
        },
        events,
    })
}

pub fn parse_binary_events_b(bytes: &[u8]) -> Result<Vec<CanonicalEvent>, IngestError> {
    if !bytes.len().is_multiple_of(BINARY_RECORD_LEN) {
        return Err(IngestError::InvalidBinaryLength);
    }
    bytes
        .chunks_exact(BINARY_RECORD_LEN)
        .map(parse_record)
        .collect()
}

pub fn encode_binary_feed(events: &[CanonicalEvent]) -> Result<Vec<u8>, IngestError> {
    let mut bytes = Vec::with_capacity(events.len() * BINARY_RECORD_LEN);
    for event in events {
        event.validate()?;
        let mut record = [0_u8; BINARY_RECORD_LEN];
        record[0..8].copy_from_slice(&event.timestamp_ns.to_le_bytes());
        record[8..16].copy_from_slice(&event.sequence.to_le_bytes());
        write_text_field(&mut record[16..24], &event.symbol);
        write_text_field(&mut record[24..32], &event.venue);

        match &event.event {
            Event::AddOrder {
                order_id,
                side,
                price_ticks,
                qty,
            } => {
                record[32] = 1;
                let payload = &mut record[33..65];
                payload[0..8].copy_from_slice(&order_id.0.to_le_bytes());
                payload[8] = side.as_u8();
                payload[9..17].copy_from_slice(&price_ticks.to_le_bytes());
                payload[17..25].copy_from_slice(&qty.to_le_bytes());
            }
            Event::ModifyOrder { order_id, new_qty } => {
                record[32] = 2;
                let payload = &mut record[33..65];
                payload[0..8].copy_from_slice(&order_id.0.to_le_bytes());
                payload[8..16].copy_from_slice(&new_qty.to_le_bytes());
            }
            Event::CancelOrder {
                order_id,
                cancel_qty,
            } => {
                record[32] = 3;
                let payload = &mut record[33..65];
                payload[0..8].copy_from_slice(&order_id.0.to_le_bytes());
                payload[8..16].copy_from_slice(&cancel_qty.to_le_bytes());
            }
            Event::Trade {
                maker_order_id,
                taker_side,
                price_ticks,
                qty,
            } => {
                record[32] = 4;
                let payload = &mut record[33..65];
                payload[0..8].copy_from_slice(&maker_order_id.0.to_le_bytes());
                payload[8] = taker_side.as_u8();
                payload[9..17].copy_from_slice(&price_ticks.to_le_bytes());
                payload[17..25].copy_from_slice(&qty.to_le_bytes());
            }
            Event::BookSnapshotTop {
                bid_px,
                bid_sz,
                ask_px,
                ask_sz,
            } => {
                record[32] = 5;
                let payload = &mut record[33..65];
                payload[0..8].copy_from_slice(&bid_px.to_le_bytes());
                payload[8..16].copy_from_slice(&bid_sz.to_le_bytes());
                payload[16..24].copy_from_slice(&ask_px.to_le_bytes());
                payload[24..32].copy_from_slice(&ask_sz.to_le_bytes());
            }
        }
        bytes.extend_from_slice(&record);
    }
    Ok(bytes)
}

fn map_csv_row(row: CsvRow) -> Result<CanonicalEvent, IngestError> {
    let CsvRow {
        timestamp_ns,
        sequence,
        symbol,
        venue,
        event_type,
        order_id,
        side,
        price_ticks,
        qty,
        new_qty,
        cancel_qty,
        maker_order_id,
        taker_side,
        bid_px,
        bid_sz,
        ask_px,
        ask_sz,
    } = row;

    let event = match event_type.as_str() {
        "AddOrder" => Event::AddOrder {
            order_id: OrderId(required_u64(order_id, "order_id")?),
            side: parse_side(required_string(side, "side")?)?,
            price_ticks: required_u64(price_ticks, "price_ticks")?,
            qty: required_u64(qty, "qty")?,
        },
        "ModifyOrder" => Event::ModifyOrder {
            order_id: OrderId(required_u64(order_id, "order_id")?),
            new_qty: required_u64(new_qty, "new_qty")?,
        },
        "CancelOrder" => Event::CancelOrder {
            order_id: OrderId(required_u64(order_id, "order_id")?),
            cancel_qty: required_u64(cancel_qty, "cancel_qty")?,
        },
        "Trade" => Event::Trade {
            maker_order_id: OrderId(required_u64(maker_order_id, "maker_order_id")?),
            taker_side: parse_side(required_string(taker_side, "taker_side")?)?,
            price_ticks: required_u64(price_ticks, "price_ticks")?,
            qty: required_u64(qty, "qty")?,
        },
        "BookSnapshotTop" => Event::BookSnapshotTop {
            bid_px: required_u64(bid_px, "bid_px")?,
            bid_sz: required_u64(bid_sz, "bid_sz")?,
            ask_px: required_u64(ask_px, "ask_px")?,
            ask_sz: required_u64(ask_sz, "ask_sz")?,
        },
        other => return Err(IngestError::InvalidEventType(other.to_owned())),
    };

    let event = CanonicalEvent::new(timestamp_ns, sequence, symbol, venue, event);
    event.validate()?;
    Ok(event)
}

fn parse_record(record: &[u8]) -> Result<CanonicalEvent, IngestError> {
    if record.len() != BINARY_RECORD_LEN {
        return Err(IngestError::InvalidBinaryLength);
    }
    let timestamp_ns = u64::from_le_bytes(record[0..8].try_into().expect("slice len"));
    let sequence = u64::from_le_bytes(record[8..16].try_into().expect("slice len"));
    let symbol = parse_text_field(&record[16..24])?;
    let venue = parse_text_field(&record[24..32])?;
    let tag = record[32];
    let payload = &record[33..65];

    let event = match tag {
        1 => Event::AddOrder {
            order_id: OrderId(u64::from_le_bytes(
                payload[0..8].try_into().expect("slice len"),
            )),
            side: Side::from_u8(payload[8])?,
            price_ticks: u64::from_le_bytes(payload[9..17].try_into().expect("slice len")),
            qty: u64::from_le_bytes(payload[17..25].try_into().expect("slice len")),
        },
        2 => Event::ModifyOrder {
            order_id: OrderId(u64::from_le_bytes(
                payload[0..8].try_into().expect("slice len"),
            )),
            new_qty: u64::from_le_bytes(payload[8..16].try_into().expect("slice len")),
        },
        3 => Event::CancelOrder {
            order_id: OrderId(u64::from_le_bytes(
                payload[0..8].try_into().expect("slice len"),
            )),
            cancel_qty: u64::from_le_bytes(payload[8..16].try_into().expect("slice len")),
        },
        4 => Event::Trade {
            maker_order_id: OrderId(u64::from_le_bytes(
                payload[0..8].try_into().expect("slice len"),
            )),
            taker_side: Side::from_u8(payload[8])?,
            price_ticks: u64::from_le_bytes(payload[9..17].try_into().expect("slice len")),
            qty: u64::from_le_bytes(payload[17..25].try_into().expect("slice len")),
        },
        5 => Event::BookSnapshotTop {
            bid_px: u64::from_le_bytes(payload[0..8].try_into().expect("slice len")),
            bid_sz: u64::from_le_bytes(payload[8..16].try_into().expect("slice len")),
            ask_px: u64::from_le_bytes(payload[16..24].try_into().expect("slice len")),
            ask_sz: u64::from_le_bytes(payload[24..32].try_into().expect("slice len")),
        },
        other => return Err(IngestError::InvalidEventType(other.to_string())),
    };

    let event = CanonicalEvent::new(timestamp_ns, sequence, symbol, venue, event);
    event.validate()?;
    Ok(event)
}

fn read_exact_or_eof(reader: &mut impl Read, buffer: &mut [u8]) -> Result<Option<()>, IngestError> {
    let mut read_total = 0;
    while read_total < buffer.len() {
        let bytes_read = reader.read(&mut buffer[read_total..])?;
        if bytes_read == 0 {
            if read_total == 0 {
                return Ok(None);
            }
            return Err(IngestError::TruncatedBinary);
        }
        read_total += bytes_read;
    }
    Ok(Some(()))
}

fn write_text_field(target: &mut [u8], value: &str) {
    let bytes = value.as_bytes();
    let len = bytes.len().min(target.len());
    target[..len].copy_from_slice(&bytes[..len]);
}

fn parse_text_field(bytes: &[u8]) -> Result<String, IngestError> {
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end])
        .map(str::to_owned)
        .map_err(|_| IngestError::InvalidText)
}

fn required_u64(value: Option<u64>, field: &'static str) -> Result<u64, IngestError> {
    value.ok_or(IngestError::MissingField(field))
}

fn required_string(value: Option<String>, field: &'static str) -> Result<String, IngestError> {
    value.ok_or(IngestError::MissingField(field))
}

fn parse_side(value: String) -> Result<Side, IngestError> {
    match value.as_str() {
        "Bid" => Ok(Side::Bid),
        "Ask" => Ok(Side::Ask),
        _ => Err(IngestError::InvalidSide(value)),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use exchange_lab_core::{CanonicalEvent, Event, OrderId, Side};
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use tempfile::tempdir;

    use super::{encode_binary_feed, parse_binary_events_b, parse_binary_feed_a, parse_csv_file};

    fn sample_event(sequence: u64) -> CanonicalEvent {
        CanonicalEvent::new(
            1_000 + sequence,
            sequence,
            "XBTUSD",
            "LAB",
            Event::AddOrder {
                order_id: OrderId(sequence),
                side: if sequence.is_multiple_of(2) {
                    Side::Bid
                } else {
                    Side::Ask
                },
                price_ticks: 100 + sequence,
                qty: 1 + sequence,
            },
        )
    }

    fn random_event(rng: &mut StdRng, sequence: u64) -> CanonicalEvent {
        let event = match rng.gen_range(0..5) {
            0 => Event::AddOrder {
                order_id: OrderId(sequence + 1_000),
                side: if rng.gen_bool(0.5) {
                    Side::Bid
                } else {
                    Side::Ask
                },
                price_ticks: rng.gen_range(90..110),
                qty: rng.gen_range(1..20),
            },
            1 => Event::ModifyOrder {
                order_id: OrderId(rng.gen_range(1_000..1_100)),
                new_qty: rng.gen_range(1..20),
            },
            2 => Event::CancelOrder {
                order_id: OrderId(rng.gen_range(1_000..1_100)),
                cancel_qty: rng.gen_range(1..20),
            },
            3 => Event::Trade {
                maker_order_id: OrderId(rng.gen_range(1_000..1_100)),
                taker_side: if rng.gen_bool(0.5) {
                    Side::Bid
                } else {
                    Side::Ask
                },
                price_ticks: rng.gen_range(90..110),
                qty: rng.gen_range(1..20),
            },
            _ => Event::BookSnapshotTop {
                bid_px: rng.gen_range(90..100),
                bid_sz: rng.gen_range(1..20),
                ask_px: rng.gen_range(100..110),
                ask_sz: rng.gen_range(1..20),
            },
        };
        CanonicalEvent::new(1_000 + sequence, sequence, "XBTUSD", "LAB", event)
    }

    #[test]
    fn parses_csv_fixture() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("fixture.csv");
        fs::write(
            &path,
            "timestamp_ns,sequence,symbol,venue,event_type,order_id,side,price_ticks,qty,new_qty,cancel_qty,maker_order_id,taker_side,bid_px,bid_sz,ask_px,ask_sz\n1,1,XBTUSD,LAB,AddOrder,1,Bid,100,2,,,,,,,,\n",
        )
        .expect("write fixture");

        let parsed = parse_csv_file(&path).expect("parse csv");
        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0], sample_event(1));
    }

    #[test]
    fn parser_a_and_b_match_on_randomized_corpus() {
        let mut rng = StdRng::seed_from_u64(42);
        let corpus: Vec<_> = (1..=256)
            .map(|sequence| random_event(&mut rng, sequence))
            .collect();
        let encoded = encode_binary_feed(&corpus).expect("encode");
        let parsed_a = parse_binary_feed_a(encoded.as_slice()).expect("parse a");
        let parsed_b = parse_binary_events_b(&encoded).expect("parse b");
        assert_eq!(parsed_a.events, parsed_b);
        assert_eq!(parsed_a.events, corpus);
    }
}
