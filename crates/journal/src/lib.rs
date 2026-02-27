use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use exchange_lab_core::{CanonicalEvent, EngineSnapshot};
use thiserror::Error;

const JOURNAL_MAGIC: &[u8; 8] = b"EXLABJNL";
const INDEX_MAGIC: &[u8; 8] = b"EXLABIDX";
const SNAPSHOT_MAGIC: &[u8; 8] = b"EXLABSNP";
const VERSION: u16 = 1;
const HEADER_LEN: u64 = 10;
const INDEX_ENTRY_LEN: usize = 24;

#[derive(Debug, Clone, Copy)]
pub struct JournalConfig {
    pub index_stride: u64,
    pub writer_capacity: usize,
}

impl Default for JournalConfig {
    fn default() -> Self {
        Self {
            index_stride: 64,
            writer_capacity: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub sequence: u64,
    pub timestamp_ns: u64,
    pub byte_offset: u64,
}

#[derive(Debug, Error)]
pub enum JournalError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serialize(#[from] Box<bincode::ErrorKind>),
    #[error("invalid {0} header")]
    InvalidHeader(&'static str),
    #[error("record payload too large: {0} bytes")]
    RecordTooLarge(usize),
    #[error("crc mismatch at byte offset {offset}")]
    CrcMismatch { offset: u64 },
    #[error("truncated record at byte offset {0}")]
    TruncatedRecord(u64),
    #[error("corrupt index file")]
    CorruptIndex,
}

pub struct JournalWriter {
    log: BufWriter<File>,
    index: BufWriter<File>,
    config: JournalConfig,
    records_written: u64,
    next_offset: u64,
}

impl JournalWriter {
    pub fn create(
        log_path: impl AsRef<Path>,
        index_path: impl AsRef<Path>,
        config: JournalConfig,
    ) -> Result<Self, JournalError> {
        let mut log = BufWriter::with_capacity(config.writer_capacity, File::create(log_path)?);
        let mut index = BufWriter::with_capacity(config.writer_capacity, File::create(index_path)?);
        write_header(&mut log, JOURNAL_MAGIC)?;
        write_header(&mut index, INDEX_MAGIC)?;
        Ok(Self {
            log,
            index,
            config,
            records_written: 0,
            next_offset: HEADER_LEN,
        })
    }

    pub fn append(&mut self, event: &CanonicalEvent) -> Result<u64, JournalError> {
        let payload = bincode::serialize(event)?;
        let payload_len: u32 = payload
            .len()
            .try_into()
            .map_err(|_| JournalError::RecordTooLarge(payload.len()))?;
        let offset = self.next_offset;
        let crc = crc32fast::hash(&payload);

        self.log.write_all(&payload_len.to_le_bytes())?;
        self.log.write_all(&crc.to_le_bytes())?;
        self.log.write_all(&payload)?;

        if self.config.index_stride == 0
            || self
                .records_written
                .is_multiple_of(self.config.index_stride)
        {
            let entry = IndexEntry {
                sequence: event.sequence,
                timestamp_ns: event.timestamp_ns,
                byte_offset: offset,
            };
            self.index.write_all(&entry.sequence.to_le_bytes())?;
            self.index.write_all(&entry.timestamp_ns.to_le_bytes())?;
            self.index.write_all(&entry.byte_offset.to_le_bytes())?;
        }

        self.records_written += 1;
        self.next_offset += 8 + u64::from(payload_len);
        Ok(offset)
    }

    pub fn flush(&mut self) -> Result<(), JournalError> {
        self.log.flush()?;
        self.index.flush()?;
        Ok(())
    }
}

pub struct JournalReader {
    log: BufReader<File>,
    index: Vec<IndexEntry>,
}

impl JournalReader {
    pub fn open(
        log_path: impl AsRef<Path>,
        index_path: impl AsRef<Path>,
    ) -> Result<Self, JournalError> {
        let mut log = BufReader::new(File::open(log_path)?);
        validate_header(&mut log, JOURNAL_MAGIC, "journal")?;
        let index = load_index(index_path)?;
        Ok(Self { log, index })
    }

    pub fn read_all(&mut self) -> Result<Vec<CanonicalEvent>, JournalError> {
        self.log.seek(SeekFrom::Start(HEADER_LEN))?;
        self.read_from_current()
    }

    pub fn read_from_sequence(
        &mut self,
        sequence: u64,
    ) -> Result<Vec<CanonicalEvent>, JournalError> {
        let offset = self
            .index
            .iter()
            .rev()
            .find(|entry| entry.sequence <= sequence)
            .map_or(HEADER_LEN, |entry| entry.byte_offset);
        self.log.seek(SeekFrom::Start(offset))?;

        let mut events = Vec::new();
        for event in self.read_from_current()? {
            if event.sequence >= sequence {
                events.push(event);
            }
        }
        Ok(events)
    }

    fn read_from_current(&mut self) -> Result<Vec<CanonicalEvent>, JournalError> {
        let mut events = Vec::new();
        while let Some(event) = read_record(&mut self.log)? {
            events.push(event);
        }
        Ok(events)
    }
}

pub fn write_snapshot(
    path: impl AsRef<Path>,
    snapshot: &EngineSnapshot,
) -> Result<(), JournalError> {
    let payload = bincode::serialize(snapshot)?;
    let payload_len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| JournalError::RecordTooLarge(payload.len()))?;
    let crc = crc32fast::hash(&payload);

    let mut writer = BufWriter::new(File::create(path)?);
    write_header(&mut writer, SNAPSHOT_MAGIC)?;
    writer.write_all(&payload_len.to_le_bytes())?;
    writer.write_all(&crc.to_le_bytes())?;
    writer.write_all(&payload)?;
    writer.flush()?;
    Ok(())
}

pub fn read_snapshot(path: impl AsRef<Path>) -> Result<EngineSnapshot, JournalError> {
    let mut reader = BufReader::new(File::open(path)?);
    validate_header(&mut reader, SNAPSHOT_MAGIC, "snapshot")?;
    let start = HEADER_LEN;
    read_snapshot_payload(&mut reader, start)
}

pub fn load_snapshot_and_tail(
    snapshot_path: impl AsRef<Path>,
    log_path: impl AsRef<Path>,
    index_path: impl AsRef<Path>,
) -> Result<(EngineSnapshot, Vec<CanonicalEvent>), JournalError> {
    let snapshot = read_snapshot(snapshot_path)?;
    let mut reader = JournalReader::open(log_path, index_path)?;
    let tail = reader.read_from_sequence(snapshot.cursor_sequence.saturating_add(1))?;
    Ok((snapshot, tail))
}

fn write_header(writer: &mut impl Write, magic: &[u8; 8]) -> Result<(), io::Error> {
    writer.write_all(magic)?;
    writer.write_all(&VERSION.to_le_bytes())?;
    Ok(())
}

fn validate_header(
    reader: &mut impl Read,
    magic: &[u8; 8],
    label: &'static str,
) -> Result<(), JournalError> {
    let mut magic_buf = [0_u8; 8];
    let mut version_buf = [0_u8; 2];
    reader.read_exact(&mut magic_buf)?;
    reader.read_exact(&mut version_buf)?;
    let version = u16::from_le_bytes(version_buf);
    if &magic_buf != magic || version != VERSION {
        return Err(JournalError::InvalidHeader(label));
    }
    Ok(())
}

fn load_index(path: impl AsRef<Path>) -> Result<Vec<IndexEntry>, JournalError> {
    let mut reader = BufReader::new(File::open(path)?);
    validate_header(&mut reader, INDEX_MAGIC, "index")?;
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes)?;
    if !bytes.len().is_multiple_of(INDEX_ENTRY_LEN) {
        return Err(JournalError::CorruptIndex);
    }
    let mut entries = Vec::with_capacity(bytes.len() / INDEX_ENTRY_LEN);
    for chunk in bytes.chunks_exact(INDEX_ENTRY_LEN) {
        let sequence = u64::from_le_bytes(chunk[0..8].try_into().expect("slice len"));
        let timestamp_ns = u64::from_le_bytes(chunk[8..16].try_into().expect("slice len"));
        let byte_offset = u64::from_le_bytes(chunk[16..24].try_into().expect("slice len"));
        entries.push(IndexEntry {
            sequence,
            timestamp_ns,
            byte_offset,
        });
    }
    Ok(entries)
}

fn read_record(reader: &mut BufReader<File>) -> Result<Option<CanonicalEvent>, JournalError> {
    let offset = reader.stream_position()?;
    let mut len_buf = [0_u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(JournalError::Io(error)),
    }

    let mut crc_buf = [0_u8; 4];
    if let Err(error) = reader.read_exact(&mut crc_buf) {
        return if error.kind() == io::ErrorKind::UnexpectedEof {
            Err(JournalError::TruncatedRecord(offset))
        } else {
            Err(JournalError::Io(error))
        };
    }

    let payload_len = u32::from_le_bytes(len_buf) as usize;
    let expected_crc = u32::from_le_bytes(crc_buf);
    let mut payload = vec![0_u8; payload_len];
    if let Err(error) = reader.read_exact(&mut payload) {
        return if error.kind() == io::ErrorKind::UnexpectedEof {
            Err(JournalError::TruncatedRecord(offset))
        } else {
            Err(JournalError::Io(error))
        };
    }

    if crc32fast::hash(&payload) != expected_crc {
        return Err(JournalError::CrcMismatch { offset });
    }

    let event = bincode::deserialize(&payload)?;
    Ok(Some(event))
}

fn read_snapshot_payload(
    reader: &mut BufReader<File>,
    offset: u64,
) -> Result<EngineSnapshot, JournalError> {
    let mut len_buf = [0_u8; 4];
    let mut crc_buf = [0_u8; 4];
    reader.read_exact(&mut len_buf)?;
    reader.read_exact(&mut crc_buf)?;
    let payload_len = u32::from_le_bytes(len_buf) as usize;
    let expected_crc = u32::from_le_bytes(crc_buf);
    let mut payload = vec![0_u8; payload_len];
    reader.read_exact(&mut payload)?;
    if crc32fast::hash(&payload) != expected_crc {
        return Err(JournalError::CrcMismatch { offset });
    }
    Ok(bincode::deserialize(&payload)?)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use exchange_lab_core::{CanonicalEvent, Event, OrderId, Side};
    use tempfile::tempdir;

    use super::{
        load_snapshot_and_tail, read_snapshot, write_snapshot, JournalConfig, JournalReader,
        JournalWriter,
    };

    fn event(sequence: u64) -> CanonicalEvent {
        CanonicalEvent::new(
            sequence,
            sequence,
            "XBTUSD",
            "LAB",
            Event::AddOrder {
                order_id: OrderId(sequence),
                side: Side::Bid,
                price_ticks: 100 + sequence,
                qty: 1,
            },
        )
    }

    fn paths(dir: &Path) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
        (
            dir.join("events.log"),
            dir.join("events.idx"),
            dir.join("snapshot.bin"),
        )
    }

    #[test]
    fn round_trips_journal_and_snapshot() {
        let dir = tempdir().expect("tempdir");
        let (log_path, index_path, snapshot_path) = paths(dir.path());
        let mut writer = JournalWriter::create(
            &log_path,
            &index_path,
            JournalConfig {
                index_stride: 1,
                writer_capacity: 8 * 1024,
            },
        )
        .expect("writer");
        writer.append(&event(1)).expect("append 1");
        writer.append(&event(2)).expect("append 2");
        writer.flush().expect("flush");

        let snapshot = exchange_lab_core::EngineSnapshot {
            cursor_sequence: 1,
            orders: Vec::new(),
        };
        write_snapshot(&snapshot_path, &snapshot).expect("snapshot write");
        let loaded_snapshot = read_snapshot(&snapshot_path).expect("snapshot read");
        assert_eq!(loaded_snapshot.cursor_sequence, 1);

        let mut reader = JournalReader::open(&log_path, &index_path).expect("reader");
        let all = reader.read_all().expect("read all");
        assert_eq!(all.len(), 2);

        let (_, tail) =
            load_snapshot_and_tail(&snapshot_path, &log_path, &index_path).expect("tail");
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].sequence, 2);
    }
}
