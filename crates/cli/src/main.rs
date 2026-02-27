use std::fs;
use std::io::{self, BufRead};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use clap::{Parser, Subcommand, ValueEnum};
use exchange_lab_api::{serve, ApiState};
use exchange_lab_core::{CanonicalEvent, EngineSnapshot, Event};
use exchange_lab_engine::MatchingEngine;
use exchange_lab_ingest::{parse_binary_file_a, parse_binary_file_b, parse_csv_file};
use exchange_lab_journal::{
    load_snapshot_and_tail, write_snapshot, JournalConfig, JournalReader, JournalWriter,
};
use exchange_lab_metrics::{ExchangeMetrics, MetricsError, DEFAULT_METRICS};
use exchange_lab_replay::{
    replay_events, replay_events_with_stepper, replay_journal, FaultInjection, ReplayMode,
};
use tempfile::tempdir;
use thiserror::Error;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "exchange-lab")]
#[command(about = "Deterministic event-sourced matching engine lab")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    IngestCsv {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        index: PathBuf,
        #[arg(long, default_value_t = 64)]
        index_stride: u64,
    },
    IngestBin {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        index: PathBuf,
        #[arg(long, value_enum, default_value_t = BinaryParser::A)]
        parser: BinaryParser,
        #[arg(long, default_value_t = 64)]
        index_stride: u64,
    },
    RunEngine {
        #[arg(long)]
        input_log: PathBuf,
        #[arg(long)]
        input_index: PathBuf,
        #[arg(long)]
        output_log: PathBuf,
        #[arg(long)]
        output_index: PathBuf,
        #[arg(long, default_value_t = 64)]
        index_stride: u64,
    },
    Replay {
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        index: PathBuf,
        #[arg(long, value_enum, default_value_t = ReplayModeArg::MaxSpeed)]
        mode: ReplayModeArg,
        #[arg(long, default_value_t = 1.0)]
        speed: f64,
        #[arg(long, default_value_t = 0.0)]
        packet_loss_pct: f64,
        #[arg(long, default_value_t = 0)]
        reorder_window: usize,
        #[arg(long, default_value_t = 0)]
        timestamp_skew_ns: i64,
        #[arg(long, default_value_t = 7)]
        seed: u64,
    },
    VerifyDeterminism {
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        index: PathBuf,
        #[arg(long, default_value_t = 0.0)]
        packet_loss_pct: f64,
        #[arg(long, default_value_t = 0)]
        reorder_window: usize,
        #[arg(long, default_value_t = 0)]
        timestamp_skew_ns: i64,
        #[arg(long, default_value_t = 7)]
        seed: u64,
    },
    Snapshot {
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        index: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    Resume {
        #[arg(long)]
        snapshot: PathBuf,
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        index: PathBuf,
    },
    Bench {
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        index: PathBuf,
        #[arg(long, default_value_t = 10)]
        iterations: u64,
    },
    Serve {
        #[arg(long)]
        log: PathBuf,
        #[arg(long)]
        index: PathBuf,
        #[arg(long)]
        snapshot: Option<PathBuf>,
        #[arg(long, default_value = "127.0.0.1:50051")]
        grpc_addr: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:8080")]
        http_addr: SocketAddr,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum BinaryParser {
    A,
    B,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReplayModeArg {
    MaxSpeed,
    Paced,
    Step,
}

#[derive(Debug, Error)]
enum CliError {
    #[error("ingest error: {0}")]
    Ingest(#[from] exchange_lab_ingest::IngestError),
    #[error("journal error: {0}")]
    Journal(#[from] exchange_lab_journal::JournalError),
    #[error("engine error: {0}")]
    Engine(#[from] exchange_lab_engine::EngineError),
    #[error("replay error: {0}")]
    Replay(#[from] exchange_lab_replay::ReplayError),
    #[error("api error: {0}")]
    Api(#[from] exchange_lab_api::ApiError),
    #[error("metrics error: {0}")]
    Metrics(#[from] MetricsError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

fn main() -> Result<(), CliError> {
    init_tracing();
    let args = Args::parse();

    match args.command {
        Command::IngestCsv {
            input,
            log,
            index,
            index_stride,
        } => ingest_csv(&input, &log, &index, index_stride)?,
        Command::IngestBin {
            input,
            log,
            index,
            parser,
            index_stride,
        } => ingest_bin(&input, &log, &index, parser, index_stride)?,
        Command::RunEngine {
            input_log,
            input_index,
            output_log,
            output_index,
            index_stride,
        } => run_engine(
            &input_log,
            &input_index,
            &output_log,
            &output_index,
            index_stride,
        )?,
        Command::Replay {
            log,
            index,
            mode,
            speed,
            packet_loss_pct,
            reorder_window,
            timestamp_skew_ns,
            seed,
        } => replay_command(
            &log,
            &index,
            mode,
            speed,
            FaultInjection {
                packet_loss_pct,
                reorder_window,
                timestamp_skew_ns,
                seed,
            },
        )?,
        Command::VerifyDeterminism {
            log,
            index,
            packet_loss_pct,
            reorder_window,
            timestamp_skew_ns,
            seed,
        } => verify_determinism(
            &log,
            &index,
            FaultInjection {
                packet_loss_pct,
                reorder_window,
                timestamp_skew_ns,
                seed,
            },
        )?,
        Command::Snapshot { log, index, output } => snapshot_command(&log, &index, &output)?,
        Command::Resume {
            snapshot,
            log,
            index,
        } => resume_command(&snapshot, &log, &index)?,
        Command::Bench {
            log,
            index,
            iterations,
        } => bench_command(&log, &index, iterations)?,
        Command::Serve {
            log,
            index,
            snapshot,
            grpc_addr,
            http_addr,
        } => serve_command(&log, &index, snapshot.as_deref(), grpc_addr, http_addr)?,
    }

    Ok(())
}

fn ingest_csv(input: &Path, log: &Path, index: &Path, index_stride: u64) -> Result<(), CliError> {
    let parsed = parse_csv_file(input)?;
    DEFAULT_METRICS.record_ingest(parsed.events.len(), parsed.stats.elapsed_ns);
    write_events(log, index, index_stride, &parsed.events)?;
    info!(events = parsed.events.len(), input = %input.display(), "ingested csv events");
    println!("ingested {} csv events", parsed.events.len());
    Ok(())
}

fn ingest_bin(
    input: &Path,
    log: &Path,
    index: &Path,
    parser: BinaryParser,
    index_stride: u64,
) -> Result<(), CliError> {
    let parsed = match parser {
        BinaryParser::A => parse_binary_file_a(input)?,
        BinaryParser::B => parse_binary_file_b(input)?,
    };
    DEFAULT_METRICS.record_ingest(parsed.events.len(), parsed.stats.elapsed_ns);
    write_events(log, index, index_stride, &parsed.events)?;
    info!(events = parsed.events.len(), input = %input.display(), "ingested binary events");
    println!("ingested {} binary events", parsed.events.len());
    Ok(())
}

fn run_engine(
    input_log: &Path,
    input_index: &Path,
    output_log: &Path,
    output_index: &Path,
    index_stride: u64,
) -> Result<(), CliError> {
    let mut reader = JournalReader::open(input_log, input_index)?;
    let events = reader.read_all()?;
    let mut writer = JournalWriter::create(output_log, output_index, journal_config(index_stride))?;
    let mut engine = MatchingEngine::new();
    let mut next_sequence = 1_u64;

    for event in &events {
        let normalized = resequence_event(event, next_sequence);
        next_sequence += 1;
        writer.append(&normalized)?;
        if matches!(
            normalized.event,
            Event::AddOrder { .. } | Event::ModifyOrder { .. } | Event::CancelOrder { .. }
        ) {
            for derived in engine.process(&normalized)? {
                let normalized_derived = resequence_event(&derived, next_sequence);
                next_sequence += 1;
                writer.append(&normalized_derived)?;
            }
        }
    }

    writer.flush()?;
    println!("processed {} input events", events.len());
    Ok(())
}

fn replay_command(
    log: &Path,
    index: &Path,
    mode: ReplayModeArg,
    speed: f64,
    fault: FaultInjection,
) -> Result<(), CliError> {
    let replay_mode = match mode {
        ReplayModeArg::MaxSpeed => ReplayMode::MaxSpeed,
        ReplayModeArg::Paced => ReplayMode::Paced { speed },
        ReplayModeArg::Step => ReplayMode::Step,
    };

    let result = if matches!(replay_mode, ReplayMode::Step) {
        let events = load_events(log, index)?;
        let stdin = io::stdin();
        let mut handle = stdin.lock();
        replay_events_with_stepper(&events, replay_mode, fault, |event| {
            println!(
                "step sequence={} kind={}",
                event.sequence,
                event.event.kind()
            );
            let mut buffer = String::new();
            let _ = handle.read_line(&mut buffer);
        })?
    } else {
        replay_journal(log, index, replay_mode, fault)?
    };

    DEFAULT_METRICS.record_replay(
        result.emitted_events.len(),
        result.stats.elapsed_ns,
        result.failures.len(),
    );
    println!(
        "replayed {} events with {} invariant failures",
        result.stats.events_replayed, result.stats.invariant_failures
    );
    Ok(())
}

fn verify_determinism(log: &Path, index: &Path, fault: FaultInjection) -> Result<(), CliError> {
    let events = load_events(log, index)?;
    let first = replay_events(&events, ReplayMode::MaxSpeed, fault)?;
    let second = replay_events(&events, ReplayMode::MaxSpeed, fault)?;

    let dir = tempdir()?;
    let first_log = dir.path().join("first.log");
    let first_index = dir.path().join("first.idx");
    let second_log = dir.path().join("second.log");
    let second_index = dir.path().join("second.idx");

    let first_normalized = normalize_event_stream(&first.emitted_events);
    let second_normalized = normalize_event_stream(&second.emitted_events);
    write_events(&first_log, &first_index, 1, &first_normalized)?;
    write_events(&second_log, &second_index, 1, &second_normalized)?;

    let same_log = fs::read(&first_log)? == fs::read(&second_log)?;
    let same_index = fs::read(&first_index)? == fs::read(&second_index)?;
    println!("deterministic={}", same_log && same_index);
    Ok(())
}

fn snapshot_command(log: &Path, index: &Path, output: &Path) -> Result<(), CliError> {
    let events = load_events(log, index)?;
    let snapshot = build_snapshot(&events)?;
    write_snapshot(output, &snapshot)?;
    println!(
        "snapshot written: cursor_sequence={} resting_orders={}",
        snapshot.cursor_sequence,
        snapshot.orders.len()
    );
    Ok(())
}

fn resume_command(snapshot: &Path, log: &Path, index: &Path) -> Result<(), CliError> {
    let (loaded_snapshot, tail) = load_snapshot_and_tail(snapshot, log, index)?;
    let mut engine = MatchingEngine::load_snapshot(&loaded_snapshot)?;
    for event in &tail {
        if matches!(
            event.event,
            Event::AddOrder { .. } | Event::ModifyOrder { .. } | Event::CancelOrder { .. }
        ) {
            let _ = engine.process(event)?;
        }
    }

    let final_snapshot = engine.snapshot(
        tail.last()
            .map_or(loaded_snapshot.cursor_sequence, |event| event.sequence),
    );
    println!(
        "resumed cursor={} resting_orders={}",
        final_snapshot.cursor_sequence,
        final_snapshot.orders.len()
    );
    Ok(())
}

fn bench_command(log: &Path, index: &Path, iterations: u64) -> Result<(), CliError> {
    let started = Instant::now();
    let mut total_events = 0_u64;
    for _ in 0..iterations {
        let result = replay_journal(log, index, ReplayMode::MaxSpeed, FaultInjection::default())?;
        total_events += result.stats.events_replayed;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let throughput = if elapsed > 0.0 {
        total_events as f64 / elapsed
    } else {
        0.0
    };
    println!("iterations={} throughput_eps={:.2}", iterations, throughput);
    Ok(())
}

fn serve_command(
    log: &Path,
    index: &Path,
    snapshot: Option<&Path>,
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
) -> Result<(), CliError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let metrics = Arc::new(ExchangeMetrics::new()?);
    let state = Arc::new(ApiState::new(
        log.to_path_buf(),
        index.to_path_buf(),
        snapshot.map(std::path::Path::to_path_buf),
        metrics,
    ));
    println!(
        "serving dashboard at http://{} and gRPC at {}",
        http_addr, grpc_addr
    );
    runtime.block_on(serve(grpc_addr, http_addr, state))?;
    Ok(())
}

fn write_events(
    log: &Path,
    index: &Path,
    index_stride: u64,
    events: &[CanonicalEvent],
) -> Result<(), CliError> {
    let mut writer = JournalWriter::create(log, index, journal_config(index_stride))?;
    for event in events {
        writer.append(event)?;
    }
    writer.flush()?;
    Ok(())
}

fn load_events(log: &Path, index: &Path) -> Result<Vec<CanonicalEvent>, CliError> {
    let mut reader = JournalReader::open(log, index)?;
    Ok(reader.read_all()?)
}

fn build_snapshot(events: &[CanonicalEvent]) -> Result<EngineSnapshot, CliError> {
    let mut engine = MatchingEngine::new();
    let mut last_sequence = 0;
    for event in events {
        if matches!(
            event.event,
            Event::AddOrder { .. } | Event::ModifyOrder { .. } | Event::CancelOrder { .. }
        ) {
            let _ = engine.process(event)?;
            last_sequence = event.sequence;
        }
    }
    Ok(engine.snapshot(last_sequence))
}

fn journal_config(index_stride: u64) -> JournalConfig {
    JournalConfig {
        index_stride: index_stride.max(1),
        ..JournalConfig::default()
    }
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

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(io::stderr)
        .try_init();
}
