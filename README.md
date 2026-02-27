# exchange-lab

Deterministic event-sourced matching engine + replay + fault-injection lab

`exchange-lab` is a systems-engineering showcase for event-driven market microstructure in Rust. It is intentionally offline-only: no brokerage flows, no exchange connectivity, no live order routing.

## Architecture

```text
            +-------------------+
CSV / BIN ->| ingest            |-- CanonicalEvent -->+-------------------+
            +-------------------+                     | journal           |
                                                       | [Header][Len][CRC]|
                                                       | + sparse index    |
                                                       +---------+---------+
                                                                 |
                                                                 v
            +-------------------+                     +-------------------+
            | engine            |<-- replay stream ---| replay            |
            | price-time book   |                     | pacing + faults   |
            | partial fills     |                     | invariants        |
            +---------+---------+                     +---------+---------+
                      |                                         |
                      v                                         v
            +-------------------+                     +-------------------+
            | snapshot          |                     | api + metrics     |
            | full book state   |                     | gRPC /healthz     |
            | cursor sequence   |                     | /readyz /metrics  |
            +-------------------+                     +-------------------+
```

## Workspace Layout

```text
crates/
  core/
  ingest/
  engine/
  journal/
  replay/
  api/
  cli/
  metrics/
data/
tests/
```

## Deterministic Guarantees

- Canonical events are explicit and serializable with stable field order.
- Journal records are append-only and protected by CRC32.
- Sparse index entries pin `sequence`, `timestamp_ns`, and byte offset every `N` records.
- Snapshots contain the full resting-book state plus the cursor sequence.
- Replay fault injection is seed-driven; the same seed and input produce the same reordered or dropped stream.
- CLI-generated derived journals normalize sequences to a single monotonic stream.
- Determinism tests compare byte-for-byte journal and index output.

## Performance Notes

- Hot paths use `BufReader` and `BufWriter`.
- Binary ingest uses a fixed-width safe parser and avoids `unsafe`.
- The book uses `BTreeMap` price levels with `VecDeque` FIFO queues for deterministic price-time priority.
- Replay pacing is optional; `max-speed` mode does no intentional sleeping.
- Prometheus histograms track parse and replay latency in nanoseconds.

## Failure Modes Tested

- CRC mismatch detection in journal and snapshot payloads.
- Resume from snapshot plus journal tail.
- Packet loss percentage during replay.
- Local reordering windows during replay.
- Timestamp skew injection.
- Parser differential checks between two independent binary parsers.
- Randomized property coverage for engine invariants.

## Build And Test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Exact Run Commands

Ingest CSV into a raw journal:

```bash
cargo run -p exchange-lab-cli -- ingest-csv \
  --input data/sample_events.csv \
  --log data/raw.log \
  --index data/raw.idx
```

Ingest the sample binary feed:

```bash
cargo run -p exchange-lab-cli -- ingest-bin \
  --input data/sample_feed.bin \
  --log data/bin.log \
  --index data/bin.idx \
  --parser a
```

Materialize a monotonic engine journal with derived trades and top-of-book snapshots:

```bash
cargo run -p exchange-lab-cli -- run-engine \
  --input-log data/raw.log \
  --input-index data/raw.idx \
  --output-log data/engine.log \
  --output-index data/engine.idx
```

Replay with deterministic faults:

```bash
cargo run -p exchange-lab-cli -- replay \
  --log data/raw.log \
  --index data/raw.idx \
  --mode paced \
  --speed 10.0 \
  --packet-loss-pct 2.5 \
  --reorder-window 3 \
  --timestamp-skew-ns 250 \
  --seed 42
```

Verify byte-identical deterministic output for a fixed seed:

```bash
cargo run -p exchange-lab-cli -- verify-determinism \
  --log data/raw.log \
  --index data/raw.idx \
  --packet-loss-pct 1.0 \
  --reorder-window 2 \
  --timestamp-skew-ns 100 \
  --seed 42
```

Write a snapshot:

```bash
cargo run -p exchange-lab-cli -- snapshot \
  --log data/raw.log \
  --index data/raw.idx \
  --output data/book.snapshot
```

Resume from snapshot plus journal tail:

```bash
cargo run -p exchange-lab-cli -- resume \
  --snapshot data/book.snapshot \
  --log data/raw.log \
  --index data/raw.idx
```

Benchmark replay throughput:

```bash
cargo run -p exchange-lab-cli -- bench \
  --log data/raw.log \
  --index data/raw.idx \
  --iterations 50
```

Run the local live dashboard:

```bash
cargo run -p exchange-lab-cli -- serve \
  --log data/engine.log \
  --index data/engine.idx \
  --snapshot data/book.snapshot \
  --grpc-addr 127.0.0.1:50051 \
  --http-addr 127.0.0.1:8080
```

On Windows, the one-click launcher is:

```powershell
.\run-live.bat
```

## API Surface

The `api` crate exposes:

- tonic gRPC streaming for replay events
- unary gRPC status and snapshot metadata endpoints
- REST `/`, `/healthz`, `/readyz`, and Prometheus `/metrics`

The server entrypoint is available through `exchange_lab_api::serve(...)` and the CLI `serve` command.
