use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use axum::{http::StatusCode, response::Html, routing::get, Router};
use exchange_lab_core::{CanonicalEvent, Event};
use exchange_lab_journal::read_snapshot;
use exchange_lab_metrics::ExchangeMetrics;
use exchange_lab_replay::{replay_journal, FaultInjection, ReplayMode};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

pub mod proto {
    tonic::include_proto!("exchange_lab");
}

use proto::exchange_lab_server::{ExchangeLab, ExchangeLabServer};
use proto::{
    EventKind, EventMessage, SnapshotMetaRequest, SnapshotMetaResponse, StatusRequest,
    StatusResponse, StreamReplayRequest,
};

#[derive(Debug)]
pub struct ApiState {
    journal_path: PathBuf,
    index_path: PathBuf,
    snapshot_path: Option<PathBuf>,
    started_at: Instant,
    ready: AtomicBool,
    metrics: Arc<ExchangeMetrics>,
}

impl ApiState {
    pub fn new(
        journal_path: impl Into<PathBuf>,
        index_path: impl Into<PathBuf>,
        snapshot_path: Option<PathBuf>,
        metrics: Arc<ExchangeMetrics>,
    ) -> Self {
        Self {
            journal_path: journal_path.into(),
            index_path: index_path.into(),
            snapshot_path,
            started_at: Instant::now(),
            ready: AtomicBool::new(false),
            metrics,
        }
    }

    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Relaxed);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("grpc transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

#[derive(Clone)]
pub struct ExchangeLabService {
    state: Arc<ApiState>,
}

impl ExchangeLabService {
    pub fn new(state: Arc<ApiState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl ExchangeLab for ExchangeLabService {
    type StreamReplayStream = ReceiverStream<Result<EventMessage, Status>>;

    async fn stream_replay(
        &self,
        _request: Request<StreamReplayRequest>,
    ) -> Result<Response<Self::StreamReplayStream>, Status> {
        let (tx, rx) = mpsc::channel(64);
        let state = self.state.clone();

        tokio::spawn(async move {
            let journal_path = state.journal_path.clone();
            let index_path = state.index_path.clone();
            let replay_result = tokio::task::spawn_blocking(move || {
                replay_journal(
                    journal_path,
                    index_path,
                    ReplayMode::MaxSpeed,
                    FaultInjection::default(),
                )
            })
            .await;

            match replay_result {
                Ok(Ok(result)) => {
                    state.metrics.record_replay(
                        result.emitted_events.len(),
                        result.stats.elapsed_ns,
                        result.failures.len(),
                    );
                    for event in result.emitted_events {
                        if tx.send(Ok(proto_event(&event))).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(Err(error)) => {
                    let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                }
                Err(error) => {
                    let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let response = StatusResponse {
            ready: self.state.is_ready(),
            uptime_ms: self
                .state
                .started_at
                .elapsed()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
            journal_path: self.state.journal_path.display().to_string(),
        };
        Ok(Response::new(response))
    }

    async fn snapshot_meta(
        &self,
        _request: Request<SnapshotMetaRequest>,
    ) -> Result<Response<SnapshotMetaResponse>, Status> {
        let response = match self.state.snapshot_path.as_deref() {
            Some(path) if path.exists() => {
                let snapshot =
                    read_snapshot(path).map_err(|error| Status::internal(error.to_string()))?;
                SnapshotMetaResponse {
                    available: true,
                    cursor_sequence: snapshot.cursor_sequence,
                    resting_orders: snapshot.orders.len() as u64,
                }
            }
            _ => SnapshotMetaResponse {
                available: false,
                cursor_sequence: 0,
                resting_orders: 0,
            },
        };
        Ok(Response::new(response))
    }
}

pub async fn serve(
    grpc_addr: SocketAddr,
    http_addr: SocketAddr,
    state: Arc<ApiState>,
) -> Result<(), ApiError> {
    let http_router = build_http_router(state.clone());
    let grpc_service = ExchangeLabServer::new(ExchangeLabService::new(state.clone()));
    state.mark_ready();

    let http_listener = TcpListener::bind(http_addr).await?;
    info!(
        %grpc_addr,
        %http_addr,
        journal_path = %state.journal_path.display(),
        index_path = %state.index_path.display(),
        "starting api servers"
    );

    let grpc = Server::builder().add_service(grpc_service).serve(grpc_addr);
    let http = axum::serve(http_listener, http_router);

    let (grpc_result, http_result) = tokio::join!(grpc, http);
    grpc_result?;
    http_result?;
    Ok(())
}

fn build_http_router(state: Arc<ApiState>) -> Router {
    let dashboard_state = state.clone();
    let ready_state = state.clone();
    Router::new()
        .route(
            "/",
            get(move || {
                let state = dashboard_state.clone();
                async move { Html(render_dashboard(&state)) }
            }),
        )
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route(
            "/readyz",
            get(move || {
                let state = ready_state.clone();
                async move {
                    if state.is_ready() {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        )
        .merge(ExchangeMetrics::router(state.metrics.clone()))
}

fn render_dashboard(state: &ApiState) -> String {
    let snapshot = match state.snapshot_path.as_deref() {
        Some(path) if path.exists() => match read_snapshot(path) {
            Ok(snapshot) => format!(
                "<li>Snapshot: available, cursor_sequence={}, resting_orders={}</li>",
                snapshot.cursor_sequence,
                snapshot.orders.len()
            ),
            Err(error) => format!(
                "<li>Snapshot: error ({})</li>",
                escape_html(&error.to_string())
            ),
        },
        Some(path) => format!(
            "<li>Snapshot: configured but missing ({})</li>",
            escape_html(&path.display().to_string())
        ),
        None => "<li>Snapshot: not configured</li>".to_owned(),
    };

    let uptime_ms = state
        .started_at
        .elapsed()
        .as_millis()
        .min(u128::from(u64::MAX));
    let journal = escape_html(&state.journal_path.display().to_string());
    let index = escape_html(&state.index_path.display().to_string());

    let mut html = String::from(concat!(
        "<!doctype html>",
        "<html lang=\"en\">",
        "<head>",
        "<meta charset=\"utf-8\">",
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">",
        "<meta http-equiv=\"refresh\" content=\"5\">",
        "<title>exchange-lab</title>",
        "<style>",
        ":root{color-scheme:light;font-family:Consolas,'Lucida Console',monospace;}",
        "body{margin:0;background:#f3efe5;color:#1d1d1d;}",
        ".wrap{max-width:960px;margin:0 auto;padding:32px 20px 48px;}",
        ".hero{background:linear-gradient(135deg,#f8d9a0,#d9f0e7);border:2px solid #1d1d1d;padding:24px;box-shadow:8px 8px 0 #1d1d1d;}",
        "h1{margin:0 0 10px;font-size:2.2rem;}",
        "p{line-height:1.5;}",
        ".grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:16px;margin-top:20px;}",
        ".card{background:#fff;border:2px solid #1d1d1d;padding:18px;}",
        "ul{margin:10px 0 0;padding-left:18px;}",
        "a.button{display:inline-block;margin:8px 10px 0 0;padding:10px 14px;border:2px solid #1d1d1d;text-decoration:none;color:#1d1d1d;background:#f8d9a0;}",
        "code{background:#efe7d8;padding:2px 4px;}",
        "</style>",
        "</head>",
        "<body><div class=\"wrap\">",
        "<section class=\"hero\">",
        "<h1>exchange-lab</h1>",
        "<p>Deterministic event-sourced matching engine + replay + fault-injection lab.</p>",
        "<p>Local live dashboard for the currently configured journal.</p>",
        "<a class=\"button\" href=\"/metrics\">Metrics</a>",
        "<a class=\"button\" href=\"/healthz\">Health</a>",
        "<a class=\"button\" href=\"/readyz\">Ready</a>",
        "</section>",
        "<section class=\"grid\">",
        "<div class=\"card\">",
        "<strong>Server</strong>",
        "<ul>"
    ));
    write!(html, "<li>Ready: {}</li>", state.is_ready()).expect("write ready");
    write!(html, "<li>Uptime: {} ms</li>", uptime_ms).expect("write uptime");
    write!(html, "<li>Journal: <code>{}</code></li>", journal).expect("write journal");
    write!(html, "<li>Index: <code>{}</code></li>", index).expect("write index");
    html.push_str(&snapshot);
    html.push_str(concat!(
        "</ul>",
        "</div>",
        "<div class=\"card\">",
        "<strong>How to use it</strong>",
        "<ul>",
        "<li>Keep this window open while the local API is running.</li>",
        "<li>Prometheus metrics are exposed at <code>/metrics</code>.</li>",
        "<li>gRPC replay stream listens on the configured gRPC port.</li>",
        "<li>The page refreshes every 5 seconds.</li>",
        "</ul>",
        "</div>",
        "</section>",
        "</div></body></html>"
    ));
    html
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn proto_event(event: &CanonicalEvent) -> EventMessage {
    let mut message = EventMessage {
        timestamp_ns: event.timestamp_ns,
        sequence: event.sequence,
        symbol: event.symbol.clone(),
        venue: event.venue.clone(),
        kind: 0,
        order_id: 0,
        side: 0,
        price_ticks: 0,
        qty: 0,
        alt_u64_1: 0,
        alt_u64_2: 0,
    };

    match &event.event {
        Event::AddOrder {
            order_id,
            side,
            price_ticks,
            qty,
        } => {
            message.kind = EventKind::AddOrder as i32;
            message.order_id = order_id.0;
            message.side = u32::from(side.as_u8());
            message.price_ticks = *price_ticks;
            message.qty = *qty;
        }
        Event::ModifyOrder { order_id, new_qty } => {
            message.kind = EventKind::ModifyOrder as i32;
            message.order_id = order_id.0;
            message.qty = *new_qty;
        }
        Event::CancelOrder {
            order_id,
            cancel_qty,
        } => {
            message.kind = EventKind::CancelOrder as i32;
            message.order_id = order_id.0;
            message.qty = *cancel_qty;
        }
        Event::Trade {
            maker_order_id,
            taker_side,
            price_ticks,
            qty,
        } => {
            message.kind = EventKind::Trade as i32;
            message.order_id = maker_order_id.0;
            message.side = u32::from(taker_side.as_u8());
            message.price_ticks = *price_ticks;
            message.qty = *qty;
        }
        Event::BookSnapshotTop {
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
        } => {
            message.kind = EventKind::BookSnapshotTop as i32;
            message.price_ticks = *bid_px;
            message.qty = *bid_sz;
            message.alt_u64_1 = *ask_px;
            message.alt_u64_2 = *ask_sz;
        }
    }

    message
}

pub fn snapshot_path(path: impl AsRef<Path>) -> PathBuf {
    path.as_ref().to_path_buf()
}
