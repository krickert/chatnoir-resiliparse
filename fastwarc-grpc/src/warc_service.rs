// Copyright 2026 Kristian Rickert
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The `fastwarc.v1.WarcService` implementation.
//!
//! Both RPCs run the same blocking parse pipeline against an emit callback.
//! For `ParseWarc`, request chunks feed a private `ChannelReader` via `mpsc`
//! and emitted messages return on a second bounded channel
//! (`ReceiverStream`). For the unary `ParseArchive`, the request bytes are
//! parsed from an in-memory cursor and the emitted messages are folded into
//! a single response.

use std::io::{self, BufRead, Read, Seek, SeekFrom};

use fastwarc::stream_io::bufread::RawReaderAdapter;
use fastwarc::stream_io::traits::IntoWarcReader;
use fastwarc::warc::iter::{ArchiveIterator, ArchiveIteratorOptions};
use fastwarc::warc::record::WarcRecord;
use prost::bytes::{Buf, Bytes, BytesMut};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::convert;
use crate::proto::fastwarc::v1 as pb;

/// Default maximum WARC/HTTP header block length (matches the crate default).
const DEFAULT_MAX_HEADER_LEN: usize = 32 << 10;
/// Default payload chunk size for `payload_chunk` messages.
const DEFAULT_PAYLOAD_CHUNK_SIZE: usize = 64 << 10;
/// Default buffer capacity of the byte stream fed into the parser.
const DEFAULT_INPUT_BUFFER_SIZE: usize = 64 << 10;
/// Cap unread archive bytes queued into the parser thread. Slot count
/// follows `input_buffer_size`.
const CHUNK_CHANNEL_BYTES: usize = 32 * 1024 * 1024;
const CHUNK_CHANNEL_BOUND_MAX: usize = 256;

pub(crate) fn chunk_channel_bound(input_buffer_size: u32) -> usize {
    let hint = if input_buffer_size == 0 {
        DEFAULT_INPUT_BUFFER_SIZE
    } else {
        input_buffer_size as usize
    };
    (CHUNK_CHANNEL_BYTES / hint.max(1)).clamp(2, CHUNK_CHANNEL_BOUND_MAX)
}

/// Bound of the response channel back to the client.
pub(crate) const RESPONSE_CHANNEL_BOUND: usize = 1024;
/// Flush a batch before it approaches the gRPC message-size cap.
const MAX_BATCH_BYTES: usize = 2 << 20;

pub(crate) type ResponseSender = mpsc::Sender<Result<pb::ParseWarcResponse, Status>>;

/// Consumer of parse protocol messages; returns `false` when the consumer is
/// gone and the parse should stop.
type EmitFn<'a> = &'a mut dyn FnMut(pb::ParseWarcResponse) -> bool;

/// The `fastwarc.v1.WarcService` gRPC service (stateless).
///
/// By default `ParseWarcConfig.archive_path` is rejected with
/// `PermissionDenied`: letting remote clients name server-side files is a
/// separate security domain from parsing bytes the client supplied, so it
/// must be an explicit operator decision (see [`WarcParser::with_local_files`]).
#[derive(Default, Clone, Copy)]
pub struct WarcParser {
    pub(crate) allow_local_files: bool,
}

impl WarcParser {
    /// A parser that only parses client-supplied bytes; `archive_path`
    /// requests are rejected with `PermissionDenied`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A parser that additionally allows `ParseWarcConfig.archive_path` to
    /// open files on the server's filesystem. Only enable this when every
    /// client is trusted with read access to the server's files.
    #[must_use]
    pub fn with_local_files() -> Self {
        Self {
            allow_local_files: true,
        }
    }
}

#[tonic::async_trait]
impl pb::warc_service_server::WarcService for WarcParser {
    type ParseWarcStream = ReceiverStream<Result<pb::ParseWarcResponse, Status>>;

    async fn parse_warc(
        &self,
        request: Request<Streaming<pb::ParseWarcRequest>>,
    ) -> Result<Response<Self::ParseWarcStream>, Status> {
        let mut stream = request.into_inner();

        // First message must carry config.
        let config = match stream.message().await {
            Ok(Some(msg)) => match msg.kind {
                Some(pb::parse_warc_request::Kind::Config(config)) => config,
                _ => {
                    return Err(Status::invalid_argument("first ParseWarc request message must set `config`"));
                }
            },
            Ok(None) => {
                return Err(Status::invalid_argument(
                    "empty ParseWarc request stream; first message must set `config`",
                ));
            }
            Err(e) => return Err(e),
        };
        if !config.archive_path.is_empty() && !self.allow_local_files {
            return Err(Status::permission_denied(
                "archive_path is disabled on this server; stream the archive as chunks instead",
            ));
        }

        let (chunk_tx, chunk_rx) = mpsc::channel::<Bytes>(chunk_channel_bound(config.input_buffer_size));
        let (resp_tx, resp_rx) = mpsc::channel(RESPONSE_CHANNEL_BOUND);
        spawn_request_forwarder(stream, chunk_tx, resp_tx.clone());
        start_pipeline(config, chunk_rx, resp_tx);
        Ok(Response::new(ReceiverStream::new(resp_rx)))
    }

    async fn parse_archive(
        &self,
        request: Request<pb::ParseArchiveRequest>,
    ) -> Result<Response<pb::ParseArchiveResponse>, Status> {
        let request = request.into_inner();
        let Some(config) = request.config else {
            return Err(Status::invalid_argument("ParseArchive request must set `config`"));
        };
        let archive = request.archive;

        let joined = tokio::task::spawn_blocking(move || {
            let mut records = Vec::new();
            let mut errors = Vec::new();
            let mut open: Option<pb::ParsedRecord> = None;
            let mut emit = |resp: pb::ParseWarcResponse| {
                fold_response(resp, &mut open, &mut records, &mut errors);
                true
            };
            let next_index = std::sync::atomic::AtomicU64::new(0);
            parse_into(io::Cursor::new(archive), &config, &mut emit, &next_index);
            pb::ParseArchiveResponse { records, errors }
        })
        .await;
        match joined {
            Ok(response) => Ok(Response::new(response)),
            Err(e) if e.is_panic() => Err(Status::internal("WARC parser task panicked")),
            Err(_) => Err(Status::cancelled("WARC parser task cancelled")),
        }
    }
}

/// Launch the parse pipeline for one configured stream: archive bytes come
/// in on `chunk_rx`, protocol messages go out on `resp_tx`. Shared by the
/// tonic-codec route and the raw-frame route.
pub(crate) fn start_pipeline(config: pb::ParseWarcConfig, chunk_rx: mpsc::Receiver<Bytes>, resp_tx: ResponseSender) {
    let parallelism = config.parallelism.min(MAX_PARALLELISM) as usize;
    if parallelism >= 2 && config.archive_path.is_empty() {
        spawn_parallel_pipeline(chunk_rx, resp_tx, config, parallelism);
        return;
    }

    let panic_tx = resp_tx.clone();
    // Map JoinError (panic/cancel) to a gRPC status; bare spawn_blocking
    // would drop the sender and look like a clean EOF.
    tokio::spawn(async move {
        match tokio::task::spawn_blocking(move || {
            run_parser(chunk_rx, &resp_tx, &config);
        })
        .await
        {
            Ok(()) => {}
            Err(e) if e.is_panic() => {
                let _ = panic_tx.send(Err(Status::internal("WARC parser task panicked"))).await;
            }
            Err(_) => {
                let _ = panic_tx
                    .send(Err(Status::cancelled("WARC parser task cancelled")))
                    .await;
            }
        }
    });
}

/// Forward request chunks into the parse pipeline; protocol violations and
/// transport errors go to `err_tx`. Dropping `chunk_tx` signals EOF.
fn spawn_request_forwarder(
    mut stream: Streaming<pb::ParseWarcRequest>,
    chunk_tx: mpsc::Sender<Bytes>,
    err_tx: ResponseSender,
) {
    tokio::spawn(async move {
        loop {
            match stream.message().await {
                Ok(Some(msg)) => match msg.kind {
                    Some(pb::parse_warc_request::Kind::Chunk(chunk)) => {
                        if chunk_tx.send(chunk).await.is_err() {
                            break;
                        }
                    }
                    Some(pb::parse_warc_request::Kind::Config(_)) => {
                        let _ = err_tx
                            .send(Err(Status::invalid_argument(
                                "`config` may only be set on the first request message",
                            )))
                            .await;
                        break;
                    }
                    None => {
                        let _ = err_tx
                            .send(Err(Status::invalid_argument(
                                "ParseWarc request message must set `config` or `chunk`",
                            )))
                            .await;
                        break;
                    }
                },
                Ok(None) => break,
                Err(e) => {
                    let _ = err_tx.send(Err(e)).await;
                    break;
                }
            }
        }
    });
}

// ===========================================================
// Parallel parse (config.parallelism >= 2)
// ===========================================================

/// Upper bound on `config.parallelism`.
const MAX_PARALLELISM: u32 = 64;
/// Compressed bytes per parallel segment before the scanner starts hunting
/// for the next gzip member boundary.
const SEGMENT_TARGET_BYTES: usize = 256 * 1024;
/// Slots in each segment's chunk channel; bounds in-flight memory.
const SEGMENT_CHUNK_BOUND: usize = 32;
/// Minimum lookahead behind a boundary candidate before validating it,
/// unless the stream has ended.
const VALIDATE_MIN_BYTES: usize = 512;
/// Compressed prefix handed to the boundary validator.
const VALIDATE_WINDOW_BYTES: usize = 4096;
/// Cap on the scanner's hunt buffer: a stream with no valid boundaries
/// (single-member gzip) flushes to the current segment instead of growing.
const HUNT_WINDOW_MAX_BYTES: usize = 4 * SEGMENT_TARGET_BYTES;

const GZIP_MAGIC: [u8; 3] = [0x1f, 0x8b, 0x08];

/// One parallel work unit: a chunk stream plus its absolute offset in the
/// uploaded archive. Each segment starts at a gzip member boundary.
struct Segment {
    rx: mpsc::Receiver<Bytes>,
    base: u64,
}

/// True when `buf` starts a gzip member whose decompressed bytes begin with
/// `WARC/`. Rejects magic-byte false positives inside compressed data.
fn is_warc_gzip_member(buf: &[u8]) -> bool {
    let head = &buf[..buf.len().min(VALIDATE_WINDOW_BYTES)];
    let mut reader = fastwarc::stream_io::gzip::GzipReader::new(io::Cursor::new(head.to_vec()));
    let mut magic = [0u8; 5];
    reader.read_exact(&mut magic).is_ok() && &magic == b"WARC/"
}

/// Shift record positions by the segment's base so `stream_pos` stays the
/// absolute offset in the uploaded stream.
fn offset_stream_pos(resp: &mut pb::ParseWarcResponse, base: u64) {
    match &mut resp.kind {
        Some(pb::parse_warc_response::Kind::RecordStart(start)) => {
            if let Some(metadata) = &mut start.metadata {
                metadata.stream_pos += base;
            }
        }
        Some(pb::parse_warc_response::Kind::RecordError(error)) => error.stream_pos += base,
        _ => {}
    }
}

/// Launch the scanner and `workers` parser threads for one parallel stream.
fn spawn_parallel_pipeline(
    chunk_rx: mpsc::Receiver<Bytes>,
    resp_tx: ResponseSender,
    config: pb::ParseWarcConfig,
    workers: usize,
) {
    let panic_tx = resp_tx.clone();
    tokio::spawn(async move {
        let (seg_tx, seg_rx) = mpsc::channel::<Segment>(workers * 2);
        let seg_rx = std::sync::Arc::new(std::sync::Mutex::new(seg_rx));
        let config = std::sync::Arc::new(config);
        let next_index = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

        let mut joins = vec![tokio::task::spawn_blocking(move || run_scanner(chunk_rx, &seg_tx))];
        for _ in 0..workers {
            let seg_rx = seg_rx.clone();
            let resp_tx = resp_tx.clone();
            let config = config.clone();
            let next_index = next_index.clone();
            joins.push(tokio::task::spawn_blocking(move || {
                run_parallel_worker(&seg_rx, &resp_tx, &config, &next_index);
            }));
        }
        drop(resp_tx);
        for join in joins {
            match join.await {
                Ok(()) => {}
                Err(e) if e.is_panic() => {
                    let _ = panic_tx.send(Err(Status::internal("WARC parser task panicked"))).await;
                }
                Err(_) => {
                    let _ = panic_tx
                        .send(Err(Status::cancelled("WARC parser task cancelled")))
                        .await;
                }
            }
        }
    });
}

/// Cut the incoming chunk stream into segments at validated gzip member
/// boundaries. Non-gzip input (and gzip without further member boundaries)
/// flows through as one segment, which parses exactly like the sequential
/// path.
fn run_scanner(mut chunk_rx: mpsc::Receiver<Bytes>, seg_tx: &mpsc::Sender<Segment>) {
    // Sniff the first three bytes to pick the mode.
    let mut sniff = BytesMut::new();
    let mut pending: std::collections::VecDeque<Bytes> = std::collections::VecDeque::new();
    let mut ended = false;
    while sniff.len() < GZIP_MAGIC.len() {
        match chunk_rx.blocking_recv() {
            Some(chunk) if chunk.is_empty() => {}
            Some(chunk) => {
                sniff.extend_from_slice(&chunk);
                pending.push_back(chunk);
            }
            None => {
                ended = true;
                break;
            }
        }
    }
    let gzip = sniff.len() >= GZIP_MAGIC.len() && sniff[..GZIP_MAGIC.len()] == GZIP_MAGIC;

    if !gzip {
        // Passthrough: one segment, no buffering beyond the channel bounds.
        let Some(segment) = open_segment(seg_tx, 0) else { return };
        loop {
            let Some(chunk) = pending.pop_front() else {
                if ended {
                    return;
                }
                match chunk_rx.blocking_recv() {
                    Some(chunk) => pending.push_back(chunk),
                    None => ended = true,
                }
                continue;
            };
            if segment.blocking_send(chunk).is_err() {
                return;
            }
        }
    }
    run_gzip_scanner(chunk_rx, seg_tx, pending, ended);
}

/// Open the next segment and hand its receiving half to the worker queue.
fn open_segment(seg_tx: &mpsc::Sender<Segment>, base: u64) -> Option<mpsc::Sender<Bytes>> {
    let (tx, rx) = mpsc::channel(SEGMENT_CHUNK_BOUND);
    seg_tx.blocking_send(Segment { rx, base }).ok().map(|()| tx)
}

/// Gzip mode of [`run_scanner`]: forward zero-copy up to the segment
/// target, then buffer into a contiguous window and cut at the next
/// validated member boundary.
fn run_gzip_scanner(
    mut chunk_rx: mpsc::Receiver<Bytes>,
    seg_tx: &mpsc::Sender<Segment>,
    mut pending: std::collections::VecDeque<Bytes>,
    mut ended: bool,
) {
    let Some(mut segment) = open_segment(seg_tx, 0) else {
        return;
    };
    let mut abs: u64 = 0;
    let mut sent_in_segment: usize = 0;
    // Contiguous hunt buffer, used only while looking for a boundary.
    let mut window = BytesMut::new();
    let mut window_base: u64 = 0;
    let mut scan_pos: usize = 0;

    loop {
        if pending.is_empty() && !ended {
            match chunk_rx.blocking_recv() {
                Some(chunk) if chunk.is_empty() => continue,
                Some(chunk) => pending.push_back(chunk),
                None => ended = true,
            }
        }

        if window.is_empty() && sent_in_segment < SEGMENT_TARGET_BYTES {
            // Below target: forward chunks zero-copy.
            let Some(chunk) = pending.pop_front() else {
                if ended {
                    return;
                }
                continue;
            };
            sent_in_segment += chunk.len();
            abs += chunk.len() as u64;
            if segment.blocking_send(chunk).is_err() {
                return;
            }
            continue;
        }

        // Hunting: accumulate into the contiguous window.
        if let Some(chunk) = pending.pop_front() {
            if window.is_empty() {
                window_base = abs;
            }
            abs += chunk.len() as u64;
            window.extend_from_slice(&chunk);
        } else if !ended {
            continue;
        }

        // Scan the window for a validated member boundary.
        let mut cut_at: Option<usize> = None;
        while scan_pos + GZIP_MAGIC.len() <= window.len() {
            let Some(rel) = window[scan_pos..]
                .windows(GZIP_MAGIC.len())
                .position(|w| w == GZIP_MAGIC)
            else {
                scan_pos = window.len() - (GZIP_MAGIC.len() - 1);
                break;
            };
            let candidate = scan_pos + rel;
            if window.len() - candidate < VALIDATE_MIN_BYTES && !ended {
                scan_pos = candidate;
                break; // Wait for more lookahead before validating.
            }
            if is_warc_gzip_member(&window[candidate..]) {
                cut_at = Some(candidate);
                break;
            }
            scan_pos = candidate + 1;
        }

        if let Some(cut) = cut_at {
            let before = window.split_to(cut).freeze();
            if !before.is_empty() && segment.blocking_send(before).is_err() {
                return;
            }
            let base = window_base + cut as u64;
            let Some(next) = open_segment(seg_tx, base) else { return };
            segment = next;
            let rest = std::mem::take(&mut window).freeze();
            window_base = base;
            sent_in_segment = rest.len();
            scan_pos = 0;
            if !rest.is_empty() && segment.blocking_send(rest).is_err() {
                return;
            }
            continue;
        }

        // No boundary yet: bound the hunt buffer (single-member gzip never
        // yields one), keeping a small tail so a magic split across the
        // flush point is still found.
        if window.len() > HUNT_WINDOW_MAX_BYTES {
            let keep = VALIDATE_MIN_BYTES.min(window.len());
            let flush = window.split_to(window.len() - keep).freeze();
            window_base += flush.len() as u64;
            scan_pos = 0;
            if segment.blocking_send(flush).is_err() {
                return;
            }
        }

        if ended && pending.is_empty() {
            if !window.is_empty() {
                let rest = window.freeze();
                let _ = segment.blocking_send(rest);
            }
            return;
        }
    }
}

/// Pull segments off the shared queue and parse each with the standard
/// pipeline. `record_index` values come from the shared allocator and
/// `stream_pos` is shifted to the segment's absolute base.
fn run_parallel_worker(
    seg_rx: &std::sync::Mutex<mpsc::Receiver<Segment>>,
    resp_tx: &ResponseSender,
    config: &pb::ParseWarcConfig,
    next_index: &std::sync::atomic::AtomicU64,
) {
    let mut emitter = BatchEmitter::new(resp_tx, convert::response_batch_size(config));
    loop {
        let segment = match seg_rx.lock() {
            Ok(mut guard) => guard.blocking_recv(),
            Err(_) => None,
        };
        let Some(segment) = segment else { break };
        let base = segment.base;
        let mut emit = |mut resp: pb::ParseWarcResponse| {
            offset_stream_pos(&mut resp, base);
            emitter.emit(resp)
        };
        parse_into(RawReaderAdapter::new(ChannelReader::new(segment.rx)), config, &mut emit, next_index);
    }
    emitter.flush();
}

/// Fold one parse protocol message into the unary response accumulators.
///
/// The pipeline guarantees well-formed sequences (`record_start` before
/// chunks and `record_end`, errors outside record sequences), so the fold
/// does not re-validate them.
fn fold_response(
    resp: pb::ParseWarcResponse,
    open: &mut Option<pb::ParsedRecord>,
    records: &mut Vec<pb::ParsedRecord>,
    errors: &mut Vec<pb::RecordError>,
) {
    match resp.kind {
        Some(pb::parse_warc_response::Kind::RecordStart(start)) => {
            *open = Some(pb::ParsedRecord {
                metadata: start.metadata,
                ..Default::default()
            });
        }
        Some(pb::parse_warc_response::Kind::PayloadChunk(chunk)) => {
            if let Some(record) = open.as_mut() {
                record.payload.extend_from_slice(&chunk.data);
            }
        }
        Some(pb::parse_warc_response::Kind::RecordEnd(end)) => {
            if let Some(mut record) = open.take() {
                record.block_digest_status = end.block_digest_status;
                record.payload_digest_status = end.payload_digest_status;
                record.digest_detail = end.digest_detail;
                records.push(record);
            }
        }
        Some(pb::parse_warc_response::Kind::RecordError(error)) => errors.push(error),
        Some(pb::parse_warc_response::Kind::Batch(batch)) => {
            for item in batch.items {
                fold_response(item, open, records, errors);
            }
        }
        None => {}
    }
}

fn response(kind: pb::parse_warc_response::Kind) -> pb::ParseWarcResponse {
    pb::ParseWarcResponse { kind: Some(kind) }
}

fn record_error(stream_pos: u64, recoverable: bool, message: String) -> pb::ParseWarcResponse {
    response(pb::parse_warc_response::Kind::RecordError(pb::RecordError {
        stream_pos,
        recoverable,
        message,
    }))
}

/// Blocking parse entry point for the streaming RPC: reads archive bytes
/// from `chunk_rx` and emits protocol messages on `resp_tx`.
fn run_parser(chunk_rx: mpsc::Receiver<Bytes>, resp_tx: &ResponseSender, config: &pb::ParseWarcConfig) {
    let input_buffer_size = if config.input_buffer_size == 0 {
        DEFAULT_INPUT_BUFFER_SIZE
    } else {
        config.input_buffer_size as usize
    };
    let mut emitter = BatchEmitter::new(resp_tx, convert::response_batch_size(config));
    let mut emit = |resp: pb::ParseWarcResponse| emitter.emit(resp);
    if config.archive_path.is_empty() {
        // ChannelReader is BufRead over the received chunks themselves, so
        // the parser scans and skips archive bytes in place; wrapping it in
        // a BufReader would memcpy the whole stream a second time.
        let next_index = std::sync::atomic::AtomicU64::new(0);
        parse_into(RawReaderAdapter::new(ChannelReader::new(chunk_rx)), config, &mut emit, &next_index);
    } else {
        match std::fs::File::open(&config.archive_path) {
            Ok(file) => {
                let reader = io::BufReader::with_capacity(input_buffer_size, file);
                let next_index = std::sync::atomic::AtomicU64::new(0);
                parse_into(reader, config, &mut emit, &next_index);
            }
            Err(e) => {
                emit(record_error(0, false, format!("failed to open archive_path {}: {e}", config.archive_path)));
            }
        }
    }
    emitter.flush();
}

/// Packs protocol events into gRPC messages and `try_send`s them so the
/// parser thread is not parked on HTTP/2 drain. Falls back to
/// `blocking_send` only when the response channel is actually full.
struct BatchEmitter<'a> {
    tx: &'a ResponseSender,
    batch: Vec<pb::ParseWarcResponse>,
    batch_size: usize,
    batch_bytes: usize,
}

impl BatchEmitter<'_> {
    fn new(tx: &ResponseSender, batch_size: usize) -> BatchEmitter<'_> {
        BatchEmitter {
            tx,
            batch: Vec::with_capacity(batch_size),
            batch_size,
            batch_bytes: 0,
        }
    }

    fn emit(&mut self, resp: pb::ParseWarcResponse) -> bool {
        if self.batch_size <= 1 {
            return send_response(self.tx, resp);
        }
        let add = event_wire_bytes(&resp);
        if !self.batch.is_empty() && self.batch_bytes + add > MAX_BATCH_BYTES && !self.flush() {
            return false;
        }
        self.batch_bytes += add;
        self.batch.push(resp);
        if self.batch.len() >= self.batch_size || self.batch_bytes >= MAX_BATCH_BYTES {
            self.flush()
        } else {
            true
        }
    }

    fn flush(&mut self) -> bool {
        if self.batch.is_empty() {
            return true;
        }
        self.batch_bytes = 0;
        let items = std::mem::take(&mut self.batch);
        let msg = if items.len() == 1 {
            items.into_iter().next().expect("checked non-empty")
        } else {
            response(pb::parse_warc_response::Kind::Batch(pb::RecordBatch { items }))
        };
        send_response(self.tx, msg)
    }
}

/// Cheap size estimate for batch flushing; protobuf tags add a little more.
fn event_wire_bytes(resp: &pb::ParseWarcResponse) -> usize {
    match resp.kind.as_ref() {
        Some(pb::parse_warc_response::Kind::PayloadChunk(chunk)) => chunk.data.len().saturating_add(64),
        Some(pb::parse_warc_response::Kind::RecordStart(start)) => start
            .metadata
            .as_ref()
            .and_then(|m| m.warc_headers.as_ref())
            .map_or(128, |h| h.raw_block.len().saturating_add(256)),
        Some(pb::parse_warc_response::Kind::Batch(batch)) => batch.items.iter().map(event_wire_bytes).sum(),
        _ => 64,
    }
}

fn send_response(tx: &ResponseSender, msg: pb::ParseWarcResponse) -> bool {
    match tx.try_send(Ok(msg)) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(m)) => tx.blocking_send(m).is_ok(),
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

/// Core parse loop shared by both RPCs: `record_start` / `payload_chunk`* /
/// `record_end` per kept record, or `record_error` on failure, delivered to
/// `emit` in stream order.
///
/// Iterator-level errors (invalid WARC framing) end the parse: the crate
/// detaches the reader on those failures and subsequent `next()` calls loop
/// on `"No reader set"`. HTTP-header failures inside an already-framed record
/// are recoverable; the iterator consumes the remainder on the next step.
fn parse_into(
    reader: impl IntoWarcReader,
    config: &pb::ParseWarcConfig,
    emit: EmitFn<'_>,
    next_index: &std::sync::atomic::AtomicU64,
) {
    let chunk_size = if config.payload_chunk_size == 0 {
        DEFAULT_PAYLOAD_CHUNK_SIZE
    } else {
        config.payload_chunk_size as usize
    };
    let max_header_len = if config.max_header_len == 0 {
        DEFAULT_MAX_HEADER_LEN
    } else {
        config.max_header_len as usize
    };
    // Unset optional defaults to true (Python/Rust ArchiveIterator default).
    let stream_detect = config.stream_detect.unwrap_or(true);
    let options = ArchiveIteratorOptions {
        stream_detect,
        // HTTP parse is manual after block-digest verify: WARC-Block-Digest
        // covers the raw block and must run before HTTP advances the stream.
        parse_http: false,
        decode_http_payload: convert::auto_decode(config.decode_http_payload),
        // Iterator-level verify skips mismatches; we verify per record so
        // results can be reported on the wire.
        verify_digests: false,
        quirks_mode: config.quirks_mode,
        max_header_len,
        // Reuse the iterator buffer when payload is not copied and the record
        // does not need to be frozen for digest verification.
        inplace: !convert::include_payload(config) && !config.verify_digests,
    };
    let iterator = ArchiveIterator::with_options(reader, options);

    for item in iterator {
        let record = match item {
            Ok(record) => record,
            Err(e) => {
                // Framing failure: the crate has lost the reader; do not continue.
                let _ = emit(record_error(0, false, e.to_string()));
                return;
            }
        };

        {
            let mut borrowed = record.borrow_mut();
            if !convert::record_passes_filters(&mut borrowed, config) {
                // Skip without emitting; next() consumes any unread payload.
                continue;
            }
        }

        // Allocate an index for every framed, non-filtered record (skips do
        // not consume one). Sequential parses see 0, 1, 2, …; parallel
        // parses share the allocator across workers, so indexes stay
        // globally unique but follow completion order, not file order.
        let record_index = next_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        match process_record(&record, record_index, config, max_header_len, chunk_size, emit) {
            ProcessOutcome::Stop => return,
            ProcessOutcome::Emitted | ProcessOutcome::Continue => {}
        }
    }
}

/// Result of attempting to emit one framed record.
enum ProcessOutcome {
    /// `record_start` / chunks / `record_end` were sent; advance `record_index`.
    Emitted,
    /// Recoverable per-record failure (`record_error`); parse continues, index unchanged.
    Continue,
    /// Fatal: consumer gone or non-recoverable payload failure.
    Stop,
}

/// One record: block digest, optional HTTP parse, payload digest, then
/// `record_start` / `payload_chunk`* / `record_end`.
///
/// Digests run before payload streaming because verification rewinds a frozen
/// record, not a live stream.
fn process_record(
    shared: &std::rc::Rc<std::cell::RefCell<WarcRecord>>,
    record_index: u64,
    config: &pb::ParseWarcConfig,
    max_header_len: usize,
    chunk_size: usize,
    emit: EmitFn<'_>,
) -> ProcessOutcome {
    let mut record = shared.borrow_mut();

    let (block_digest_status, block_detail) = if config.verify_digests {
        convert::digest_status(record.verify_block_digest(false))
    } else {
        (pb::DigestStatus::Unspecified, None)
    };

    if config.parse_http
        && let Err(e) = record.parse_http_with_opts(
            convert::auto_decode(config.decode_http_payload),
            max_header_len,
            config.quirks_mode,
        )
    {
        // Already-framed record: the iterator will consume the remainder on
        // the next step, so this is recoverable.
        let stream_pos = record.stream_pos();
        return if emit(record_error(stream_pos, true, format!("failed to parse HTTP headers: {e}"))) {
            ProcessOutcome::Continue
        } else {
            ProcessOutcome::Stop
        };
    }

    let (payload_digest_status, payload_detail) = if config.verify_digests {
        convert::digest_status(record.verify_payload_digest(false))
    } else {
        (pb::DigestStatus::Unspecified, None)
    };

    let metadata = convert::record_metadata(&record, record_index, convert::include_headers(config));
    if !emit(response(pb::parse_warc_response::Kind::RecordStart(pb::RecordStart {
        metadata: Some(metadata),
    }))) {
        return ProcessOutcome::Stop;
    }

    let payload_length = if convert::include_payload(config) {
        match stream_payload(&mut record, record_index, chunk_size, emit) {
            Ok(len) => len,
            Err(e) => {
                let stream_pos = record.stream_pos();
                let _ = emit(record_error(stream_pos, false, format!("failed to read record payload: {e}")));
                return ProcessOutcome::Stop;
            }
        }
    } else {
        record.content_length()
    };

    let details: Vec<String> = [block_detail, payload_detail].into_iter().flatten().collect();

    if emit(response(pb::parse_warc_response::Kind::RecordEnd(pb::RecordEnd {
        record_index,
        payload_length,
        block_digest_status: block_digest_status.into(),
        payload_digest_status: payload_digest_status.into(),
        digest_detail: if details.is_empty() {
            None
        } else {
            Some(details.join("; "))
        },
    }))) {
        ProcessOutcome::Emitted
    } else {
        ProcessOutcome::Stop
    }
}

/// Emit remaining payload as `payload_chunk` messages; return total bytes.
///
/// Chunks are copied straight out of the record reader's `fill_buf` window
/// into exact-size `Bytes` (single copy, no scratch buffer), so a chunk may
/// be shorter than `chunk_size` when it ends at an input buffer boundary.
fn stream_payload(record: &mut WarcRecord, record_index: u64, chunk_size: usize, emit: EmitFn<'_>) -> io::Result<u64> {
    let Some(reader) = record.reader_mut() else {
        return Ok(0);
    };
    let mut offset = 0u64;
    loop {
        let window = reader.fill_buf()?;
        if window.is_empty() {
            return Ok(offset);
        }
        let n = window.len().min(chunk_size);
        let data = Bytes::copy_from_slice(&window[..n]);
        reader.consume(n);
        if !emit(response(pb::parse_warc_response::Kind::PayloadChunk(pb::PayloadChunk {
            record_index,
            offset,
            data,
        }))) {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "consumer gone"));
        }
        offset += n as u64;
    }
}

/// `BufRead` + `Seek` over streamed request chunks.
///
/// `fill_buf` hands the parser a window into the received `Bytes` chunk
/// itself, so header scans and payload skips run in place with no
/// intermediate copy. Blocks until the next chunk or EOF. Only
/// current-position seeks succeed; the parser only queries position during
/// linear reads. Digest verification freezes the record into an in-memory
/// `Cursor` before seeking, so identity seeks on this adapter are
/// sufficient for the linear parse path.
struct ChannelReader {
    rx: mpsc::Receiver<Bytes>,
    current: Bytes,
    pos: u64,
}

/// Compression autodetection reads the first four bytes from one `fill_buf`
/// window; coalesce the stream head until it can satisfy that.
const MAGIC_LEN: usize = 4;

impl ChannelReader {
    fn new(rx: mpsc::Receiver<Bytes>) -> Self {
        Self {
            rx,
            current: Bytes::new(),
            pos: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let src = self.fill_buf()?;
        let n = src.len().min(buf.len());
        buf[..n].copy_from_slice(&src[..n]);
        self.consume(n);
        Ok(n)
    }
}

impl BufRead for ChannelReader {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        while self.current.is_empty() {
            match self.rx.blocking_recv() {
                Some(chunk) if chunk.is_empty() => {}
                Some(chunk) => self.current = chunk,
                None => return Ok(&[]),
            }
        }
        if self.pos == 0 && self.current.len() < MAGIC_LEN {
            let mut head = BytesMut::from(&self.current[..]);
            while head.len() < MAGIC_LEN {
                match self.rx.blocking_recv() {
                    Some(chunk) => head.extend_from_slice(&chunk),
                    None => break,
                }
            }
            self.current = head.freeze();
        }
        Ok(&self.current)
    }

    fn consume(&mut self, amt: usize) {
        let n = amt.min(self.current.len());
        self.current.advance(n);
        self.pos += n as u64;
    }
}

impl Seek for ChannelReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match pos {
            SeekFrom::Current(0) => Ok(self.pos),
            SeekFrom::Start(p) if p == self.pos => Ok(self.pos),
            _ => Err(io::Error::new(io::ErrorKind::Unsupported, "streamed WARC input does not support repositioning")),
        }
    }
}
