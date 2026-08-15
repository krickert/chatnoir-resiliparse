// Layer cost ladder for WARC parsing behind gRPC.
//
// A number like "gRPC does 300 MiB/s where fastwarc does 5 GiB/s" compares
// the bottom of a stack against the top and says nothing about where the
// cost lives. This harness prices each layer separately: every row adds
// exactly one layer between the archive bytes and the parser, so the cost
// of a layer is the delta between adjacent rows, and each row reports both
// sides of its ledger (bytes in, wire traffic, records out, CPU spent).

use std::io::{BufRead, Read, Seek, SeekFrom, Write};
use std::time::Instant;

use fastwarc::stream_io::bufread::RawReaderAdapter;
use fastwarc::warc::iter::ArchiveIterator;
use fastwarc_grpc::proto::fastwarc::v1 as pb;
use fastwarc_grpc::proto::fastwarc::v1::warc_service_client::WarcServiceClient;
use fastwarc_grpc::proto::fastwarc::v1::warc_service_server::WarcServiceServer;
use fastwarc_grpc::warc_service::WarcParser;
use hyper_util::rt::TokioIo;
use prost::Message;
use prost::bytes::Bytes;
use tokio::net::UnixStream;
use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
use tonic::transport::{Channel, Uri};
use tower::service_fn;

const CHUNK: usize = 64 << 10;
const MIB: f64 = 1024.0 * 1024.0;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        println!(
            "Usage: {} WARCFILE (uncompressed; a WARCFILE.gz sibling adds the wire-compression section)",
            args[0]
        );
        return;
    }
    let path = args[1].clone();
    let file_bytes = std::fs::metadata(&path).expect("stat WARCFILE").len();
    let gz_path = format!("{path}.gz");
    let gz = std::fs::metadata(&gz_path).ok().map(|m| m.len());

    println!("WARC transport layer costs: {path} ({:.1} MiB on disk)", file_bytes as f64 / MIB);
    if file_bytes < (1 << 30) {
        println!("WARNING: file under 1 GiB; rows finish in milliseconds and adjacent");
        println!("deltas are mostly noise. Use a multi-GiB WARC for stable numbers.");
    }
    println!();
    println!("Each row adds ONE layer between the archive bytes and the fastwarc parser.");
    println!("Price a layer by comparing adjacent rows, not a row against the baseline.");
    println!("Ledger per row: bytes in, wire traffic both directions, records out, CPU.");
    println!("MiB/s is record payload per wall second, the shared benchmark metric.");
    println!();

    println!("== Ceilings (no parsing; what the machine itself allows) ==");
    let disk = ceiling_disk(&path);
    let sock = ceiling_socket(&path);
    println!();

    println!("== Layer ladder (uncompressed input, parse-only replies) ==");
    let l0 = l0_parser_disk(&path);
    let l1 = l1_grpc_disk(&path);
    let l2 = l2_socket_parser(&path);
    let l3 = l3_grpc_upload(&path, file_bytes, false);
    println!();

    let mut gz_rows = None;
    if let Some(gz_bytes) = gz {
        println!("== Wire compression (gzip input: parser does the same work, the socket");
        println!("   moves {:.1} MiB instead of {:.1} MiB) ==", gz_bytes as f64 / MIB, file_bytes as f64 / MIB);
        let g0 = l0_parser_disk(&gz_path);
        let g3 = l3_grpc_upload(&gz_path, gz_bytes, false);
        gz_rows = Some((g0, g3));
        println!();
    }

    println!("== Output ladder (what producing and shipping results costs; the input");
    println!("   is the file on disk in EVERY row, so output is the only variable) ==");
    let o0 = o0_parse_read(&path);
    let o1 = o1_serialize(&path);
    let o2 = o2_socket_raw(&path);
    let o3 = o3_socket_proto(&path);
    let o4 = o4_grpc_echo(&path);
    println!();

    println!("== End-to-end (upload AND payload echo: the wire carries the archive");
    println!("   both ways through the full stack) ==");
    let f3 = l3_grpc_upload(&path, file_bytes, true);
    println!();

    let jobs: usize = std::env::var("FASTWARC_COMPARE_JOBS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);
    if let Some(gz_bytes) = gz
        && jobs > 1
    {
        println!("== Concurrency (the service use case: {jobs} clients, each uploading the");
        println!("   gzip archive on its own stream; rates are the aggregate) ==");
        jobs_grpc_tonic(&gz_path, gz_bytes, jobs);
        println!();
    }

    println!("== The story in one table ==");
    println!("{:<44} {:>10} {:>8}", "layer", "MiB/s", "vs prev");
    ladder_row("ceiling: disk read (1 copy)", disk, None);
    ladder_row("ceiling: unix socket (2 kernel copies)", sock, None);
    ladder_row("L0 parser, file on disk", l0.rate(), None);
    ladder_row("L1 + gRPC service (file stays on disk)", l1.rate(), Some(l0.rate()));
    ladder_row("L2 + socket delivery (no protocol)", l2.rate(), Some(l1.rate()));
    ladder_row("L3 + gRPC upload (client streams the bytes)", l3.rate(), Some(l2.rate()));
    if let Some((g0, g3)) = gz_rows {
        ladder_row("gzip: L0 parser, file on disk", g0.rate(), None);
        ladder_row("gzip: L3 gRPC upload", g3.rate(), Some(g0.rate()));
    }
    ladder_row("O0 parse + read payloads (no output)", o0.rate(), None);
    ladder_row("O1 + protobuf serialization, discard", o1.rate(), Some(o0.rate()));
    ladder_row("O2 + raw bytes over unix socket", o2.rate(), Some(o0.rate()));
    ladder_row("O3 + protobuf over unix socket", o3.rate(), Some(o2.rate()));
    ladder_row("O4 + full gRPC stack (payload echo)", o4.rate(), Some(o3.rate()));
    ladder_row("end-to-end: gRPC upload + payload echo", f3.rate(), Some(l3.rate()));
    println!();
    println!("Reading the input ladder: L0->L1 is the gRPC session, L1->L2 the two");
    println!("kernel socket copies every socket consumer pays, L2->L3 the HTTP/2");
    println!("transport machinery plus the tonic codec's copies.");
    println!();
    println!("Reading the output ladder: O0->O1 prices protobuf serialization by");
    println!("itself, O0->O2 prices the output socket by itself, O2->O3 puts the");
    println!("serialized form on that socket, O3->O4 adds gRPC on top.");
    println!();
    println!("The socket and transport layers dominate and are not properties of");
    println!("this server; compressed wire input and concurrent streams are the two");
    println!("ways around them.");
}

// ===========================================================
// Ledger plumbing
// ===========================================================

/// One scenario's measurements.
struct Ledger {
    wall: f64,
    cpu: f64,
    payload: u64,
    records: usize,
    wire_in: Option<u64>,
    wire_out: Option<u64>,
}

impl Ledger {
    fn rate(&self) -> f64 {
        self.payload as f64 / MIB / self.wall
    }
}

fn cpu_seconds() -> f64 {
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    tv(usage.ru_utime) + tv(usage.ru_stime)
}

/// Run `body`, measuring wall and process CPU (client and in-process server
/// combined; the point is total cost, not who paid it).
fn measured(body: impl FnOnce() -> (u64, usize, Option<u64>, Option<u64>)) -> Ledger {
    let cpu0 = cpu_seconds();
    let start = Instant::now();
    let (payload, records, wire_in, wire_out) = body();
    Ledger {
        wall: start.elapsed().as_secs_f64(),
        cpu: cpu_seconds() - cpu0,
        payload,
        records,
        wire_in,
        wire_out,
    }
}

fn print_row(label: &str, why: &str, ledger: &Ledger) {
    println!("{label}");
    println!("    {why}");
    let wire = match (ledger.wire_in, ledger.wire_out) {
        (Some(i), Some(o)) => {
            format!(" | wire {:.1} MiB up, {:.1} MiB down", i as f64 / MIB, o as f64 / MIB)
        }
        (None, Some(o)) => format!(" | wire {:.1} MiB down", o as f64 / MIB),
        _ => String::new(),
    };
    println!(
        "    {} records, {:.1} MiB payload in {:.2}s = {:.0} MiB/s | cpu {:.2}s ({:.1} cores){wire}",
        ledger.records,
        ledger.payload as f64 / MIB,
        ledger.wall,
        ledger.rate(),
        ledger.cpu,
        ledger.cpu / ledger.wall.max(1e-9),
    );
}

fn ladder_row(label: &str, rate: f64, prev: Option<f64>) {
    match prev {
        Some(p) if p > 0.0 => println!("{label:<44} {rate:>10.0} {:>+7.0}%", (rate / p - 1.0) * 100.0),
        _ => println!("{label:<44} {rate:>10.0} {:>8}", "-"),
    }
}

// ===========================================================
// Ceilings
// ===========================================================

fn ceiling_disk(path: &str) -> f64 {
    let mut file = std::fs::File::open(path).unwrap();
    let mut buf = vec![0u8; CHUNK];
    let start = Instant::now();
    let mut total = 0u64;
    loop {
        match file.read(&mut buf).unwrap() {
            0 => break,
            n => total += n as u64,
        }
    }
    let rate = total as f64 / MIB / start.elapsed().as_secs_f64();
    println!("disk read, discard:        {rate:>7.0} MiB/s   (one copy: page cache -> buffer)");
    rate
}

fn ceiling_socket(path: &str) -> f64 {
    use std::os::unix::net::{UnixListener, UnixStream};
    let sock = std::env::temp_dir().join(format!("fastwarc-compare-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let listener = UnixListener::bind(&sock).unwrap();
    let path = path.to_owned();
    let sock2 = sock.clone();
    let sender = std::thread::spawn(move || {
        let mut file = std::fs::File::open(path).unwrap();
        let mut conn = UnixStream::connect(&sock2).unwrap();
        let mut buf = vec![0u8; CHUNK];
        loop {
            match file.read(&mut buf).unwrap() {
                0 => break,
                n => conn.write_all(&buf[..n]).unwrap(),
            }
        }
    });
    let (mut conn, _) = listener.accept().unwrap();
    let start = Instant::now();
    let mut buf = vec![0u8; CHUNK];
    let mut total = 0u64;
    loop {
        match conn.read(&mut buf).unwrap() {
            0 => break,
            n => total += n as u64,
        }
    }
    let rate = total as f64 / MIB / start.elapsed().as_secs_f64();
    sender.join().unwrap();
    let _ = std::fs::remove_file(&sock);
    println!("unix socket, discard:      {rate:>7.0} MiB/s   (three copies: disk read + kernel in + kernel out)");
    rate
}

// ===========================================================
// L0/L2: parser fed directly (no gRPC)
// ===========================================================

fn count_records(iterator: ArchiveIterator) -> (u64, usize) {
    let mut payload = 0u64;
    let mut records = 0usize;
    for record in iterator.with_parse_http(false).with_inplace(true) {
        let Ok(record) = record else { continue };
        payload += record.borrow().content_length();
        records += 1;
    }
    (payload, records)
}

fn l0_parser_disk(path: &str) -> Ledger {
    let file = std::fs::File::open(path).unwrap();
    let ledger = measured(|| {
        let reader = std::io::BufReader::with_capacity(CHUNK, file);
        let (payload, records) = count_records(ArchiveIterator::new(reader));
        (payload, records, None, None)
    });
    print_row(
        "L0  parser, file on disk",
        "the baseline every layer builds on: fastwarc + the one disk-read copy",
        &ledger,
    );
    ledger
}

/// `BufRead` + identity `Seek` over a socket, so the parser can consume a
/// stream exactly like the gRPC service does (same trick as its
/// `ChannelReader`).
struct StreamRead<R: Read> {
    inner: std::io::BufReader<R>,
    pos: u64,
}

impl<R: Read> Read for StreamRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl<R: Read> BufRead for StreamRead<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.inner.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        self.pos += amt as u64;
        self.inner.consume(amt);
    }
}

impl<R: Read> Seek for StreamRead<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Current(0) => Ok(self.pos),
            SeekFrom::Start(p) if p == self.pos => Ok(self.pos),
            _ => Err(std::io::Error::new(std::io::ErrorKind::Unsupported, "stream input")),
        }
    }
}

fn l2_socket_parser(path: &str) -> Ledger {
    use std::os::unix::net::{UnixListener, UnixStream};
    let sock = std::env::temp_dir().join(format!("fastwarc-compare-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let listener = UnixListener::bind(&sock).unwrap();
    let sender_path = path.to_owned();
    let sock2 = sock.clone();
    let sender = std::thread::spawn(move || {
        let mut file = std::fs::File::open(sender_path).unwrap();
        let mut conn = UnixStream::connect(&sock2).unwrap();
        let mut buf = vec![0u8; CHUNK];
        loop {
            match file.read(&mut buf).unwrap() {
                0 => break,
                n => {
                    if conn.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
    });
    let (conn, _) = listener.accept().unwrap();
    let ledger = measured(|| {
        let reader = RawReaderAdapter::new(StreamRead {
            inner: std::io::BufReader::with_capacity(CHUNK, conn),
            pos: 0,
        });
        let (payload, records) = count_records(ArchiveIterator::new(reader));
        (payload, records, None, None)
    });
    sender.join().unwrap();
    let _ = std::fs::remove_file(&sock);
    print_row(
        "L2  + socket delivery (no protocol)",
        "same parser, but the bytes now arrive through a unix socket: this row adds the two kernel copies, nothing else",
        &ledger,
    );
    ledger
}

// ===========================================================
// gRPC scenarios
// ===========================================================

fn parse_config(full: bool, local_path: Option<&str>) -> pb::ParseWarcConfig {
    pb::ParseWarcConfig {
        parse_http: false,
        verify_digests: false,
        input_buffer_size: CHUNK as u32,
        payload_chunk_size: (1024 << 10) as u32,
        include_payload: Some(full),
        include_headers: Some(full),
        response_batch_size: 64,
        archive_path: local_path.unwrap_or("").to_owned(),
        ..Default::default()
    }
}

/// Start a server on a fresh Unix socket.
async fn start_server() -> std::path::PathBuf {
    let sock = std::env::temp_dir().join(format!("fastwarc-compare-grpc-{}.sock", std::process::id()));
    let _ = std::fs::remove_file(&sock);
    let listener = tokio::net::UnixListener::bind(&sock).unwrap();
    tokio::spawn(
        fastwarc_grpc::transport::configure_server(tonic::transport::Server::builder())
            .add_service(fastwarc_grpc::transport::configure_warc_server(WarcServiceServer::new(
                WarcParser::with_local_files(),
            )))
            .serve_with_incoming(UnixListenerStream::new(listener)),
    );
    sock
}

async fn connect(sock: &std::path::Path) -> Channel {
    let sock = sock.to_path_buf();
    fastwarc_grpc::transport::configure_endpoint(tonic::transport::Endpoint::from_static("http://[::]:50051"))
        .connect_with_connector(service_fn(move |_: Uri| {
            let sock = sock.clone();
            async move { Ok::<_, std::io::Error>(TokioIo::new(UnixStream::connect(sock).await?)) }
        }))
        .await
        .unwrap()
}

/// Feed the config and then file chunks as `ParseWarcRequest`s.
fn feed_tonic(path: String, tx: tokio::sync::mpsc::Sender<pb::ParseWarcRequest>, config: pb::ParseWarcConfig) {
    std::thread::spawn(move || {
        if tx
            .blocking_send(pb::ParseWarcRequest {
                kind: Some(pb::parse_warc_request::Kind::Config(config)),
            })
            .is_err()
        {
            return;
        }
        let mut file = std::fs::File::open(&path).unwrap();
        loop {
            let mut buf = Vec::with_capacity(CHUNK);
            match Read::take(Read::by_ref(&mut file), CHUNK as u64).read_to_end(&mut buf) {
                Ok(0) => break,
                Ok(_) => {
                    let request = pb::ParseWarcRequest {
                        kind: Some(pb::parse_warc_request::Kind::Chunk(buf.into())),
                    };
                    if tx.blocking_send(request).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
}

/// Wire bytes an upload of `file_bytes` produces: one config message plus a
/// framed chunk per `CHUNK` slice.
fn upload_wire_bytes(file_bytes: u64, config: &pb::ParseWarcConfig) -> u64 {
    let config_msg = pb::ParseWarcRequest {
        kind: Some(pb::parse_warc_request::Kind::Config(config.clone())),
    };
    let mut wire = 5 + config_msg.encoded_len() as u64;
    let full_chunks = file_bytes / CHUNK as u64;
    wire += full_chunks * (chunk_frame_overhead(CHUNK) + CHUNK as u64);
    let tail = file_bytes % CHUNK as u64;
    if tail > 0 {
        wire += chunk_frame_overhead(tail as usize) + tail;
    }
    wire
}

/// Wire overhead of one framed chunk message: the 5-byte gRPC header, the
/// protobuf tag byte, and the length varint.
fn chunk_frame_overhead(chunk_len: usize) -> u64 {
    5 + 1 + prost::length_delimiter_len(chunk_len) as u64
}

/// Encode `msg` as one length-prefixed gRPC frame through a reusable
/// scratch buffer.
fn encode_grpc_frame(scratch: &mut prost::bytes::BytesMut, msg: &impl prost::Message) -> Bytes {
    use prost::bytes::BufMut;
    let body_len = msg.encoded_len();
    scratch.reserve(5 + body_len);
    scratch.put_u8(0);
    scratch.put_u32(u32::try_from(body_len).expect("message under 4 GiB"));
    msg.encode(scratch).expect("BytesMut has reserved capacity");
    scratch.split().freeze()
}

fn count_end(resp: &pb::ParseWarcResponse, payload: &mut u64, records: &mut usize) {
    match resp.kind.as_ref() {
        Some(pb::parse_warc_response::Kind::RecordEnd(end)) => {
            *payload += end.payload_length;
            *records += 1;
        }
        Some(pb::parse_warc_response::Kind::Batch(batch)) => {
            for item in &batch.items {
                count_end(item, payload, records);
            }
        }
        Some(pb::parse_warc_response::Kind::RecordError(e)) if !e.recoverable => {
            eprintln!("record error: {}", e.message);
        }
        _ => {}
    }
}

/// L1: the service in front of a file it reads itself. gRPC session,
/// streaming metadata replies, no archive bytes on the wire.
fn l1_grpc_disk(path: &str) -> Ledger {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let path_abs = std::fs::canonicalize(path).unwrap().to_string_lossy().into_owned();
    let ledger = measured(|| {
        runtime.block_on(async {
            let sock = start_server().await;
            let mut client = WarcServiceClient::new(connect(&sock).await)
                .max_decoding_message_size(fastwarc_grpc::transport::MAX_MESSAGE_SIZE);
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            tx.send(pb::ParseWarcRequest {
                kind: Some(pb::parse_warc_request::Kind::Config(parse_config(false, Some(&path_abs)))),
            })
            .await
            .unwrap();
            drop(tx);
            let mut stream = client.parse_warc(ReceiverStream::new(rx)).await.unwrap().into_inner();
            let (mut payload, mut records, mut wire_out) = (0u64, 0usize, 0u64);
            while let Some(resp) = stream.message().await.unwrap() {
                wire_out += 5 + resp.encoded_len() as u64;
                count_end(&resp, &mut payload, &mut records);
            }
            let _ = std::fs::remove_file(&sock);
            (payload, records, None, Some(wire_out))
        })
    });
    print_row(
        "L1  + gRPC service (file stays on disk)",
        "adds the whole gRPC apparatus (session, protobuf, streaming replies) but no archive bytes on the wire: prices gRPC itself",
        &ledger,
    );
    ledger
}

fn l3_grpc_upload(path: &str, file_bytes: u64, full: bool) -> Ledger {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let config = parse_config(full, None);
    let wire_in = upload_wire_bytes(file_bytes, &config);
    let path = path.to_owned();
    let ledger = measured(|| {
        runtime.block_on(async {
            let sock = start_server().await;
            let mut client = WarcServiceClient::new(connect(&sock).await)
                .max_decoding_message_size(fastwarc_grpc::transport::MAX_MESSAGE_SIZE)
                .max_encoding_message_size(fastwarc_grpc::transport::MAX_MESSAGE_SIZE);
            let (tx, rx) = tokio::sync::mpsc::channel(512);
            feed_tonic(path, tx, config);
            let mut stream = client.parse_warc(ReceiverStream::new(rx)).await.unwrap().into_inner();
            let (mut payload, mut records, mut wire_out) = (0u64, 0usize, 0u64);
            while let Some(resp) = stream.message().await.unwrap() {
                wire_out += 5 + resp.encoded_len() as u64;
                count_end(&resp, &mut payload, &mut records);
            }
            let _ = std::fs::remove_file(&sock);
            (payload, records, Some(wire_in), Some(wire_out))
        })
    });
    let (label, why) = if full {
        (
            "L3F gRPC upload + full payload echo",
            "L3 with every payload byte streamed back: the wire carries the archive up AND down",
        )
    } else {
        (
            "L3  + gRPC upload (client streams the bytes)",
            "the full stack an off-the-shelf gRPC client pays: HTTP/2 transport plus the codec copies on send and receive",
        )
    };
    print_row(label, why, &ledger);
    ledger
}

fn jobs_grpc_tonic(path: &str, file_bytes: u64, jobs: usize) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let config = parse_config(false, None);
    let wire_in = upload_wire_bytes(file_bytes, &config) * jobs as u64;
    let path = path.to_owned();
    let ledger = measured(|| {
        runtime.block_on(async {
            let sock = start_server().await;
            let mut handles = Vec::new();
            for _ in 0..jobs {
                let mut client = WarcServiceClient::new(connect(&sock).await)
                    .max_decoding_message_size(fastwarc_grpc::transport::MAX_MESSAGE_SIZE)
                    .max_encoding_message_size(fastwarc_grpc::transport::MAX_MESSAGE_SIZE);
                let (tx, rx) = tokio::sync::mpsc::channel(512);
                feed_tonic(path.clone(), tx, config.clone());
                handles.push(tokio::spawn(async move {
                    let mut stream = client.parse_warc(ReceiverStream::new(rx)).await.unwrap().into_inner();
                    let (mut payload, mut records, mut wire_out) = (0u64, 0usize, 0u64);
                    while let Some(resp) = stream.message().await.unwrap() {
                        wire_out += 5 + resp.encoded_len() as u64;
                        count_end(&resp, &mut payload, &mut records);
                    }
                    (payload, records, wire_out)
                }));
            }
            let (mut payload, mut records, mut wire_out) = (0u64, 0usize, 0u64);
            for handle in handles {
                let (p, r, w) = handle.await.unwrap();
                payload += p;
                records += r;
                wire_out += w;
            }
            let _ = std::fs::remove_file(&sock);
            (payload, records, Some(wire_in), Some(wire_out))
        })
    });
    print_row(
        &format!("C{jobs}  {jobs} concurrent gzip uploads (tonic codec)"),
        "per-stream socket cost stops mattering once streams stack: aggregate throughput scales with cores until decompression saturates them",
        &ledger,
    );
    println!("    per-stream average: {:.0} MiB/s across {jobs} streams", ledger.rate() / jobs as f64);
}

// ===========================================================
// Output ladder: input is always the file on disk; each row changes only
// what happens to the parse results.
// ===========================================================

/// Consumer of parse results for the output ladder.
trait OutputSink {
    fn record_start(&mut self, record: &fastwarc::warc::record::WarcRecord, idx: u64);
    fn payload_window(&mut self, idx: u64, offset: u64, window: &[u8]);
    fn record_end(&mut self, idx: u64, payload_len: u64);
    /// Flush and return bytes produced (serialized or written), if any.
    fn finish(&mut self) -> Option<u64>;
}

/// Parse from disk, read every payload byte, hand results to `sink`.
fn run_output(path: &str, sink: &mut impl OutputSink) -> (u64, usize) {
    let file = std::fs::File::open(path).unwrap();
    let reader = std::io::BufReader::with_capacity(CHUNK, file);
    let mut payload_total = 0u64;
    let mut records = 0usize;
    let mut idx = 0u64;
    for record in ArchiveIterator::new(reader).with_parse_http(false) {
        let Ok(record) = record else { continue };
        let mut record = record.borrow_mut();
        sink.record_start(&record, idx);
        let mut offset = 0u64;
        if let Some(reader) = record.reader_mut() {
            loop {
                let window = reader.fill_buf().unwrap();
                if window.is_empty() {
                    break;
                }
                let n = window.len();
                sink.payload_window(idx, offset, window);
                reader.consume(n);
                offset += n as u64;
            }
        }
        sink.record_end(idx, offset);
        payload_total += offset;
        records += 1;
        idx += 1;
    }
    (payload_total, records)
}

/// O0: read payloads, produce nothing.
struct DiscardSink;

impl OutputSink for DiscardSink {
    fn record_start(&mut self, _: &fastwarc::warc::record::WarcRecord, _: u64) {}
    fn payload_window(&mut self, _: u64, _: u64, _: &[u8]) {}
    fn record_end(&mut self, _: u64, _: u64) {}
    fn finish(&mut self) -> Option<u64> {
        None
    }
}

/// O1/O3: build the service's protobuf messages (RecordStart with lossless
/// headers, PayloadChunk per window, RecordEnd), batch 64 per gRPC frame,
/// encode; either discard the encoded frames or write them to a socket.
struct ProtoSink {
    batch: Vec<pb::ParseWarcResponse>,
    batch_bytes: usize,
    scratch: prost::bytes::BytesMut,
    out: Option<std::os::unix::net::UnixStream>,
    produced: u64,
}

impl ProtoSink {
    fn new(out: Option<std::os::unix::net::UnixStream>) -> Self {
        Self {
            batch: Vec::with_capacity(64),
            batch_bytes: 0,
            scratch: prost::bytes::BytesMut::new(),
            out,
            produced: 0,
        }
    }

    fn push(&mut self, resp: pb::ParseWarcResponse, approx: usize) {
        self.batch.push(resp);
        self.batch_bytes += approx;
        if self.batch.len() >= 64 || self.batch_bytes >= (2 << 20) {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.batch.is_empty() {
            return;
        }
        let items = std::mem::take(&mut self.batch);
        self.batch_bytes = 0;
        let msg = pb::ParseWarcResponse {
            kind: Some(pb::parse_warc_response::Kind::Batch(pb::RecordBatch { items })),
        };
        let frame = encode_grpc_frame(&mut self.scratch, &msg);
        self.produced += frame.len() as u64;
        if let Some(out) = &mut self.out {
            out.write_all(&frame).unwrap();
        }
    }
}

impl OutputSink for ProtoSink {
    fn record_start(&mut self, record: &fastwarc::warc::record::WarcRecord, idx: u64) {
        let metadata = fastwarc_grpc::convert::record_metadata(record, idx, true);
        self.push(
            pb::ParseWarcResponse {
                kind: Some(pb::parse_warc_response::Kind::RecordStart(pb::RecordStart {
                    metadata: Some(metadata),
                })),
            },
            512,
        );
    }

    fn payload_window(&mut self, idx: u64, offset: u64, window: &[u8]) {
        // Same single copy out of the reader window the service makes.
        let data = Bytes::copy_from_slice(window);
        let approx = data.len() + 64;
        self.push(
            pb::ParseWarcResponse {
                kind: Some(pb::parse_warc_response::Kind::PayloadChunk(pb::PayloadChunk {
                    record_index: idx,
                    offset,
                    data,
                })),
            },
            approx,
        );
    }

    fn record_end(&mut self, idx: u64, payload_len: u64) {
        self.push(
            pb::ParseWarcResponse {
                kind: Some(pb::parse_warc_response::Kind::RecordEnd(pb::RecordEnd {
                    record_index: idx,
                    payload_length: payload_len,
                    ..Default::default()
                })),
            },
            64,
        );
    }

    fn finish(&mut self) -> Option<u64> {
        self.flush();
        Some(self.produced)
    }
}

/// O2: write raw payload bytes to a socket, no structure at all.
struct RawSocketSink {
    out: std::os::unix::net::UnixStream,
    produced: u64,
}

impl OutputSink for RawSocketSink {
    fn record_start(&mut self, _: &fastwarc::warc::record::WarcRecord, _: u64) {}
    fn payload_window(&mut self, _: u64, _: u64, window: &[u8]) {
        self.out.write_all(window).unwrap();
        self.produced += window.len() as u64;
    }
    fn record_end(&mut self, _: u64, _: u64) {}
    fn finish(&mut self) -> Option<u64> {
        Some(self.produced)
    }
}

/// A Unix socket pair plus a thread that reads and discards one side.
fn discard_socket() -> (std::os::unix::net::UnixStream, std::thread::JoinHandle<()>) {
    let (write_half, mut read_half) = std::os::unix::net::UnixStream::pair().unwrap();
    let reader = std::thread::spawn(move || {
        let mut buf = vec![0u8; CHUNK];
        loop {
            match read_half.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    });
    (write_half, reader)
}

fn o0_parse_read(path: &str) -> Ledger {
    let ledger = measured(|| {
        let mut sink = DiscardSink;
        let (payload, records) = run_output(path, &mut sink);
        (payload, records, None, None)
    });
    print_row(
        "O0  parse + read payloads, produce nothing",
        "the output baseline: same parse as L0 but every payload byte is read out of the record, since results have to exist before they can be shipped",
        &ledger,
    );
    ledger
}

fn o1_serialize(path: &str) -> Ledger {
    let ledger = measured(|| {
        let mut sink = ProtoSink::new(None);
        let (payload, records) = run_output(path, &mut sink);
        let produced = sink.finish();
        (payload, records, None, produced)
    });
    print_row(
        "O1  + protobuf serialization, discard",
        "builds the service's exact response messages (metadata, payload chunks, batches of 64) and encodes them, but nothing leaves the process: prices serialization alone",
        &ledger,
    );
    ledger
}

fn o2_socket_raw(path: &str) -> Ledger {
    let (write_half, reader) = discard_socket();
    let ledger = measured(|| {
        let mut sink = RawSocketSink {
            out: write_half,
            produced: 0,
        };
        let (payload, records) = run_output(path, &mut sink);
        let produced = sink.finish();
        drop(sink);
        (payload, records, None, produced)
    });
    reader.join().unwrap();
    print_row(
        "O2  + raw payload bytes over a unix socket",
        "no protobuf, no framing: prices the output socket alone (the same two kernel copies as the input side)",
        &ledger,
    );
    ledger
}

fn o3_socket_proto(path: &str) -> Ledger {
    let (write_half, reader) = discard_socket();
    let ledger = measured(|| {
        let mut sink = ProtoSink::new(Some(write_half));
        let (payload, records) = run_output(path, &mut sink);
        let produced = sink.finish();
        drop(sink);
        (payload, records, None, produced)
    });
    reader.join().unwrap();
    print_row(
        "O3  + protobuf over the unix socket",
        "serialized results on the wire, still no gRPC: O2's socket carrying O1's messages",
        &ledger,
    );
    ledger
}

/// O4: the full stack on the output side only: server reads the file from
/// disk, client receives every payload byte through gRPC.
fn o4_grpc_echo(path: &str) -> Ledger {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let path_abs = std::fs::canonicalize(path).unwrap().to_string_lossy().into_owned();
    let ledger = measured(|| {
        runtime.block_on(async {
            let sock = start_server().await;
            let mut client = WarcServiceClient::new(connect(&sock).await)
                .max_decoding_message_size(fastwarc_grpc::transport::MAX_MESSAGE_SIZE);
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            tx.send(pb::ParseWarcRequest {
                kind: Some(pb::parse_warc_request::Kind::Config(parse_config(true, Some(&path_abs)))),
            })
            .await
            .unwrap();
            drop(tx);
            let mut stream = client.parse_warc(ReceiverStream::new(rx)).await.unwrap().into_inner();
            let (mut payload, mut records, mut wire_out) = (0u64, 0usize, 0u64);
            while let Some(resp) = stream.message().await.unwrap() {
                wire_out += 5 + resp.encoded_len() as u64;
                count_end(&resp, &mut payload, &mut records);
            }
            let _ = std::fs::remove_file(&sock);
            (payload, records, None, Some(wire_out))
        })
    });
    print_row(
        "O4  + the full gRPC stack (server reads disk, client receives everything)",
        "O3 plus HTTP/2, flow control, and the tonic codec on both ends: the complete output path as a real client sees it",
        &ledger,
    );
    ledger
}
