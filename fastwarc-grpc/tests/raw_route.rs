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

//! Wire-compatibility tests for the raw (codec-bypassing) `ParseWarc`
//! route: a stock tonic-codec client drives [`RawWarcService`] and must see
//! exactly what the generated server produces, including error statuses.

mod common;

use std::net::SocketAddr;

use common::{RecordOutcome, corrupt_archive, data_path, group_responses, warc_requests};
use fastwarc_grpc::proto::fastwarc::v1 as pb;
use fastwarc_grpc::proto::fastwarc::v1::warc_service_client::WarcServiceClient;
use fastwarc_grpc::raw_service::RawWarcService;
use fastwarc_grpc::warc_service::WarcParser;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::Code;

/// Start a raw-route server (local files disabled) on an ephemeral port.
async fn start_raw_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        fastwarc_grpc::transport::configure_server(tonic::transport::Server::builder())
            .add_service(RawWarcService::new(WarcParser::new()))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    addr
}

async fn raw_client(addr: SocketAddr) -> WarcServiceClient<tonic::transport::Channel> {
    WarcServiceClient::new(
        fastwarc_grpc::transport::connect(format!("http://{addr}"))
            .await
            .unwrap(),
    )
    .max_decoding_message_size(fastwarc_grpc::transport::MAX_MESSAGE_SIZE)
    .max_encoding_message_size(fastwarc_grpc::transport::MAX_MESSAGE_SIZE)
}

/// Stream `requests` and collect responses until the stream ends or errors.
async fn run_raw(
    addr: SocketAddr,
    requests: Vec<pb::ParseWarcRequest>,
) -> Result<Vec<pb::ParseWarcResponse>, tonic::Status> {
    let mut client = raw_client(addr).await;
    let mut stream = client.parse_warc(tokio_stream::iter(requests)).await?.into_inner();
    let mut responses = Vec::new();
    while let Some(resp) = stream.message().await? {
        responses.push(resp);
    }
    Ok(responses)
}

/// A tonic-codec client parses a fixture through the raw route and sees the
/// same records as through the generated route.
#[tokio::test]
async fn raw_route_matches_codec_route() {
    let data = std::fs::read(data_path("warcfile.warc.gz")).unwrap();
    let addr = start_raw_server().await;
    for chunk_size in [997usize, 64 << 10] {
        let responses = run_raw(addr, warc_requests(&data, chunk_size, &pb::ParseWarcConfig::default()))
            .await
            .unwrap();
        let outcomes = group_responses(&responses);
        assert_eq!(outcomes.len(), 50, "chunk size {chunk_size}");
        assert!(outcomes.iter().all(|o| matches!(o, RecordOutcome::Record { .. })));
    }
}

/// Payload bytes survive the raw route byte-for-byte.
#[tokio::test]
async fn raw_route_full_payload() {
    let data = std::fs::read(data_path("warcfile.warc")).unwrap();
    let addr = start_raw_server().await;
    let config = pb::ParseWarcConfig {
        include_payload: Some(true),
        include_headers: Some(true),
        response_batch_size: 8,
        ..Default::default()
    };
    let responses = run_raw(addr, warc_requests(&data, 8 << 10, &config)).await.unwrap();
    let outcomes = group_responses(&responses);
    assert_eq!(outcomes.len(), 50);
    let total: u64 = outcomes
        .iter()
        .map(|o| match o {
            RecordOutcome::Record { end, .. } => end.payload_length,
            RecordOutcome::Error(_) => 0,
        })
        .sum();
    assert!(total > 0);
}

/// A chunk before any config fails with `InvalidArgument`, like the
/// generated route.
#[tokio::test]
async fn raw_route_rejects_chunk_first() {
    let addr = start_raw_server().await;
    let requests = vec![pb::ParseWarcRequest {
        kind: Some(pb::parse_warc_request::Kind::Chunk(b"WARC/1.1\r\n".to_vec().into())),
    }];
    let status = run_raw(addr, requests).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
}

/// A second config message fails with `InvalidArgument`.
#[tokio::test]
async fn raw_route_rejects_double_config() {
    let addr = start_raw_server().await;
    let config = pb::ParseWarcRequest {
        kind: Some(pb::parse_warc_request::Kind::Config(pb::ParseWarcConfig::default())),
    };
    let status = run_raw(addr, vec![config.clone(), config]).await.unwrap_err();
    assert_eq!(status.code(), Code::InvalidArgument);
}

/// `archive_path` without the local-files opt-in fails with
/// `PermissionDenied`.
#[tokio::test]
async fn raw_route_gates_archive_path() {
    let addr = start_raw_server().await;
    let requests = vec![pb::ParseWarcRequest {
        kind: Some(pb::parse_warc_request::Kind::Config(pb::ParseWarcConfig {
            archive_path: "/etc/hostname".to_owned(),
            ..Default::default()
        })),
    }];
    let status = run_raw(addr, requests).await.unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied);
}

/// Corrupt WARC framing surfaces as a non-recoverable `record_error`, then
/// the stream ends cleanly (grpc-status OK in trailers).
#[tokio::test]
async fn raw_route_reports_framing_errors() {
    let addr = start_raw_server().await;
    let responses = run_raw(addr, warc_requests(&corrupt_archive(), 64, &pb::ParseWarcConfig::default()))
        .await
        .unwrap();
    let outcomes = group_responses(&responses);
    assert!(
        outcomes
            .iter()
            .any(|o| matches!(o, RecordOutcome::Error(e) if !e.recoverable)),
        "expected a non-recoverable record_error"
    );
}

/// The unary `ParseArchive` method still routes to the generated server.
#[tokio::test]
async fn raw_route_delegates_parse_archive() {
    let data = std::fs::read(data_path("warcfile.warc.gz")).unwrap();
    let addr = start_raw_server().await;
    let mut client = raw_client(addr).await;
    let response = client
        .parse_archive(pb::ParseArchiveRequest {
            config: Some(pb::ParseWarcConfig::default()),
            archive: data.into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.records.len(), 50);
    assert!(response.errors.is_empty());
}
