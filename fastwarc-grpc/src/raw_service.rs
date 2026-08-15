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

//! Zero-copy `ParseWarc` route (experimental).
//!
//! [`RawWarcService`] serves the same `fastwarc.v1.WarcService` contract as
//! the generated tonic server and is wire-compatible with every gRPC
//! client, but handles the `ParseWarc` method's HTTP/2 body directly:
//! request DATA frames are split into gRPC messages by reference
//! ([`crate::grpc_frame::MessageSplitter`]) instead of being merged into a
//! contiguous scratch buffer, so uploaded archive bytes reach the parser
//! without an extra copy. All other methods delegate to the generated
//! server unchanged.

use std::pin::Pin;
use std::task::{Context, Poll};

use http_body_util::BodyExt;
use prost::Message;
use prost::bytes::Bytes;
use tokio::sync::mpsc;
use tonic::body::Body;
use tonic::server::NamedService;
use tonic::{Status, codegen::http};

use crate::grpc_frame::{MessageSplitter, encode_message_into};
use crate::proto::fastwarc::v1 as pb;
use crate::proto::fastwarc::v1::warc_service_server::WarcServiceServer;
use crate::transport::MAX_MESSAGE_SIZE;
use crate::warc_service::{self, WarcParser};
use prost::bytes::BytesMut;

/// gRPC service wrapper that serves `ParseWarc` without the tonic codec.
///
/// Mount it exactly like the generated server:
///
/// ```no_run
/// # use fastwarc_grpc::raw_service::RawWarcService;
/// # use fastwarc_grpc::warc_service::WarcParser;
/// # async fn serve() -> Result<(), tonic::transport::Error> {
/// tonic::transport::Server::builder()
///     .add_service(RawWarcService::new(WarcParser::new()))
///     .serve("[::]:50051".parse().unwrap())
///     .await
/// # }
/// ```
#[derive(Clone)]
pub struct RawWarcService {
    parser: WarcParser,
    inner: WarcServiceServer<WarcParser>,
}

impl RawWarcService {
    /// Wrap a [`WarcParser`]; `ParseWarc` runs on the raw route, everything
    /// else on the generated server.
    #[must_use]
    pub fn new(parser: WarcParser) -> Self {
        let inner = crate::transport::configure_warc_server(WarcServiceServer::new(parser));
        Self { parser, inner }
    }
}

impl NamedService for RawWarcService {
    const NAME: &'static str = <WarcServiceServer<WarcParser> as NamedService>::NAME;
}

impl tower_service::Service<http::Request<Body>> for RawWarcService {
    type Response = http::Response<Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        if req.uri().path() == "/fastwarc.v1.WarcService/ParseWarc" {
            let parser = self.parser;
            Box::pin(async move { Ok(parse_warc_raw(req.into_body(), parser)) })
        } else {
            let mut inner = self.inner.clone();
            Box::pin(async move { tower_service::Service::call(&mut inner, req).await })
        }
    }
}

/// Handle one raw `ParseWarc` call: spawn the request pump and return the
/// streaming response.
fn parse_warc_raw(body: Body, parser: WarcParser) -> http::Response<Body> {
    let (resp_tx, resp_rx) = mpsc::channel(warc_service::RESPONSE_CHANNEL_BOUND);
    tokio::spawn(pump_request(body, resp_tx, parser));
    http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .body(Body::new(RawResponseBody {
            rx: resp_rx,
            scratch: BytesMut::new(),
            trailers: None,
            done: false,
        }))
        .expect("static response parts are valid")
}

/// Read request DATA frames, split and decode messages, and drive the
/// parse pipeline. Mirrors the protocol checks of the tonic route.
async fn pump_request(mut body: Body, resp_tx: warc_service::ResponseSender, parser: WarcParser) {
    let mut splitter = MessageSplitter::new();
    // Present after the config message has started the pipeline. The pump
    // keeps its own `resp_tx` clone so protocol violations found after the
    // pipeline launch still reach the client, mirroring the tonic route's
    // request forwarder.
    let mut chunk_tx: Option<mpsc::Sender<Bytes>> = None;

    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(status) => {
                let _ = resp_tx.send(Err(status)).await;
                return;
            }
        };
        let Ok(data) = frame.into_data() else { continue };
        splitter.push(data);
        loop {
            let message = match splitter.next_message(MAX_MESSAGE_SIZE) {
                Ok(Some(message)) => message,
                Ok(None) => break,
                Err(status) => {
                    let _ = resp_tx.send(Err(status)).await;
                    return;
                }
            };
            let request = match pb::ParseWarcRequest::decode(message) {
                Ok(request) => request,
                Err(e) => {
                    let _ = resp_tx
                        .send(Err(Status::invalid_argument(format!("malformed ParseWarcRequest: {e}"))))
                        .await;
                    return;
                }
            };
            match (request.kind, &mut chunk_tx) {
                (Some(pb::parse_warc_request::Kind::Config(config)), slot @ None) => {
                    if !config.archive_path.is_empty() && !parser.allow_local_files {
                        let _ = resp_tx
                            .send(Err(Status::permission_denied(
                                "archive_path is disabled on this server; stream the archive as chunks instead",
                            )))
                            .await;
                        return;
                    }
                    let (tx, rx) = mpsc::channel::<Bytes>(warc_service::chunk_channel_bound(config.input_buffer_size));
                    warc_service::start_pipeline(config, rx, resp_tx.clone());
                    *slot = Some(tx);
                }
                (Some(pb::parse_warc_request::Kind::Config(_)), Some(_)) => {
                    let _ = resp_tx
                        .send(Err(Status::invalid_argument("`config` may only be set on the first request message")))
                        .await;
                    return;
                }
                (Some(pb::parse_warc_request::Kind::Chunk(chunk)), Some(tx)) => {
                    if tx.send(chunk).await.is_err() {
                        return; // Parser gone (fatal error already reported).
                    }
                }
                (Some(pb::parse_warc_request::Kind::Chunk(_)), None) => {
                    let _ = resp_tx
                        .send(Err(Status::invalid_argument("first ParseWarc request message must set `config`")))
                        .await;
                    return;
                }
                (None, _) => {
                    let _ = resp_tx
                        .send(Err(Status::invalid_argument("ParseWarc request message must set `config` or `chunk`")))
                        .await;
                    return;
                }
            }
        }
    }
    if chunk_tx.is_none() {
        let _ = resp_tx
            .send(Err(Status::invalid_argument("empty ParseWarc request stream; first message must set `config`")))
            .await;
    }
    // Dropping chunk_tx signals EOF to the parser.
}

/// Streaming gRPC response body: encodes each protocol message as one
/// length-prefixed frame and finishes with `grpc-status` trailers.
struct RawResponseBody {
    rx: mpsc::Receiver<Result<pb::ParseWarcResponse, Status>>,
    /// Reused encode buffer; `reserve` reclaims it once the previous
    /// message's `Bytes` has been written and dropped by HTTP/2.
    scratch: BytesMut,
    trailers: Option<http::HeaderMap>,
    done: bool,
}

impl RawResponseBody {
    fn status_trailers(status: &Status) -> http::HeaderMap {
        let mut trailers = http::HeaderMap::with_capacity(2);
        if let Err(fallback) = status.add_header(&mut trailers) {
            let _ = fallback.add_header(&mut trailers);
        }
        trailers
    }
}

impl http_body::Body for RawResponseBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.done {
            return Poll::Ready(None);
        }
        if let Some(trailers) = self.trailers.take() {
            self.done = true;
            return Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))));
        }
        match self.rx.poll_recv(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(message))) => {
                let frame = encode_message_into(&mut self.scratch, &message);
                Poll::Ready(Some(Ok(http_body::Frame::data(frame))))
            }
            Poll::Ready(Some(Err(status))) => {
                self.rx.close();
                self.done = true;
                Poll::Ready(Some(Ok(http_body::Frame::trailers(Self::status_trailers(&status)))))
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(Some(Ok(http_body::Frame::trailers(Self::status_trailers(&Status::ok(""))))))
            }
        }
    }
}
