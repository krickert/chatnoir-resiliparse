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

//! gRPC wire framing without the tonic codec layer (experimental).
//!
//! Tonic's streaming codec copies every inbound HTTP/2 DATA frame into a
//! contiguous scratch buffer before decoding, and copies every outbound
//! message into a fresh frame buffer while encoding. For a bulk byte stream
//! both copies are pure overhead: the length-prefixed gRPC message format
//! (RFC: gRPC over HTTP/2, `Length-Prefixed-Message`) can be split off
//! received `Bytes` frames by reference, and a large `bytes` field can be
//! sent as its own DATA frame behind a small hand-encoded prefix.
//!
//! [`MessageSplitter`] implements the receive side: frames go in, complete
//! messages come out as `Bytes` slices of the original frames (a copy only
//! happens when a message straddles a frame boundary). [`encode_message`]
//! and [`chunk_prefix`] implement the send side.

use prost::bytes::{BufMut, Bytes, BytesMut};
use std::collections::VecDeque;
use tonic::Status;

/// `Length-Prefixed-Message` header: 1-byte compressed flag + u32 length.
const HEADER_LEN: usize = 5;

/// Splits gRPC length-prefixed messages out of a stream of HTTP/2 DATA
/// frames without merging the frames into one buffer.
#[derive(Default)]
pub struct MessageSplitter {
    segments: VecDeque<Bytes>,
    buffered: usize,
}

impl MessageSplitter {
    /// An empty splitter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue one received DATA frame.
    pub fn push(&mut self, frame: Bytes) {
        if !frame.is_empty() {
            self.buffered += frame.len();
            self.segments.push_back(frame);
        }
    }

    /// Pop the next complete message, or `None` until more frames arrive.
    ///
    /// The returned `Bytes` is a zero-copy slice of a queued frame whenever
    /// the message does not straddle a frame boundary.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` for a compressed-flag byte other than zero
    /// (this crate never negotiates gRPC compression) and `OutOfRange` for
    /// a message longer than `max_message_size`.
    pub fn next_message(&mut self, max_message_size: usize) -> Result<Option<Bytes>, Status> {
        if self.buffered < HEADER_LEN {
            return Ok(None);
        }
        let mut header = [0u8; HEADER_LEN];
        let mut copied = 0;
        for segment in &self.segments {
            let n = segment.len().min(HEADER_LEN - copied);
            header[copied..copied + n].copy_from_slice(&segment[..n]);
            copied += n;
            if copied == HEADER_LEN {
                break;
            }
        }
        if header[0] != 0 {
            return Err(Status::invalid_argument("compressed gRPC messages are not supported"));
        }
        let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
        if len > max_message_size {
            return Err(Status::out_of_range(format!(
                "message of {len} bytes exceeds the {max_message_size} byte limit"
            )));
        }
        if self.buffered < HEADER_LEN + len {
            return Ok(None);
        }
        self.consume(HEADER_LEN);
        Ok(Some(self.take(len)))
    }

    /// Drop `n` buffered bytes.
    fn consume(&mut self, mut n: usize) {
        self.buffered -= n;
        while n > 0 {
            let front = self.segments.front_mut().expect("buffered bytes tracked");
            if front.len() > n {
                let _ = front.split_to(n);
                return;
            }
            n -= front.len();
            self.segments.pop_front();
        }
    }

    /// Remove and return `n` buffered bytes.
    fn take(&mut self, n: usize) -> Bytes {
        self.buffered -= n;
        let front = self.segments.front_mut().expect("caller checked buffered");
        if front.len() >= n {
            let out = front.split_to(n);
            if front.is_empty() {
                self.segments.pop_front();
            }
            return out;
        }
        // Message straddles frames: assemble (rare; frame-aligned senders
        // never hit this for the bulk chunk path).
        let mut out = BytesMut::with_capacity(n);
        let mut remaining = n;
        while remaining > 0 {
            let front = self.segments.front_mut().expect("caller checked buffered");
            let take = front.len().min(remaining);
            out.extend_from_slice(&front.split_to(take));
            remaining -= take;
            if front.is_empty() {
                self.segments.pop_front();
            }
        }
        out.freeze()
    }
}

/// Encode a whole protobuf message as one length-prefixed gRPC frame.
///
/// For the small control and metadata messages the extra copy through the
/// contiguous buffer is irrelevant; use [`chunk_prefix`] for bulk bytes.
///
/// # Panics
///
/// Panics if the encoded message exceeds 4 GiB, the gRPC frame length
/// limit.
#[must_use]
pub fn encode_message(msg: &impl prost::Message) -> Bytes {
    encode_message_into(&mut BytesMut::new(), msg)
}

/// [`encode_message`] through a reusable scratch buffer.
///
/// When the previously split-off message has been sent and dropped,
/// `reserve` reclaims the allocation instead of mapping fresh pages, which
/// matters on streams that encode gigabytes of responses.
///
/// # Panics
///
/// Panics if the encoded message exceeds 4 GiB, the gRPC frame length
/// limit.
#[must_use]
pub fn encode_message_into(scratch: &mut BytesMut, msg: &impl prost::Message) -> Bytes {
    let body_len = msg.encoded_len();
    scratch.reserve(HEADER_LEN + body_len);
    scratch.put_u8(0);
    scratch.put_u32(u32::try_from(body_len).expect("message under 4 GiB"));
    msg.encode(scratch).expect("BytesMut has reserved capacity");
    scratch.split().freeze()
}

/// gRPC frame header plus protobuf field prefix for a `ParseWarcRequest`
/// whose `chunk` field holds `chunk_len` bytes.
///
/// Sending `[chunk_prefix(chunk.len()), chunk]` as consecutive DATA frames
/// is wire-identical to encoding `ParseWarcRequest { kind: Chunk(chunk) }`
/// through the tonic codec, but the archive bytes are handed to HTTP/2 by
/// reference instead of being copied into a frame buffer.
///
/// # Panics
///
/// Panics if `chunk_len` exceeds 4 GiB, the gRPC frame length limit.
#[must_use]
pub fn chunk_prefix(chunk_len: usize) -> Bytes {
    let mut varint = [0u8; 10];
    let varint_len = {
        let mut value = chunk_len as u64;
        let mut i = 0;
        loop {
            let byte = (value & 0x7f) as u8;
            value >>= 7;
            if value == 0 {
                varint[i] = byte;
                break i + 1;
            }
            varint[i] = byte | 0x80;
            i += 1;
        }
    };
    // Message body: tag (field 2, wire type LEN) + length varint + payload.
    let body_len = 1 + varint_len + chunk_len;
    let mut buf = BytesMut::with_capacity(HEADER_LEN + 1 + varint_len);
    buf.put_u8(0);
    buf.put_u32(u32::try_from(body_len).expect("chunk under 4 GiB"));
    buf.put_u8(0x12);
    buf.put_slice(&varint[..varint_len]);
    buf.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::fastwarc::v1 as pb;
    use prost::Message;

    #[test]
    fn splitter_yields_zero_copy_aligned_messages() {
        let msg = pb::ParseWarcRequest {
            kind: Some(pb::parse_warc_request::Kind::Chunk(Bytes::from_static(b"WARC/1.1"))),
        };
        let framed = encode_message(&msg);
        let mut splitter = MessageSplitter::new();
        splitter.push(framed);
        let out = splitter.next_message(1 << 20).unwrap().unwrap();
        assert_eq!(pb::ParseWarcRequest::decode(out).unwrap(), msg);
        assert!(splitter.next_message(1 << 20).unwrap().is_none());
    }

    #[test]
    fn splitter_reassembles_straddling_messages() {
        let msg = pb::ParseWarcRequest {
            kind: Some(pb::parse_warc_request::Kind::Chunk(Bytes::from(vec![7u8; 300]))),
        };
        let framed = encode_message(&msg);
        let mut splitter = MessageSplitter::new();
        for piece in framed.chunks(11) {
            splitter.push(Bytes::copy_from_slice(piece));
        }
        let out = splitter.next_message(1 << 20).unwrap().unwrap();
        assert_eq!(pb::ParseWarcRequest::decode(out).unwrap(), msg);
    }

    #[test]
    fn chunk_prefix_matches_prost_encoding() {
        for len in [0usize, 1, 127, 128, 65536, 1 << 20] {
            let chunk = Bytes::from(vec![0xabu8; len]);
            let via_prost = encode_message(&pb::ParseWarcRequest {
                kind: Some(pb::parse_warc_request::Kind::Chunk(chunk.clone())),
            });
            let mut hand = BytesMut::from(&chunk_prefix(len)[..]);
            hand.extend_from_slice(&chunk);
            assert_eq!(&hand[..], &via_prost[..], "len {len}");
        }
    }

    #[test]
    fn splitter_rejects_compressed_flag() {
        let mut splitter = MessageSplitter::new();
        splitter.push(Bytes::from_static(&[1, 0, 0, 0, 0]));
        assert!(splitter.next_message(1 << 20).is_err());
    }

    #[test]
    fn splitter_rejects_oversized_message() {
        let mut splitter = MessageSplitter::new();
        splitter.push(Bytes::from_static(&[0, 0xff, 0xff, 0xff, 0xff]));
        assert!(splitter.next_message(1 << 20).is_err());
    }
}
