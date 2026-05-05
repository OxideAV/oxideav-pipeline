//! Multi-stream `transcode_simple` tests.
//!
//! Exercises the lift of the historical "single-stream inputs only"
//! restriction. A self-contained 2-stream stub demuxer feeds two
//! audio-shaped streams (different codec ids / sample rates so the
//! per-route plumbing is observable), the test calls
//! [`transcode_simple`] with a per-stream plan picker (one Copy + one
//! Reencode), and a stub muxer captures every packet it sees.
//!
//! The assertions verify:
//!   * One output stream per non-Drop input stream.
//!   * The muxer header sees the encoder's `output_params()` for
//!     reencoded routes and the input params for copy routes.
//!   * Every output packet's `stream_index` is the muxer-side index,
//!     not the encoder's per-encoder `0`.
//!   * The Drop variant excludes a stream from the output entirely.

use std::sync::{Arc, Mutex};

use oxideav_core::registry::CodecInfo;
use oxideav_core::{
    packet::PacketFlags, AudioFrame, CodecCapabilities, CodecId, CodecParameters, CodecRegistry,
    Decoder, DecoderFactory, Demuxer, Encoder, EncoderFactory, Error, Frame, Muxer, Packet, Result,
    SampleFormat, StreamInfo, TimeBase,
};
use oxideav_pipeline::{transcode_simple, StreamPlan};

// ---- two-stream stub demuxer ----

const A_CODEC_ID: &str = "stub_test_audio_a";
const B_CODEC_ID: &str = "stub_test_audio_b";
const A_OUT_CODEC_ID: &str = "stub_test_audio_a_out";

const A_SR: u32 = 8_000;
const B_SR: u32 = 16_000;

struct TwoStreamStubDemuxer {
    streams: Vec<StreamInfo>,
    /// `(stream_index, pts)` packets remaining to emit, interleaved.
    remaining: std::collections::VecDeque<(u32, i64)>,
}

impl TwoStreamStubDemuxer {
    fn new(packets_per_stream: usize) -> Self {
        let mut params_a = CodecParameters::audio(CodecId::new(A_CODEC_ID));
        params_a.sample_rate = Some(A_SR);
        params_a.channels = Some(1);
        params_a.sample_format = Some(SampleFormat::S16);

        let mut params_b = CodecParameters::audio(CodecId::new(B_CODEC_ID));
        params_b.sample_rate = Some(B_SR);
        params_b.channels = Some(1);
        params_b.sample_format = Some(SampleFormat::S16);

        let streams = vec![
            StreamInfo {
                index: 0,
                time_base: TimeBase::new(1, A_SR as i64),
                duration: None,
                start_time: Some(0),
                params: params_a,
            },
            StreamInfo {
                index: 1,
                time_base: TimeBase::new(1, B_SR as i64),
                duration: None,
                start_time: Some(0),
                params: params_b,
            },
        ];

        // Interleave: A-pkt0, B-pkt0, A-pkt1, B-pkt1, ...
        let mut q = std::collections::VecDeque::with_capacity(packets_per_stream * 2);
        for i in 0..packets_per_stream {
            q.push_back((0u32, i as i64 * 800));
            q.push_back((1u32, i as i64 * 1600));
        }
        Self {
            streams,
            remaining: q,
        }
    }
}

impl Demuxer for TwoStreamStubDemuxer {
    fn format_name(&self) -> &str {
        "two_stream_stub"
    }
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }
    fn next_packet(&mut self) -> Result<Packet> {
        let (stream_index, pts) = self.remaining.pop_front().ok_or(Error::Eof)?;
        let (sr, samples) = if stream_index == 0 {
            (A_SR, 800u32) // 100 ms @ 8 kHz
        } else {
            (B_SR, 1600u32) // 100 ms @ 16 kHz
        };
        Ok(Packet {
            stream_index,
            time_base: TimeBase::new(1, sr as i64),
            pts: Some(pts),
            dts: Some(pts),
            duration: Some(samples as i64),
            flags: PacketFlags::default(),
            data: vec![0u8; (samples as usize) * 2], // S16 mono
        })
    }
    fn seek_to(&mut self, _stream_index: u32, _pts: i64) -> Result<i64> {
        Err(Error::unsupported("stub: no seek"))
    }
}

// ---- stub decoder (passthrough) for both source codecs ----

struct StubDecoder {
    id: CodecId,
    pending: Option<Packet>,
}

impl Decoder for StubDecoder {
    fn codec_id(&self) -> &CodecId {
        &self.id
    }
    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        self.pending = Some(packet.clone());
        Ok(())
    }
    fn receive_frame(&mut self) -> Result<Frame> {
        match self.pending.take() {
            Some(p) => {
                let samples = (p.data.len() / 2) as u32;
                Ok(Frame::Audio(AudioFrame {
                    samples,
                    pts: p.pts,
                    data: vec![p.data],
                }))
            }
            None => Err(Error::NeedMore),
        }
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn reset(&mut self) -> Result<()> {
        self.pending = None;
        Ok(())
    }
}

fn make_decoder_a(_p: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(StubDecoder {
        id: CodecId::new(A_CODEC_ID),
        pending: None,
    }))
}

fn make_decoder_b(_p: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(StubDecoder {
        id: CodecId::new(B_CODEC_ID),
        pending: None,
    }))
}

// ---- stub encoder for the A-out codec ----

struct StubEncoder {
    out_params: CodecParameters,
    queue: std::collections::VecDeque<Packet>,
}

impl Encoder for StubEncoder {
    fn codec_id(&self) -> &CodecId {
        &self.out_params.codec_id
    }
    fn output_params(&self) -> &CodecParameters {
        &self.out_params
    }
    fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        let Frame::Audio(a) = frame else {
            return Err(Error::invalid("stub encoder: audio only"));
        };
        let mut pkt = Packet::new(
            // Encoders emit `stream_index = 0` — `transcode_simple`
            // patches it to the muxer-side index. The test asserts
            // exactly that: see the per-stream-index counts below.
            0,
            TimeBase::new(1, self.out_params.sample_rate.unwrap_or(A_SR) as i64),
            a.data.first().cloned().unwrap_or_default(),
        );
        pkt.pts = a.pts;
        pkt.dts = a.pts;
        pkt.duration = Some(a.samples as i64);
        pkt.flags.keyframe = true;
        self.queue.push_back(pkt);
        Ok(())
    }
    fn receive_packet(&mut self) -> Result<Packet> {
        self.queue.pop_front().ok_or(Error::NeedMore)
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn make_encoder_a_out(p: &CodecParameters) -> Result<Box<dyn Encoder>> {
    Ok(Box::new(StubEncoder {
        out_params: p.clone(),
        queue: std::collections::VecDeque::new(),
    }))
}

// ---- stub muxer that captures packets + the per-stream params it was opened with ----

#[derive(Default)]
struct CapturedMuxer {
    streams: Vec<StreamInfo>,
    header_written: bool,
    trailer_written: bool,
    packets: Vec<Packet>,
}

impl CapturedMuxer {
    fn new(streams: &[StreamInfo]) -> Self {
        Self {
            streams: streams.to_vec(),
            ..Default::default()
        }
    }
}

struct CapturedMuxerHandle {
    inner: Arc<Mutex<CapturedMuxer>>,
}

impl Muxer for CapturedMuxerHandle {
    fn format_name(&self) -> &str {
        "captured"
    }
    fn write_header(&mut self) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.header_written = true;
        Ok(())
    }
    fn write_packet(&mut self, packet: &Packet) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.packets.push(packet.clone());
        Ok(())
    }
    fn write_trailer(&mut self) -> Result<()> {
        let mut g = self.inner.lock().unwrap();
        g.trailer_written = true;
        Ok(())
    }
}

// ---- registry helper ----

fn build_codec_registry() -> CodecRegistry {
    let mut reg = CodecRegistry::default();

    // Two source decoders.
    reg.register(
        CodecInfo::new(CodecId::new(A_CODEC_ID))
            .capabilities(CodecCapabilities::audio(A_CODEC_ID).with_decode())
            .decoder(make_decoder_a as DecoderFactory),
    );
    reg.register(
        CodecInfo::new(CodecId::new(B_CODEC_ID))
            .capabilities(CodecCapabilities::audio(B_CODEC_ID).with_decode())
            .decoder(make_decoder_b as DecoderFactory),
    );

    // One output encoder (only the A stream gets re-encoded in the
    // primary test; B uses Copy and needs no encoder).
    reg.register(
        CodecInfo::new(CodecId::new(A_OUT_CODEC_ID))
            .capabilities(CodecCapabilities::audio(A_OUT_CODEC_ID).with_encode())
            .encoder(make_encoder_a_out as EncoderFactory),
    );

    reg
}

// ---- tests ----

const PACKETS_PER_STREAM: usize = 5;

#[test]
fn multi_stream_mixed_copy_and_reencode() {
    // Two-stream input → both streams flow to the muxer; stream 0 goes
    // through the decoder/encoder pair, stream 1 is copied verbatim.
    let codecs = build_codec_registry();
    let mut demuxer = TwoStreamStubDemuxer::new(PACKETS_PER_STREAM);

    let captured = Arc::new(Mutex::new(CapturedMuxer::default()));
    let captured_clone = captured.clone();

    let muxer_open = move |streams: &[StreamInfo]| -> Result<Box<dyn Muxer>> {
        let mut g = captured_clone.lock().unwrap();
        *g = CapturedMuxer::new(streams);
        drop(g);
        Ok(Box::new(CapturedMuxerHandle {
            inner: captured_clone.clone(),
        }))
    };

    let plan_for = |stream: &StreamInfo| -> Result<StreamPlan> {
        if stream.index == 0 {
            Ok(StreamPlan::Reencode {
                output_codec: A_OUT_CODEC_ID.into(),
            })
        } else {
            Ok(StreamPlan::Copy)
        }
    };

    let stats = transcode_simple(&mut demuxer, muxer_open, &codecs, plan_for)
        .expect("transcode_simple two-stream mixed plan");

    // Every input packet flowed.
    assert_eq!(stats.packets_in as usize, PACKETS_PER_STREAM * 2);
    // One frame decoded per re-encoded input packet (stream 0).
    assert_eq!(stats.frames_decoded as usize, PACKETS_PER_STREAM);
    // Stream 1 (copy) + stream 0 (encoded): one out-packet per in-packet.
    assert_eq!(stats.packets_out as usize, PACKETS_PER_STREAM * 2);

    let captured = captured.lock().unwrap();
    assert!(captured.header_written, "muxer header was not written");
    assert!(captured.trailer_written, "muxer trailer was not written");
    assert_eq!(
        captured.streams.len(),
        2,
        "muxer was opened with the wrong number of output streams"
    );
    assert_eq!(captured.streams[0].index, 0);
    assert_eq!(captured.streams[1].index, 1);

    // Reencode-route output stream carries the encoder's output params
    // (codec id == A_OUT_CODEC_ID), not the input's.
    assert_eq!(
        captured.streams[0].params.codec_id.as_str(),
        A_OUT_CODEC_ID,
        "reencode route's output stream did not carry the encoder's codec id"
    );
    // Copy-route output stream still carries the source codec id (B).
    assert_eq!(
        captured.streams[1].params.codec_id.as_str(),
        B_CODEC_ID,
        "copy route's output stream lost the source codec id"
    );

    // Every captured packet's stream_index is the muxer-side index.
    let mut by_index = [0usize; 2];
    for pkt in &captured.packets {
        let idx = pkt.stream_index as usize;
        assert!(
            idx < 2,
            "captured packet has out-of-range stream_index {idx}; \
             encoder must not have leaked stream_index = 0 for the muxer"
        );
        by_index[idx] += 1;
    }
    assert_eq!(by_index[0], PACKETS_PER_STREAM);
    assert_eq!(by_index[1], PACKETS_PER_STREAM);

    // Source-order interleave is preserved on the copy path: the muxer
    // sees alternating stream-0 / stream-1 packets in the same order
    // the demuxer emits them (the encoder synchronously emits one
    // packet per input frame for the stub, so the per-route ordering
    // matches too).
    for (i, pkt) in captured.packets.iter().enumerate() {
        let expected = if i % 2 == 0 { 0u32 } else { 1u32 };
        assert_eq!(
            pkt.stream_index, expected,
            "interleave drifted at packet {i}: got stream {} expected {expected}",
            pkt.stream_index
        );
    }
}

#[test]
fn multi_stream_drop_excludes_stream_from_output() {
    // Same input, but plan_for returns Drop for stream 1 → the muxer
    // sees a single output stream (the input stream 0, copied).
    let codecs = build_codec_registry();
    let mut demuxer = TwoStreamStubDemuxer::new(PACKETS_PER_STREAM);

    let captured = Arc::new(Mutex::new(CapturedMuxer::default()));
    let captured_clone = captured.clone();

    let muxer_open = move |streams: &[StreamInfo]| -> Result<Box<dyn Muxer>> {
        let mut g = captured_clone.lock().unwrap();
        *g = CapturedMuxer::new(streams);
        drop(g);
        Ok(Box::new(CapturedMuxerHandle {
            inner: captured_clone.clone(),
        }))
    };

    let plan_for = |stream: &StreamInfo| -> Result<StreamPlan> {
        if stream.index == 0 {
            Ok(StreamPlan::Copy)
        } else {
            Ok(StreamPlan::Drop)
        }
    };

    let stats = transcode_simple(&mut demuxer, muxer_open, &codecs, plan_for)
        .expect("transcode_simple two-stream copy + drop");

    // Both input streams' packets are read…
    assert_eq!(stats.packets_in as usize, PACKETS_PER_STREAM * 2);
    // …but only stream 0's packets are written out.
    assert_eq!(stats.packets_out as usize, PACKETS_PER_STREAM);
    assert_eq!(stats.frames_decoded, 0);

    let captured = captured.lock().unwrap();
    assert_eq!(
        captured.streams.len(),
        1,
        "drop did not exclude stream 1 from the muxer"
    );
    assert_eq!(captured.streams[0].index, 0);
    for pkt in &captured.packets {
        assert_eq!(pkt.stream_index, 0);
    }
}

#[test]
fn dropping_every_stream_is_an_error() {
    // Edge case: plan_for returns Drop for every stream → no output
    // streams, no muxer to open. Surface as Error::Invalid rather than
    // silently producing an empty file.
    let codecs = build_codec_registry();
    let mut demuxer = TwoStreamStubDemuxer::new(PACKETS_PER_STREAM);

    let muxer_open = |_streams: &[StreamInfo]| -> Result<Box<dyn Muxer>> {
        unreachable!("muxer_open must not be called when every stream is dropped")
    };

    let err = transcode_simple(&mut demuxer, muxer_open, &codecs, |_| Ok(StreamPlan::Drop))
        .expect_err("expected error when every stream is dropped");

    assert!(
        format!("{err}").to_lowercase().contains("dropped"),
        "expected error message to mention dropped streams; got: {err}"
    );
}
