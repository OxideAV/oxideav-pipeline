//! Round-2 SourceRegistry typed-shape integration test.
//!
//! Exercises all three [`SourceOutput`](oxideav_core::SourceOutput)
//! variants end-to-end through the pipeline executor:
//!
//! 1. **`MockBytesSource`** — emits a hand-built WAV header + 8 PCM
//!    samples. Goes through demux → decode → frame chain.
//! 2. **`MockPacketSource`** — emits one Packet directly with a
//!    registered fake decoder. Goes through decode → frame chain
//!    (skipping demux).
//! 3. **`MockFrameSource`** — emits one Frame directly. Goes straight
//!    to the sink (skipping both demux + decode).
//!
//! Each variant runs through the same `@null`-style observer sink which
//! counts produced frames; the assertion is that all three paths
//! deliver the expected frame count.

use std::io::Cursor;
use std::sync::{Arc, Mutex};

use oxideav_core::{
    packet::PacketFlags, AudioFrame, BytesSource, CodecCapabilities, CodecId, CodecParameters,
    CodecResolver, ContainerRegistry, Decoder, DecoderFactory, Demuxer, Error, Frame, FrameSource,
    MediaType, OpenDemuxerFn, Packet, PacketSource, ReadSeek, Result, RuntimeContext, SampleFormat,
    SourceRegistry, StreamInfo, TimeBase,
};
use oxideav_core::{registry::CodecInfo, CodecRegistry};
use oxideav_pipeline::{Executor, Job, JobSink};

// ───────────────────────── observer sink ─────────────────────────

/// Observer sink: counts frames received per media kind. Same shape as
/// `oxideav-pipeline/tests/spectrogram_adds_video_stream.rs::ObserverSink`
/// but local so the two tests don't share an internal helper.
#[derive(Default)]
struct SinkCounters {
    audio_frames: usize,
    video_frames: usize,
}

struct CountingSink {
    shared: Arc<Mutex<SinkCounters>>,
}

impl JobSink for CountingSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        // The reserved `@null` sink in the executor would write packets
        // for copy paths; our test always drives the decoded-frame path.
        Ok(())
    }
    fn write_frame(&mut self, kind: MediaType, _frm: &Frame) -> Result<()> {
        let mut s = self.shared.lock().unwrap();
        match kind {
            MediaType::Audio => s.audio_frames += 1,
            MediaType::Video => s.video_frames += 1,
            _ => {}
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

// ───────────────────────── mock codec ─────────────────────────

/// Codec id for the in-test passthrough decoder. Different from the
/// shared `tests/common/stub.rs` codec id so tests can be wired in
/// parallel without name clashes.
const SOURCE_VAR_CODEC: &str = "source_variants_pcm";

fn make_decoder(_params: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(PassthroughDecoder { pending: None }))
}

/// 1:1 packet → AudioFrame passthrough. Mirrors `common::stub::StubDecoder`
/// but in-tree so this test file is self-contained.
struct PassthroughDecoder {
    pending: Option<Packet>,
}

impl Decoder for PassthroughDecoder {
    fn codec_id(&self) -> &CodecId {
        static ID: std::sync::OnceLock<CodecId> = std::sync::OnceLock::new();
        ID.get_or_init(|| CodecId::new(SOURCE_VAR_CODEC))
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

// ───────────────────────── bytes path: minimal WAV demuxer ─────────────────────────

const BYTES_CONTAINER: &str = "source_variants_wav";
const BYTES_EXT: &str = "svwav";

/// Minimal WAV-like demuxer: emits 8 PCM s16 mono samples in one
/// packet, then EOF. Container format identifier `BYTES_CONTAINER`;
/// extension `.svwav` triggers it via the registry's extension hint
/// when the URI does not start with a recognised scheme.
fn open_demuxer(
    _input: Box<dyn ReadSeek>,
    _codecs: &dyn CodecResolver,
) -> Result<Box<dyn Demuxer>> {
    Ok(Box::new(BytesDemuxer::new()))
}

struct BytesDemuxer {
    streams: Vec<StreamInfo>,
    emitted: bool,
}

impl BytesDemuxer {
    fn new() -> Self {
        let mut params = CodecParameters::audio(CodecId::new(SOURCE_VAR_CODEC));
        params.sample_rate = Some(8_000);
        params.channels = Some(1);
        params.sample_format = Some(SampleFormat::S16);
        let stream = StreamInfo {
            index: 0,
            time_base: TimeBase::new(1, 8_000),
            duration: Some(8),
            start_time: Some(0),
            params,
        };
        Self {
            streams: vec![stream],
            emitted: false,
        }
    }
}

impl Demuxer for BytesDemuxer {
    fn format_name(&self) -> &str {
        BYTES_CONTAINER
    }
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }
    fn next_packet(&mut self) -> Result<Packet> {
        if self.emitted {
            return Err(Error::Eof);
        }
        self.emitted = true;
        Ok(Packet {
            stream_index: 0,
            time_base: TimeBase::new(1, 8_000),
            pts: Some(0),
            dts: Some(0),
            duration: Some(8),
            flags: PacketFlags::default(),
            data: vec![0u8; 16], // 8 samples * 2 bytes (S16 mono)
        })
    }
    fn seek_to(&mut self, _stream_index: u32, _pts: i64) -> Result<i64> {
        self.emitted = false;
        Ok(0)
    }
}

/// Build a tiny "WAV-like" byte buffer. The custom demuxer ignores the
/// contents — the buffer just needs to exist so the source registry
/// has something to hand to the demuxer factory.
fn open_bytes(_uri: &str) -> Result<Box<dyn BytesSource>> {
    // Hand-built WAV header (44 bytes) + 8 zero PCM samples (16 bytes):
    // RIFF / WAVE / fmt / data — tiny but well-formed enough to not
    // be confused with anything else if a real WAV demuxer were to
    // probe it. Our `open_demuxer` ignores the contents either way.
    let mut buf = Vec::with_capacity(60);
    buf.extend_from_slice(b"RIFF");
    buf.extend_from_slice(&52u32.to_le_bytes()); // file size - 8
    buf.extend_from_slice(b"WAVE");
    buf.extend_from_slice(b"fmt ");
    buf.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size
    buf.extend_from_slice(&1u16.to_le_bytes()); // PCM
    buf.extend_from_slice(&1u16.to_le_bytes()); // mono
    buf.extend_from_slice(&8_000u32.to_le_bytes()); // sample rate
    buf.extend_from_slice(&16_000u32.to_le_bytes()); // byte rate
    buf.extend_from_slice(&2u16.to_le_bytes()); // block align
    buf.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
    buf.extend_from_slice(b"data");
    buf.extend_from_slice(&16u32.to_le_bytes()); // data chunk size
    buf.extend_from_slice(&[0u8; 16]);
    Ok(Box::new(Cursor::new(buf)))
}

// ───────────────────────── packet path: in-tree PacketSource ─────────────────────────

/// Emits exactly one packet with the same data shape as `BytesDemuxer`,
/// then EOF. The registered decoder is the same `PassthroughDecoder` so
/// the resulting frame chain is identical to the bytes path's — what
/// changes is that the executor skips the demuxer stage entirely.
struct OnePacketSource {
    streams: Vec<StreamInfo>,
    emitted: bool,
}

impl OnePacketSource {
    fn new() -> Self {
        let mut params = CodecParameters::audio(CodecId::new(SOURCE_VAR_CODEC));
        params.sample_rate = Some(8_000);
        params.channels = Some(1);
        params.sample_format = Some(SampleFormat::S16);
        let stream = StreamInfo {
            index: 0,
            time_base: TimeBase::new(1, 8_000),
            duration: None,
            start_time: Some(0),
            params,
        };
        Self {
            streams: vec![stream],
            emitted: false,
        }
    }
}

impl PacketSource for OnePacketSource {
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }
    fn next_packet(&mut self) -> Result<Packet> {
        if self.emitted {
            return Err(Error::Eof);
        }
        self.emitted = true;
        Ok(Packet {
            stream_index: 0,
            time_base: TimeBase::new(1, 8_000),
            pts: Some(0),
            dts: Some(0),
            duration: Some(8),
            flags: PacketFlags::default(),
            data: vec![0u8; 16],
        })
    }
}

fn open_packets(_uri: &str) -> Result<Box<dyn PacketSource>> {
    Ok(Box::new(OnePacketSource::new()))
}

// ───────────────────────── frame path: in-tree FrameSource ─────────────────────────

/// Emits exactly one decoded `AudioFrame` with the same content the
/// other two sources produce after their decode pass, then EOF.
/// Hooked straight into the filter chain — neither a demuxer nor a
/// decoder runs for this path.
struct OneFrameSource {
    params: CodecParameters,
    emitted: bool,
}

impl OneFrameSource {
    fn new() -> Self {
        let mut params = CodecParameters::audio(CodecId::new(SOURCE_VAR_CODEC));
        params.sample_rate = Some(8_000);
        params.channels = Some(1);
        params.sample_format = Some(SampleFormat::S16);
        Self {
            params,
            emitted: false,
        }
    }
}

impl FrameSource for OneFrameSource {
    fn params(&self) -> &CodecParameters {
        &self.params
    }
    fn next_frame(&mut self) -> Result<Frame> {
        if self.emitted {
            return Err(Error::Eof);
        }
        self.emitted = true;
        Ok(Frame::Audio(AudioFrame {
            samples: 8,
            pts: Some(0),
            data: vec![vec![0u8; 16]],
        }))
    }
}

fn open_frames(_uri: &str) -> Result<Box<dyn FrameSource>> {
    Ok(Box::new(OneFrameSource::new()))
}

// ───────────────────────── shared registration helper ─────────────────────────

fn register_codec_and_container(codecs: &mut CodecRegistry, containers: &mut ContainerRegistry) {
    let info = CodecInfo::new(CodecId::new(SOURCE_VAR_CODEC))
        .capabilities(CodecCapabilities::audio(SOURCE_VAR_CODEC).with_decode())
        .decoder(make_decoder as DecoderFactory);
    codecs.register(info);

    containers.register_demuxer(BYTES_CONTAINER, open_demuxer as OpenDemuxerFn);
    containers.register_extension(BYTES_EXT, BYTES_CONTAINER);
}

fn register_sources(sources: &mut SourceRegistry) {
    sources.register_bytes("svbytes", open_bytes);
    sources.register_packets("svpackets", open_packets);
    sources.register_frames("svframes", open_frames);
}

fn make_ctx() -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    register_codec_and_container(&mut ctx.codecs, &mut ctx.containers);
    register_sources(&mut ctx.sources);
    ctx
}

/// Drive a single-track audio job that pipes the named URI into a
/// playback-style `@display` sink. Returns the per-kind frame counts
/// observed by the sink.
fn run_via(uri: &str) -> SinkCounters {
    run_via_threads(uri, 1)
}

/// Same as [`run_via`] but with an explicit thread budget so callers can
/// pin the executor to either the serial path (`threads = 1`) or the
/// pipelined path (`threads ≥ 2`). Pipelined runs of typed-source jobs
/// historically panicked at `executor.rs` with `Option::unwrap()` on a
/// `None`, so the tests below cover both paths.
fn run_via_threads(uri: &str, threads: usize) -> SinkCounters {
    let ctx = make_ctx();
    // `@display` is the standard reserved sink that consumes raw
    // frames; we override it with a counting sink so we can assert on
    // the frame count without dragging a real player into the test.
    let job_json = format!(
        r#"{{
            "@display": {{
                "audio": [{{"from": "{uri}"}}]
            }}
        }}"#
    );
    let job = Job::from_json(&job_json).expect("parse job");
    let shared = Arc::new(Mutex::new(SinkCounters::default()));
    let sink = Box::new(CountingSink {
        shared: shared.clone(),
    });
    Executor::new(&job, &ctx)
        .with_threads(threads)
        .with_sink_override("@display", sink)
        .run()
        .expect("executor run");
    let s = shared.lock().unwrap();
    SinkCounters {
        audio_frames: s.audio_frames,
        video_frames: s.video_frames,
    }
}

// ───────────────────────── tests ─────────────────────────

#[test]
fn bytes_source_runs_full_demux_decode_chain() {
    // The byte-path goes through `open_demuxer` (registered above) which
    // emits exactly one packet; the passthrough decoder turns it into
    // exactly one frame. Sink should observe 1 audio frame.
    let c = run_via("svbytes://x/data.svwav");
    assert_eq!(
        c.audio_frames, 1,
        "bytes path: expected 1 audio frame from full demux→decode chain, got {}",
        c.audio_frames
    );
    assert_eq!(c.video_frames, 0);
}

#[test]
fn packet_source_runs_decode_only_chain() {
    // The packet-path skips demux: the source emits one Packet, the
    // executor wires it straight into the decoder, the decoder yields
    // one Frame. Same expected frame count as the bytes path.
    let c = run_via("svpackets://anything");
    assert_eq!(
        c.audio_frames, 1,
        "packet path: expected 1 audio frame from decode-only chain, got {}",
        c.audio_frames
    );
    assert_eq!(c.video_frames, 0);
}

#[test]
fn frame_source_routes_frames_directly_to_sink() {
    // The frame-path skips both demux + decode: the source emits one
    // Frame, the executor pushes it straight into the filter slot
    // (which is empty in this job, so the sink sees it directly).
    let c = run_via("svframes://anything");
    assert_eq!(
        c.audio_frames, 1,
        "frame path: expected 1 audio frame routed directly to sink, got {}",
        c.audio_frames
    );
    assert_eq!(c.video_frames, 0);
}

/// Regression: the pipelined runner used to *probe* the source, rewrite
/// `Demuxer { source }` → `FrameSource { source }` in a clone of the DAG,
/// then hand that *rewritten* clone to `run_output` as the fallback path
/// for typed-source jobs. `run_output`'s own `resolve_source_shapes`
/// pass only collected URIs from `Demuxer` nodes, so the second pass
/// found none, `sources_by_uri` came out empty, and the next
/// `sources_by_uri.get(&pl.source_uri).unwrap()` panicked with
/// `Option::unwrap() on a None` (executor.rs:269).
///
/// Triggered by `oxideav convert "xc:red" out.png` and any other CLI
/// path that fed a `generate://` URI into the convert verb — every
/// generator source crashed before producing a single frame.
///
/// The fix passes the *original* DAG (with its `Demuxer` leaves) to the
/// fallback so `run_output` can re-discover and re-open the URIs itself.
#[test]
fn frame_source_pipelined_path_does_not_panic() {
    // `with_threads(2)` selects `run_output_pipelined`, which in turn
    // falls back to `run_output` because the source resolves to
    // `SourceOutput::Frames` (the bytes-only pipelined runner can't
    // drive frame sources). Before the fix this fell through to the
    // .unwrap() panic; after the fix the frame is delivered normally.
    let c = run_via_threads("svframes://anything", 2);
    assert_eq!(
        c.audio_frames, 1,
        "frame path (pipelined): expected 1 audio frame, got {}",
        c.audio_frames
    );
    assert_eq!(c.video_frames, 0);
}

#[test]
fn packet_source_pipelined_path_does_not_panic() {
    // Same fallback path as the FrameSource case: pipelined → typed
    // source → fallback to serial run_output. PacketSource is also
    // typed, so it also tripped the empty-`sources_by_uri` panic.
    let c = run_via_threads("svpackets://anything", 2);
    assert_eq!(
        c.audio_frames, 1,
        "packet path (pipelined): expected 1 audio frame, got {}",
        c.audio_frames
    );
    assert_eq!(c.video_frames, 0);
}
