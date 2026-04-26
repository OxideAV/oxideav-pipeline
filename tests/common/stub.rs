//! Synthetic audio stub for pipeline integration tests.
//!
//! oxideav-pipeline is a framework crate; its tests should not pull in
//! a real codec or container just to exercise pipeline plumbing
//! (back-pressure, abort cascade, seek barriers, multi-port routing).
//! This module provides a tiny in-tree stub that lets a `Job` resolve
//! against a path on disk without going through a real demuxer:
//!
//! * `register(&mut codecs, &mut containers)` wires up a stub
//!   container (extension `.stub`) and a stub decoder for codec id
//!   `stub_pcm`.
//! * `touch(name) -> PathBuf` creates an empty file under `temp_dir()`
//!   ending in `.stub`. The stub demuxer ignores file contents.
//! * The demuxer emits N packets of zero-filled S16 mono audio with
//!   pts in `(1, sample_rate)` ticks, supports `seek_to`.
//! * The decoder is a 1:1 packet → `AudioFrame` passthrough.

use oxideav_core::{
    packet::PacketFlags, AudioFrame, CodecCapabilities, CodecId, CodecParameters, CodecResolver,
    Error, Frame, Packet, Result, SampleFormat, StreamInfo, TimeBase,
};
use oxideav_core::{registry::CodecInfo, CodecRegistry, Decoder, DecoderFactory};
use oxideav_core::{ContainerRegistry, Demuxer, OpenDemuxerFn, ReadSeek};

/// Codec id for the stub passthrough decoder.
pub const CODEC_ID: &str = "stub_pcm";
/// Container name registered into [`ContainerRegistry`].
pub const CONTAINER: &str = "stub_audio";
/// Extension that triggers the stub container via the registry's
/// extension hint.
pub const EXT: &str = "stub";

/// Sample rate of the synthetic audio. Small so the demuxer's pts
/// arithmetic is easy to reason about in tests, realistic enough that
/// any rate-aware code path under test works as in production.
pub const SAMPLE_RATE: u32 = 8_000;
/// Mono.
pub const CHANNELS: u16 = 1;
/// Each emitted packet covers 100 ms of audio.
pub const SAMPLES_PER_PACKET: u32 = SAMPLE_RATE / 10;
/// Default total stream length in milliseconds.
pub const DEFAULT_DURATION_MS: u64 = 60_000;

/// Register the stub codec + container into a fresh registry pair.
pub fn register(codecs: &mut CodecRegistry, containers: &mut ContainerRegistry) {
    let info = CodecInfo::new(CodecId::new(CODEC_ID))
        .capabilities(CodecCapabilities::audio(CODEC_ID).with_decode())
        .decoder(make_decoder as DecoderFactory);
    codecs.register(info);

    containers.register_demuxer(CONTAINER, open_demuxer as OpenDemuxerFn);
    containers.register_extension(EXT, CONTAINER);
}

/// Create an empty `.stub` file under `std::env::temp_dir()` and
/// return its absolute path. The stub demuxer ignores the file's
/// contents — only the path itself is used by the source registry to
/// produce a `Box<dyn ReadSeek>` for the demuxer's open factory.
pub fn touch(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("oxideav_pipeline_stub_{name}.{EXT}"));
    let _ = std::fs::File::create(&p).expect("create stub file");
    p
}

fn make_decoder(_params: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(StubDecoder { pending: None }))
}

fn open_demuxer(
    _input: Box<dyn ReadSeek>,
    _codecs: &dyn CodecResolver,
) -> Result<Box<dyn Demuxer>> {
    Ok(Box::new(StubDemuxer::new(DEFAULT_DURATION_MS)))
}

/// Stub demuxer: emits zero-filled S16 mono packets at fixed cadence.
pub struct StubDemuxer {
    streams: Vec<StreamInfo>,
    sample_rate: u32,
    samples_per_packet: u32,
    next_pts: i64,
    samples_remaining: u64,
    samples_total: u64,
}

impl StubDemuxer {
    pub fn new(duration_ms: u64) -> Self {
        let mut params = CodecParameters::audio(CodecId::new(CODEC_ID));
        params.sample_rate = Some(SAMPLE_RATE);
        params.channels = Some(CHANNELS);
        params.sample_format = Some(SampleFormat::S16);
        let total = SAMPLE_RATE as u64 * duration_ms / 1000;
        let stream = StreamInfo {
            index: 0,
            time_base: TimeBase::new(1, SAMPLE_RATE as i64),
            duration: Some(total as i64),
            start_time: Some(0),
            params,
        };
        Self {
            streams: vec![stream],
            sample_rate: SAMPLE_RATE,
            samples_per_packet: SAMPLES_PER_PACKET,
            next_pts: 0,
            samples_remaining: total,
            samples_total: total,
        }
    }
}

impl Demuxer for StubDemuxer {
    fn format_name(&self) -> &str {
        CONTAINER
    }
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }
    fn next_packet(&mut self) -> Result<Packet> {
        if self.samples_remaining == 0 {
            return Err(Error::Eof);
        }
        let n = self.samples_per_packet.min(self.samples_remaining as u32);
        let pts = self.next_pts;
        self.next_pts += i64::from(n);
        self.samples_remaining -= u64::from(n);
        Ok(Packet {
            stream_index: 0,
            time_base: TimeBase::new(1, self.sample_rate as i64),
            pts: Some(pts),
            dts: Some(pts),
            duration: Some(i64::from(n)),
            flags: PacketFlags::default(),
            data: vec![0u8; (n as usize) * 2], // S16, mono
        })
    }
    fn seek_to(&mut self, _stream_index: u32, pts: i64) -> Result<i64> {
        let landed = pts.clamp(0, self.samples_total as i64);
        self.next_pts = landed;
        self.samples_remaining = self.samples_total - landed as u64;
        Ok(landed)
    }
}

/// Stub decoder: 1:1 packet → AudioFrame passthrough.
pub struct StubDecoder {
    pending: Option<Packet>,
}

impl Decoder for StubDecoder {
    fn codec_id(&self) -> &CodecId {
        // OnceLock so the &CodecId reference outlives every `Decoder`
        // call. CodecId is cheaply clonable but the trait wants a
        // borrow.
        static ID: std::sync::OnceLock<CodecId> = std::sync::OnceLock::new();
        ID.get_or_init(|| CodecId::new(CODEC_ID))
    }
    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        self.pending = Some(packet.clone());
        Ok(())
    }
    fn receive_frame(&mut self) -> Result<Frame> {
        match self.pending.take() {
            Some(p) => {
                let samples = (p.data.len() / 2) as u32;
                // Stream-level properties (format, channels, sample_rate,
                // time_base) live on the stream's CodecParameters, not
                // per-frame. See StubDemuxer::open for where they get set.
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
