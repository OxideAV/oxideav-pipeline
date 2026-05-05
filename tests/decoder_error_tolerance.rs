//! Regression test for the user-reported H.264-MP4 hang on
//! `congress_mtgox_coins.mp4` (2026-05-04).
//!
//! Symptom: oxideplay sat at 00:00 and never advanced. Bisect path:
//!
//! 1. oxideav-mp4 demux: OK (200 packets emerge in standalone tests).
//! 2. oxideav-h264 decoder: OK (188 frames out of 200 packets).
//! 3. oxideav-aac decoder: OK in isolation (18 frames out of 20
//!    packets — three packets returned `Error::invalid("bitreader: out
//!    of bits")` from `receive_frame`, but the decoder self-recovered
//!    on the next `send_packet`).
//! 4. oxideav-pipeline executor: BUG. `run_decode_stage` (and the
//!    inherent decode path in `executor.rs`) propagated *any*
//!    non-NeedMore / non-Eof decoder error as a fatal `return Err(e)`,
//!    killing the entire stream after the first bad packet. The audio
//!    clock then never advanced, so oxideplay's position calc stayed
//!    pinned at 00:00.00 — the user-visible "stays at 00:00" bug.
//!
//! The fix (in this commit) makes per-packet decoder errors
//! non-fatal — same model the H.264 decoder already used internally
//! for per-slice errors (`eprintln!("h264 slice skipped: {e}")`). This
//! test pins that contract: a decoder that errors on every Nth packet
//! must still deliver every other packet's frame to the sink.

use std::sync::{Arc, Mutex};

use oxideav_core::registry::CodecInfo;
use oxideav_core::{
    packet::PacketFlags, AudioFrame, BytesSource, CodecCapabilities, CodecId, CodecParameters,
    CodecResolver, Decoder, DecoderFactory, Demuxer, Error, Frame, MediaType, OpenDemuxerFn,
    Packet, ReadSeek, Result, RuntimeContext, SampleFormat, StreamInfo, TimeBase,
};
use oxideav_pipeline::{Executor, Job, JobSink};

const FLAKY_CODEC: &str = "decoder_error_tolerance_pcm";
const FLAKY_CONTAINER: &str = "decoder_error_tolerance_audio";
const FLAKY_SCHEME: &str = "detol";
const SAMPLE_RATE: u32 = 8_000;
const SAMPLES_PER_PACKET: u32 = 800; // 100 ms
const TOTAL_PACKETS: u32 = 50;

fn make_decoder(_params: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(FlakyDecoder {
        pending: None,
        packets_seen: 0,
    }))
}

fn open_demuxer(
    _input: Box<dyn ReadSeek>,
    _codecs: &dyn CodecResolver,
) -> Result<Box<dyn Demuxer>> {
    Ok(Box::new(FlakyDemuxer::new()))
}

fn open_bytes(_uri: &str) -> Result<Box<dyn BytesSource>> {
    // Hand the demuxer a non-empty buffer so `BufferedSource::new`
    // doesn't reject zero-length input. The demuxer ignores the
    // content (it's a synthetic stream).
    Ok(Box::new(std::io::Cursor::new(vec![0u8; 64])))
}

fn make_ctx() -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    let info = CodecInfo::new(CodecId::new(FLAKY_CODEC))
        .capabilities(CodecCapabilities::audio(FLAKY_CODEC).with_decode())
        .decoder(make_decoder as DecoderFactory);
    ctx.codecs.register(info);
    ctx.containers
        .register_demuxer(FLAKY_CONTAINER, open_demuxer as OpenDemuxerFn);
    // The synthetic source uses a custom URI scheme so we don't have
    // to register an extension hint. The bytes-shape source registers
    // under the same scheme name; the demuxer is then resolved by
    // the executor via `probe_input` (which falls back to the
    // extension-less path because our scheme has no `.<ext>` tail).
    ctx.sources.register_bytes(FLAKY_SCHEME, open_bytes);
    // The probe path needs *something* to map the bytes to a
    // container — register the extension hint so the URI
    // `detol://x/data.detol` lands on FLAKY_CONTAINER.
    ctx.containers
        .register_extension(FLAKY_SCHEME, FLAKY_CONTAINER);
    ctx
}

struct FlakyDemuxer {
    streams: Vec<StreamInfo>,
    next_pts: i64,
    remaining: u32,
}

impl FlakyDemuxer {
    fn new() -> Self {
        let mut params = CodecParameters::audio(CodecId::new(FLAKY_CODEC));
        params.sample_rate = Some(SAMPLE_RATE);
        params.channels = Some(1);
        params.sample_format = Some(SampleFormat::S16);
        let total_samples = SAMPLE_RATE as i64 * (TOTAL_PACKETS as i64) / 10;
        Self {
            streams: vec![StreamInfo {
                index: 0,
                time_base: TimeBase::new(1, SAMPLE_RATE as i64),
                duration: Some(total_samples),
                start_time: Some(0),
                params,
            }],
            next_pts: 0,
            remaining: TOTAL_PACKETS,
        }
    }
}

impl Demuxer for FlakyDemuxer {
    fn format_name(&self) -> &str {
        FLAKY_CONTAINER
    }
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }
    fn next_packet(&mut self) -> Result<Packet> {
        if self.remaining == 0 {
            return Err(Error::Eof);
        }
        let pts = self.next_pts;
        self.next_pts += SAMPLES_PER_PACKET as i64;
        self.remaining -= 1;
        Ok(Packet {
            stream_index: 0,
            time_base: TimeBase::new(1, SAMPLE_RATE as i64),
            pts: Some(pts),
            dts: Some(pts),
            duration: Some(SAMPLES_PER_PACKET as i64),
            flags: PacketFlags::default(),
            data: vec![0u8; (SAMPLES_PER_PACKET as usize) * 2],
        })
    }
    fn seek_to(&mut self, _stream_index: u32, pts: i64) -> Result<i64> {
        Ok(pts.max(0))
    }
}

/// Decoder that deliberately errors on every 5th `receive_frame` call,
/// returning `Error::invalid("simulated bit-stream glitch")`. The
/// 1-of-5 frequency mirrors what we see on real-world AAC streams
/// where some frames have a subtle bit-stream bug that surfaces as
/// "out of bits" inside the spectral fill loop.
///
/// The decoder MUST still produce the next frame after such an error —
/// matching the AAC decoder's recovery behaviour where `pending.take()`
/// already cleared the offending packet so the next `send_packet`
/// arrives on a clean slate.
struct FlakyDecoder {
    pending: Option<Packet>,
    packets_seen: u32,
}

impl Decoder for FlakyDecoder {
    fn codec_id(&self) -> &CodecId {
        static ID: std::sync::OnceLock<CodecId> = std::sync::OnceLock::new();
        ID.get_or_init(|| CodecId::new(FLAKY_CODEC))
    }
    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        self.packets_seen += 1;
        self.pending = Some(packet.clone());
        Ok(())
    }
    fn receive_frame(&mut self) -> Result<Frame> {
        match self.pending.take() {
            Some(p) => {
                // Every 5th packet errors. `take()` already cleared
                // `pending`, so a *next* `send_packet` will land on a
                // clean slate — same recovery shape as oxideav-aac's
                // decoder uses for transient bit-stream glitches.
                if self.packets_seen % 5 == 0 {
                    return Err(Error::invalid("simulated bit-stream glitch"));
                }
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

#[derive(Default)]
struct Counters {
    frames: usize,
}

struct CountingSink(Arc<Mutex<Counters>>);

impl JobSink for CountingSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        self.0.lock().unwrap().frames += 1;
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

fn run_with_threads(threads: usize) -> usize {
    let ctx = make_ctx();
    let job_json = format!(
        r#"{{"@display":{{"audio":[{{"from":"{FLAKY_SCHEME}://x/data.{FLAKY_SCHEME}"}}]}}}}"#
    );
    let job = Job::from_json(&job_json).expect("parse job");

    let counters = Arc::new(Mutex::new(Counters::default()));
    let sink = Box::new(CountingSink(counters.clone()));

    Executor::new(&job, &ctx)
        .with_threads(threads)
        .with_sink_override("@display", sink)
        .run()
        .expect("executor must finish even with per-packet decoder errors");

    let frames = counters.lock().unwrap().frames;
    frames
}

#[test]
fn flaky_decoder_does_not_kill_serial_stream() {
    // Pre-fix: serial path's `pump_packet` (executor.rs) returned the
    // first non-NeedMore / non-Eof receive_frame error as a fatal
    // executor error. Stream stopped after ~4 frames.
    //
    // Post-fix: per-packet errors are logged (eprintln) and the
    // executor continues. With a decoder that errors on every 5th
    // packet (5,10,15,...,50), we should see ~40 of 50 frames.
    let frames = run_with_threads(1);
    assert!(
        frames >= 35,
        "serial path: expected at least 35 frames out of 50 packets despite per-packet errors; got {frames}"
    );
    assert!(
        frames > 10,
        "regression: serial executor bailed early on first decoder error; only got {frames} frames"
    );
}

#[test]
fn flaky_decoder_does_not_kill_pipelined_stream() {
    // Same contract for the pipelined path (`staged.rs::run_decode_stage`).
    // This is the path oxideplay uses by default.
    let frames = run_with_threads(2);
    assert!(
        frames >= 35,
        "pipelined path: expected at least 35 frames out of 50 packets despite per-packet errors; got {frames}"
    );
    assert!(
        frames > 10,
        "regression: pipelined executor bailed early on first decoder error; only got {frames} frames"
    );
}
