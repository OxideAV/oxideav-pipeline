//! Contract test for [`Progress::packets_skipped`].
//!
//! The pipelined runner already swallows a recoverable per-packet decoder
//! error (logged via `eprintln!`) so the stream doesn't wedge on a single
//! bad packet — see `tests/decoder_error_tolerance.rs`. Round-184 makes
//! that previously-invisible event observable: a counter shared between
//! `PipelineCounters::packets_skipped` and `Progress::packets_skipped`
//! lets an engine surface the same information in its status bar without
//! scraping stderr, and lets a stress harness assert on the tolerance
//! contract directly off the progress stream rather than reverse-engineering
//! it from `frames_written` counts.
//!
//! Asserts:
//!
//! 1. **`packets_skipped` is `0` on a clean stream.** A non-erroring
//!    decoder (every packet yields a frame) must report zero skips
//!    end-to-end. The accountant has no off-by-one — it only fires
//!    when the decode path actually swallowed a packet.
//!
//! 2. **`packets_skipped` is monotonically non-decreasing across
//!    consecutive emissions.** Mirrors the `frames` field's contract.
//!    Once a packet is logged-and-skipped, the counter never goes back
//!    down — engines that compute "skip rate per second" can subtract
//!    consecutive samples.
//!
//! 3. **`packets_skipped` on the EOF event equals the final stats'
//!    `packets_skipped`.** The mux loop reads from the shared atomic on
//!    each emission; the EOF emission reads it *after* the workers have
//!    joined, so it must be the final snapshot value returned by
//!    `Executor::stop()`. Any drift would mean the two paths are reading
//!    different counters.

use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_core::registry::CodecInfo;
use oxideav_core::{
    packet::PacketFlags, AudioFrame, BytesSource, CodecCapabilities, CodecId, CodecParameters,
    CodecResolver, Decoder, DecoderFactory, Demuxer, Error, Frame, MediaType, OpenDemuxerFn,
    Packet, ReadSeek, Result, RuntimeContext, SampleFormat, StreamInfo, TimeBase,
};
use oxideav_pipeline::staged::Progress;
use oxideav_pipeline::{Executor, Job, JobSink};

const FLAKY_CODEC: &str = "progress_skipped_pcm";
const FLAKY_CONTAINER: &str = "progress_skipped_audio";
const FLAKY_SCHEME: &str = "progskip";
const SAMPLE_RATE: u32 = 8_000;
const SAMPLES_PER_PACKET: u32 = 800;
const TOTAL_PACKETS: u32 = 50;

fn make_decoder_flaky(_params: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(FlakyDecoder {
        pending: None,
        packets_seen: 0,
        error_every: 5,
    }))
}

fn make_decoder_clean(_params: &CodecParameters) -> Result<Box<dyn Decoder>> {
    // `error_every: 0` disables the error injector entirely — every
    // packet yields a frame. Used by the "clean stream" assertion below.
    Ok(Box::new(FlakyDecoder {
        pending: None,
        packets_seen: 0,
        error_every: 0,
    }))
}

fn open_demuxer(
    _input: Box<dyn ReadSeek>,
    _codecs: &dyn CodecResolver,
) -> Result<Box<dyn Demuxer>> {
    Ok(Box::new(SimpleDemuxer::new()))
}

fn open_bytes(_uri: &str) -> Result<Box<dyn BytesSource>> {
    Ok(Box::new(std::io::Cursor::new(vec![0u8; 64])))
}

fn make_ctx(flaky: bool) -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    let decoder_factory: DecoderFactory = if flaky {
        make_decoder_flaky
    } else {
        make_decoder_clean
    };
    let info = CodecInfo::new(CodecId::new(FLAKY_CODEC))
        .capabilities(CodecCapabilities::audio(FLAKY_CODEC).with_decode())
        .decoder(decoder_factory);
    ctx.codecs.register(info);
    ctx.containers
        .register_demuxer(FLAKY_CONTAINER, open_demuxer as OpenDemuxerFn);
    ctx.sources.register_bytes(FLAKY_SCHEME, open_bytes);
    ctx.containers
        .register_extension(FLAKY_SCHEME, FLAKY_CONTAINER);
    ctx
}

struct SimpleDemuxer {
    streams: Vec<StreamInfo>,
    next_pts: i64,
    remaining: u32,
}

impl SimpleDemuxer {
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

impl Demuxer for SimpleDemuxer {
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

/// Decoder that errors on every `error_every`-th `receive_frame`. Set
/// `error_every: 0` to disable the injector (clean stream).
struct FlakyDecoder {
    pending: Option<Packet>,
    packets_seen: u32,
    error_every: u32,
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
                if self.error_every > 0 && self.packets_seen % self.error_every == 0 {
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

struct DrainSink {
    tx: SyncSender<()>,
}

impl JobSink for DrainSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        let _ = self.tx.try_send(());
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        let _ = self.tx.try_send(());
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Spawn an executor against the synthetic source + (clean or flaky)
/// decoder. Returns the handle + a drain receiver the harness uses to
/// keep the sink-write channel from filling up.
fn spawn_simple(flaky: bool) -> (oxideav_pipeline::ExecutorHandle, Receiver<()>) {
    let ctx = make_ctx(flaky);
    let job_json = format!(
        r#"{{"@display":{{"audio":[{{"from":"{FLAKY_SCHEME}://x/data.{FLAKY_SCHEME}"}}]}}}}"#
    );
    let job = Box::leak(Box::new(Job::from_json(&job_json).expect("parse job")));
    let ctx = Box::leak(Box::new(ctx));
    let (write_tx, write_rx) = std::sync::mpsc::sync_channel::<()>(1024);
    let sink = Box::new(DrainSink { tx: write_tx });
    let handle = Executor::new(job, ctx)
        .with_sink_override("@display", sink)
        .with_threads(2)
        .spawn()
        .expect("spawn executor");
    (handle, write_rx)
}

/// Same shape as `progress_reports_elapsed_micros::collect_all_progress`
/// — poll `try_progress` in a tight loop, drain the sink channel in a
/// background thread, return every Progress emission once the worker has
/// finished. Returns the events + the stats from the joined executor.
fn run_and_collect(flaky: bool) -> (Vec<Progress>, oxideav_pipeline::executor::ExecutorStats) {
    let (handle, write_rx) = spawn_simple(flaky);
    let _drain =
        std::thread::spawn(
            move || {
                while write_rx.recv_timeout(Duration::from_secs(30)).is_ok() {}
            },
        );
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        while let Some(p) = handle.try_progress() {
            events.push(p);
        }
        if handle.has_finished() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    std::thread::sleep(Duration::from_millis(10));
    while let Some(p) = handle.try_progress() {
        events.push(p);
    }
    let stats = handle.stop().expect("executor must finish");
    (events, stats)
}

#[test]
fn packets_skipped_is_zero_on_clean_stream() {
    let (events, stats) = run_and_collect(false);
    assert!(
        events.iter().any(|p| p.eof),
        "never observed an eof:true progress event within 30s — pipeline stalled"
    );
    assert_eq!(
        stats.packets_skipped, 0,
        "clean stream: stats.packets_skipped must be 0; got {}",
        stats.packets_skipped
    );
    for p in &events {
        assert_eq!(
            p.packets_skipped, 0,
            "clean stream: every Progress emission must report 0 packets_skipped; \
             got {} at frames={} eof={}",
            p.packets_skipped, p.frames, p.eof
        );
    }
}

#[test]
fn packets_skipped_is_non_decreasing_on_flaky_stream() {
    let (events, _stats) = run_and_collect(true);
    assert!(
        events.iter().any(|p| p.eof),
        "never observed an eof:true progress event within 30s — pipeline stalled"
    );
    // Walk consecutive emissions; the shared atomic counter only ever
    // grows so the snapshots must form a non-decreasing sequence. A
    // regression here would indicate the mux loop is reading a stale
    // value or the counter is being reset mid-run.
    let arc: Arc<Mutex<Vec<Progress>>> = Arc::new(Mutex::new(events.clone()));
    let guard = arc.lock().unwrap();
    for w in guard.windows(2) {
        assert!(
            w[1].packets_skipped >= w[0].packets_skipped,
            "packets_skipped must be non-decreasing across emissions; \
             observed {} then {} ({} regression)",
            w[0].packets_skipped,
            w[1].packets_skipped,
            w[0].packets_skipped - w[1].packets_skipped
        );
    }
}

#[test]
fn packets_skipped_eof_matches_final_stats() {
    let (events, stats) = run_and_collect(true);
    let eof = events
        .iter()
        .find(|p| p.eof)
        .expect("never observed an eof:true progress event");
    assert_eq!(
        eof.packets_skipped, stats.packets_skipped,
        "the EOF progress emission's packets_skipped ({}) must match the final \
         ExecutorStats::packets_skipped ({}) — both read from the same atomic \
         and the EOF emission is sent after workers join, so any divergence \
         would mean the two paths are wired to different counters",
        eof.packets_skipped, stats.packets_skipped
    );
    // Sanity: FlakyDecoder errors on every 5th of 50 packets, so the
    // final tally must be exactly 10. This pins the wiring of the
    // counter increment in both decode paths against accidental
    // double-counting (e.g. if a future refactor also incremented at the
    // send_packet branch when receive_frame errored).
    assert_eq!(
        stats.packets_skipped, 10,
        "expected exactly 10 skips (every 5th of 50 packets); got {}",
        stats.packets_skipped
    );
}
