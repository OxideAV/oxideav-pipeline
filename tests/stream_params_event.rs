//! The in-band `Frame::StreamParams` event, end to end.
//!
//! A decoder whose output shape changes mid-stream says so two ways
//! that always agree — `output_params_generation()` moves and, for a
//! decoder that emits it, `receive_frame` returns one
//! `Frame::StreamParams` right before the first frame of the new shape.
//! Every decoder below changes shape at packet `SWITCH_AT`, once as an
//! emitting decoder (event + generation) and once as a non-emitting one
//! (generation only); the pipeline must hand exactly one event down the
//! track either way, at the right position:
//!
//! * a raw-frame sink hears of it through `JobSink::stream_params` /
//!   `Sink::stream_params`, never through `write_frame`;
//! * a pass-through filter forwards it (or has it forwarded for it);
//! * an auto-inserted pixel-format conversion re-plans its source side
//!   and restates the event for its output;
//! * an auto-inserted resampler re-plans and keeps its output constant,
//!   so the encoder behind it never sees the change;
//! * an encoder that re-configures receives it once and the sink is
//!   told the encoder's new output; one that cannot follow fails the
//!   run with `Error::Unsupported` naming both shapes.

use std::sync::{Arc, Mutex, MutexGuard};

use oxideav_core::registry::CodecInfo;
use oxideav_core::{
    AudioFrame, BytesSource, CodecCapabilities, CodecId, CodecParameters, CodecRegistry,
    CodecResolver, Decoder, DecoderFactory, Demuxer, Encoder, EncoderFactory, Error, FilterContext,
    Frame, MediaType, Muxer, OpenDemuxerFn, Packet, PixelFormat, PortParams, PortSpec, ReadSeek,
    Result, RuntimeContext, SampleFormat, StreamFilter, StreamInfo, TimeBase, VideoFrame,
    VideoPlane, packet::PacketFlags,
};
use oxideav_pipeline::{
    Executor, FailureStage, Job, JobSink, Pipeline, Sink, SinkAcceptance, StreamPlan,
    transcode_simple, transcode_simple_in,
};

const VCODEC: &str = "ev_video";
const ACODEC: &str = "ev_audio";
/// Takes Yuv444P only (so a conversion is inserted) and re-configures
/// for a new size.
const ENC_FOLLOW: &str = "ev_enc_follow";
/// Takes Yuv444P only and refuses any change.
const ENC_FIXED: &str = "ev_enc_fixed";
/// Audio encoder taking 48 kHz s16 only; refuses any change.
const AENC: &str = "ev_aenc";
const CONTAINER: &str = "ev_container";
const SCHEME: &str = "evx";
const PACKETS: u32 = 12;
/// Index of the first packet decoded in the new shape.
const SWITCH_AT: u32 = 5;

/// One thing a consumer saw, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Seen {
    /// A media frame: total plane bytes (video) or samples (audio).
    Frame(usize),
    /// A stream-parameters event / hook call: (width, height) or
    /// (rate, channels).
    Params(u32, u32),
    /// An encoded packet.
    Packet,
}

#[derive(Default)]
struct Log {
    /// The decoder emits the in-band event (else: generation only).
    emits: bool,
    /// The video filter forwards the event itself (else: swallows it).
    filter_forwards: bool,
    decoder: Vec<Seen>,
    filter: Vec<Seen>,
    encoder: Vec<Seen>,
    sink: Vec<Seen>,
}

static LOG: Mutex<Option<Arc<Mutex<Log>>>> = Mutex::new(None);
static SERIAL: Mutex<()> = Mutex::new(());

fn log() -> Arc<Mutex<Log>> {
    LOG.lock().unwrap().clone().expect("test installs LOG")
}

fn begin(emits: bool) -> (MutexGuard<'static, ()>, Arc<Mutex<Log>>) {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let l = Arc::new(Mutex::new(Log {
        emits,
        filter_forwards: true,
        ..Log::default()
    }));
    *LOG.lock().unwrap() = Some(l.clone());
    (guard, l)
}

fn push(l: &Arc<Mutex<Log>>, f: impl FnOnce(&mut Log) -> &mut Vec<Seen>, s: Seen) {
    let mut g = l.lock().unwrap();
    f(&mut g).push(s);
}

fn describe(p: &CodecParameters) -> Seen {
    match p.media_type {
        MediaType::Audio => {
            Seen::Params(p.sample_rate.unwrap_or(0), p.channels.unwrap_or(0) as u32)
        }
        _ => Seen::Params(p.width.unwrap_or(0), p.height.unwrap_or(0)),
    }
}

fn frame_size(f: &Frame) -> Seen {
    match f {
        Frame::Video(v) => Seen::Frame(v.planes.iter().map(|p| p.data.len()).sum()),
        Frame::Audio(a) => Seen::Frame(a.samples as usize),
        Frame::StreamParams(p) => describe(p),
        _ => Seen::Frame(0),
    }
}

/// `n` media frames of `size`, an event, then `m` frames of `size2`.
fn switch_log(size: usize, ev: Seen, size2: usize) -> Vec<Seen> {
    let mut v = vec![Seen::Frame(size); SWITCH_AT as usize];
    v.push(ev);
    v.extend(vec![Seen::Frame(size2); (PACKETS - SWITCH_AT) as usize]);
    v
}

fn only_frames(v: &[Seen]) -> Vec<Seen> {
    v.iter()
        .filter(|s| matches!(s, Seen::Frame(_) | Seen::Params(..)))
        .cloned()
        .collect()
}

// ───────────────────────── source ─────────────────────────

fn video_params() -> CodecParameters {
    let mut p = CodecParameters::video(CodecId::new(VCODEC));
    p.width = Some(4);
    p.height = Some(4);
    p.pixel_format = Some(PixelFormat::Gray8);
    p
}

fn audio_params(rate: u32) -> CodecParameters {
    let mut p = CodecParameters::audio(CodecId::new(ACODEC));
    p.sample_rate = Some(rate);
    p.channels = Some(2);
    p.sample_format = Some(SampleFormat::S16);
    p
}

struct Demux {
    streams: Vec<StreamInfo>,
    next: u32,
}

impl Demux {
    fn new(audio: bool) -> Self {
        Self {
            streams: vec![StreamInfo {
                index: 0,
                time_base: TimeBase::new(1, 25),
                duration: Some(PACKETS as i64),
                start_time: Some(0),
                params: if audio {
                    audio_params(44_100)
                } else {
                    video_params()
                },
            }],
            next: 0,
        }
    }
}

impl Demuxer for Demux {
    fn format_name(&self) -> &str {
        CONTAINER
    }
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }
    fn next_packet(&mut self) -> Result<Packet> {
        if self.next == PACKETS {
            return Err(Error::Eof);
        }
        let pts = self.next as i64;
        self.next += 1;
        Ok(Packet {
            stream_index: 0,
            time_base: TimeBase::new(1, 25),
            pts: Some(pts),
            dts: Some(pts),
            duration: Some(1),
            flags: PacketFlags::default(),
            data: vec![pts as u8],
        })
    }
}

fn open_bytes(_uri: &str) -> Result<Box<dyn BytesSource>> {
    Ok(Box::new(std::io::Cursor::new(vec![0u8; 16])))
}

fn open_video(_i: Box<dyn ReadSeek>, _c: &dyn CodecResolver) -> Result<Box<dyn Demuxer>> {
    Ok(Box::new(Demux::new(false)))
}

fn open_audio(_i: Box<dyn ReadSeek>, _c: &dyn CodecResolver) -> Result<Box<dyn Demuxer>> {
    Ok(Box::new(Demux::new(true)))
}

// ───────────────────────── decoder ─────────────────────────

/// Video: Gray8 4×4, then Gray8 8×8 from packet `SWITCH_AT`. Audio:
/// s16 stereo 44.1 kHz (441 samples per packet), then 32 kHz (320).
/// Bumps the generation with the first frame of the new shape; an
/// emitting decoder returns the event right before that frame (and
/// keeps the packet for the next call).
struct EvDecoder {
    audio: bool,
    emits: bool,
    pending: Option<Packet>,
    switched: bool,
    log: Arc<Mutex<Log>>,
}

fn make_decoder(p: &CodecParameters) -> Result<Box<dyn Decoder>> {
    let log = log();
    let emits = log.lock().unwrap().emits;
    Ok(Box::new(EvDecoder {
        audio: p.media_type == MediaType::Audio,
        emits,
        pending: None,
        switched: false,
        log,
    }))
}

impl EvDecoder {
    fn current(&self) -> CodecParameters {
        match (self.audio, self.switched) {
            (true, false) => audio_params(44_100),
            (true, true) => audio_params(32_000),
            (false, false) => video_params(),
            (false, true) => {
                let mut p = video_params();
                p.width = Some(8);
                p.height = Some(8);
                p
            }
        }
    }
}

impl Decoder for EvDecoder {
    fn codec_id(&self) -> &CodecId {
        static V: std::sync::OnceLock<CodecId> = std::sync::OnceLock::new();
        static A: std::sync::OnceLock<CodecId> = std::sync::OnceLock::new();
        if self.audio {
            A.get_or_init(|| CodecId::new(ACODEC))
        } else {
            V.get_or_init(|| CodecId::new(VCODEC))
        }
    }
    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        self.pending = Some(packet.clone());
        Ok(())
    }
    fn receive_frame(&mut self) -> Result<Frame> {
        let n = self.pending.as_ref().ok_or(Error::NeedMore)?.data[0] as u32;
        if n >= SWITCH_AT && !self.switched {
            self.switched = true;
            if self.emits {
                let ev = Frame::StreamParams(Box::new(self.current()));
                push(&self.log, |l| &mut l.decoder, frame_size(&ev));
                return Ok(ev);
            }
        }
        let p = self.pending.take().expect("checked");
        let frame = if self.audio {
            let samples = if self.switched { 320 } else { 441 };
            Frame::Audio(AudioFrame {
                samples,
                pts: p.pts,
                data: vec![vec![0; samples as usize * 4]],
            })
        } else {
            let side = if self.switched { 8 } else { 4 };
            Frame::Video(VideoFrame {
                pts: p.pts,
                planes: vec![VideoPlane {
                    stride: side,
                    data: vec![100; side * side],
                }],
            })
        };
        push(&self.log, |l| &mut l.decoder, frame_size(&frame));
        Ok(frame)
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn output_params(&self) -> Option<CodecParameters> {
        Some(self.current())
    }
    fn output_params_generation(&self) -> u64 {
        u64::from(self.switched)
    }
}

// ───────────────────────── encoders ─────────────────────────

struct EvEncoder {
    params: CodecParameters,
    follows: bool,
    queued: usize,
    log: Arc<Mutex<Log>>,
}

fn make_encoder(p: &CodecParameters) -> Result<Box<dyn Encoder>> {
    let follows = p.codec_id.as_str() == ENC_FOLLOW;
    Ok(Box::new(EvEncoder {
        params: p.clone(),
        follows,
        queued: 0,
        log: log(),
    }))
}

impl Encoder for EvEncoder {
    fn codec_id(&self) -> &CodecId {
        &self.params.codec_id
    }
    fn output_params(&self) -> &CodecParameters {
        &self.params
    }
    fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        if let Some(p) = frame.stream_params() {
            push(&self.log, |l| &mut l.encoder, describe(p));
            if !self.follows {
                return Err(Error::unsupported("ev encoder: fixed input shape"));
            }
            self.params.width = p.width;
            self.params.height = p.height;
            return Ok(());
        }
        push(&self.log, |l| &mut l.encoder, frame_size(frame));
        self.queued += 1;
        Ok(())
    }
    fn receive_packet(&mut self) -> Result<Packet> {
        if self.queued == 0 {
            return Err(Error::NeedMore);
        }
        self.queued -= 1;
        Ok(Packet::new(0, TimeBase::new(1, 25), vec![0]))
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

// ───────────────────────── filters ─────────────────────────

/// Video pass-through: frames unchanged; the event forwarded or (to
/// model a filter that predates the event) swallowed.
struct PassFilter {
    ports: Vec<PortSpec>,
    forwards: bool,
    log: Arc<Mutex<Log>>,
}

/// Refuses any event.
struct RefuseFilter {
    ports: Vec<PortSpec>,
}

/// Audio resampler to a fixed output rate: re-plans on the event and
/// keeps its output constant (so it emits no event of its own).
struct Resample {
    inputs: Vec<PortSpec>,
    outputs: Vec<PortSpec>,
    in_rate: u32,
    out_rate: u32,
    log: Arc<Mutex<Log>>,
}

impl StreamFilter for PassFilter {
    fn input_ports(&self) -> &[PortSpec] {
        &self.ports
    }
    fn output_ports(&self) -> &[PortSpec] {
        &self.ports
    }
    fn push(&mut self, ctx: &mut dyn FilterContext, _port: usize, frame: &Frame) -> Result<()> {
        push(&self.log, |l| &mut l.filter, frame_size(frame));
        if frame.is_stream_params() && !self.forwards {
            return Ok(());
        }
        ctx.emit(0, frame.clone())
    }
}

impl StreamFilter for RefuseFilter {
    fn input_ports(&self) -> &[PortSpec] {
        &self.ports
    }
    fn output_ports(&self) -> &[PortSpec] {
        &self.ports
    }
    fn push(&mut self, ctx: &mut dyn FilterContext, _port: usize, frame: &Frame) -> Result<()> {
        if frame.is_stream_params() {
            return Err(Error::unsupported("ev refuse: fixed input shape"));
        }
        ctx.emit(0, frame.clone())
    }
}

impl StreamFilter for Resample {
    fn input_ports(&self) -> &[PortSpec] {
        &self.inputs
    }
    fn output_ports(&self) -> &[PortSpec] {
        &self.outputs
    }
    fn push(&mut self, ctx: &mut dyn FilterContext, _port: usize, frame: &Frame) -> Result<()> {
        push(&self.log, |l| &mut l.filter, frame_size(frame));
        match frame {
            Frame::StreamParams(p) => {
                self.in_rate = p.sample_rate.expect("rate");
                Ok(())
            }
            Frame::Audio(a) => {
                let samples =
                    (a.samples as u64 * self.out_rate as u64 / self.in_rate as u64) as u32;
                ctx.emit(
                    0,
                    Frame::Audio(AudioFrame {
                        samples,
                        pts: a.pts,
                        data: vec![vec![0; samples as usize * 4]],
                    }),
                )
            }
            _ => Err(Error::unsupported("resample: audio only")),
        }
    }
}

fn filters(ctx: &mut RuntimeContext) {
    ctx.filters.register(
        "ev_pass",
        Box::new(|_params, inputs| {
            let log = log();
            let forwards = log.lock().unwrap().filter_forwards;
            Ok(Box::new(PassFilter {
                ports: inputs.to_vec(),
                forwards,
                log,
            }) as Box<dyn StreamFilter>)
        }),
    );
    ctx.filters.register(
        "ev_refuse",
        Box::new(|_params, inputs| {
            Ok(Box::new(RefuseFilter {
                ports: inputs.to_vec(),
            }) as Box<dyn StreamFilter>)
        }),
    );
    ctx.filters.register(
        "resample",
        Box::new(|params, inputs| {
            let out_rate = params["rate"].as_u64().expect("rate") as u32;
            let PortParams::Audio {
                sample_rate,
                channels,
                format,
            } = inputs[0].params
            else {
                return Err(Error::invalid("resample: audio port expected"));
            };
            Ok(Box::new(Resample {
                inputs: inputs.to_vec(),
                outputs: vec![PortSpec::audio("audio", out_rate, channels, format)],
                in_rate: sample_rate,
                out_rate,
                log: log(),
            }) as Box<dyn StreamFilter>)
        }),
    );
}

fn codecs() -> CodecRegistry {
    let mut codecs = CodecRegistry::new();
    for (id, caps) in [
        (VCODEC, CodecCapabilities::video("ev_vdec")),
        (ACODEC, CodecCapabilities::audio("ev_adec")),
    ] {
        codecs.register(
            CodecInfo::new(CodecId::new(id))
                .capabilities(caps.with_decode())
                .decoder(make_decoder as DecoderFactory),
        );
    }
    for id in [ENC_FOLLOW, ENC_FIXED] {
        codecs.register(
            CodecInfo::new(CodecId::new(id))
                .capabilities(
                    CodecCapabilities::video(id)
                        .with_encode()
                        .with_pixel_formats(vec![PixelFormat::Yuv444P]),
                )
                .encoder(make_encoder as EncoderFactory),
        );
    }
    codecs.register(
        CodecInfo::new(CodecId::new(AENC))
            .capabilities(
                CodecCapabilities::audio(AENC)
                    .with_encode()
                    .with_sample_rates(vec![48_000])
                    .with_sample_formats(vec![SampleFormat::S16]),
            )
            .encoder(make_encoder as EncoderFactory),
    );
    codecs
}

fn runtime(audio: bool) -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    ctx.codecs = codecs();
    let open = if audio { open_audio } else { open_video };
    ctx.containers
        .register_demuxer(CONTAINER, open as OpenDemuxerFn);
    ctx.sources.register_bytes(SCHEME, open_bytes);
    ctx.containers.register_extension(SCHEME, CONTAINER);
    filters(&mut ctx);
    ctx
}

// ───────────────────────── sinks ─────────────────────────

struct RecSink {
    log: Arc<Mutex<Log>>,
}

impl JobSink for RecSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        push(&self.log, |l| &mut l.sink, Seen::Packet);
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, f: &Frame) -> Result<()> {
        assert!(!f.is_stream_params(), "the event reached write_frame");
        push(&self.log, |l| &mut l.sink, frame_size(f));
        Ok(())
    }
    fn stream_params(&mut self, _kind: MediaType, p: &CodecParameters) -> Result<()> {
        push(&self.log, |l| &mut l.sink, describe(p));
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// `track` is the JSON of the single track of the single output.
fn run_job(
    audio: bool,
    track: &str,
    threads: usize,
) -> std::result::Result<(), oxideav_pipeline::RunFailure> {
    let ctx = runtime(audio);
    let kind = if audio { "audio" } else { "video" };
    let job = format!(r#"{{"@display":{{"{kind}":[{track}]}}}}"#);
    let job = Job::from_json(&job).unwrap();
    Executor::new(&job, &ctx)
        .with_threads(threads)
        .with_sink_override("@display", Box::new(RecSink { log: log() }))
        .run_reporting()
        .map(|_| ())
}

fn from() -> String {
    format!(r#"{{"from":"{SCHEME}://x/in.{SCHEME}"}}"#)
}

// ───────────────────────── executor ─────────────────────────

#[test]
fn raw_sink_hears_of_the_change_once_either_way() {
    for emits in [true, false] {
        for threads in [1, 2] {
            let (_g, l) = begin(emits);
            run_job(false, &from(), threads).unwrap();
            let l = l.lock().unwrap();
            let want = switch_log(16, Seen::Params(8, 8), 64);
            assert_eq!(l.sink, want, "emits={emits} threads={threads}");
            // The emitting decoder's own event is the one in the log.
            let events = l
                .decoder
                .iter()
                .filter(|s| matches!(s, Seen::Params(..)))
                .count();
            assert_eq!(events, usize::from(emits));
        }
    }
}

#[test]
fn pass_through_filter_carries_the_event_forwarded_or_not() {
    for forwards in [true, false] {
        for emits in [true, false] {
            for threads in [1, 2] {
                let (_g, l) = begin(emits);
                l.lock().unwrap().filter_forwards = forwards;
                let track = format!(r#"{{"filter":"ev_pass","input":{}}}"#, from());
                run_job(false, &track, threads).unwrap();
                let l = l.lock().unwrap();
                let tag = format!("forwards={forwards} emits={emits} threads={threads}");
                // The filter is handed the event exactly once …
                assert_eq!(l.filter, switch_log(16, Seen::Params(8, 8), 64), "{tag}");
                // … and the sink hears of it once, before the 8×8 frames.
                assert_eq!(l.sink, switch_log(16, Seen::Params(8, 8), 64), "{tag}");
            }
        }
    }
}

#[test]
fn size_change_through_filter_and_conversion_reconfigures_the_encoder() {
    for emits in [true, false] {
        for threads in [1, 2] {
            let (_g, l) = begin(emits);
            let track = format!(
                r#"{{"filter":"ev_pass","input":{},"codec":"{ENC_FOLLOW}"}}"#,
                from()
            );
            run_job(false, &track, threads).unwrap();
            let l = l.lock().unwrap();
            let tag = format!("emits={emits} threads={threads}");
            // Converted to Yuv444P (3 full planes) by a conversion that
            // re-planned for 8×8; the encoder got exactly one event.
            assert_eq!(l.encoder, switch_log(48, Seen::Params(8, 8), 192), "{tag}");
            // The sink: the packets, and the encoder's new output
            // stated once, in order.
            let mut want = vec![Seen::Packet; SWITCH_AT as usize];
            want.push(Seen::Params(8, 8));
            want.extend(vec![Seen::Packet; (PACKETS - SWITCH_AT) as usize]);
            assert_eq!(l.sink, want, "{tag}");
        }
    }
}

#[test]
fn an_encoder_that_cannot_follow_fails_the_run_naming_both_shapes() {
    for emits in [true, false] {
        for threads in [1, 2] {
            let (_g, l) = begin(emits);
            let track = format!(
                r#"{{"filter":"ev_pass","input":{},"codec":"{ENC_FIXED}"}}"#,
                from()
            );
            let fail = run_job(false, &track, threads).unwrap_err();
            let tag = format!("emits={emits} threads={threads}");
            assert_eq!(fail.stage, FailureStage::Encode, "{tag}");
            let err = fail.error.to_string();
            assert!(matches!(fail.error, Error::Unsupported(_)), "{tag}: {err}");
            assert!(err.contains("4x4") && err.contains("8x8"), "{tag}: {err}");
            let l = l.lock().unwrap();
            // Nothing of the new shape reached the encoder.
            assert_eq!(
                only_frames(&l.encoder).len(),
                SWITCH_AT as usize + 1,
                "{tag}"
            );
        }
    }
}

#[test]
fn a_filter_that_refuses_fails_the_run_at_the_filter() {
    for threads in [1, 2] {
        let (_g, _l) = begin(true);
        let track = format!(r#"{{"filter":"ev_refuse","input":{}}}"#, from());
        let fail = run_job(false, &track, threads).unwrap_err();
        assert_eq!(fail.stage, FailureStage::Filter, "threads={threads}");
        assert!(
            matches!(fail.error, Error::Unsupported(_)),
            "{}",
            fail.error
        );
        assert!(fail.error.to_string().contains("8x8"), "{}", fail.error);
    }
}

#[test]
fn resampler_absorbs_a_sample_rate_change_ahead_of_the_encoder() {
    for emits in [true, false] {
        for threads in [1, 2] {
            let (_g, l) = begin(emits);
            let track = format!(r#"{{"from":"{SCHEME}://x/in.{SCHEME}","codec":"{AENC}"}}"#);
            run_job(true, &track, threads).unwrap();
            let l = l.lock().unwrap();
            let tag = format!("emits={emits} threads={threads}");
            // The auto-inserted resampler (44.1 kHz → 48 kHz) was told
            // of the 32 kHz input once …
            assert_eq!(
                l.filter,
                switch_log(441, Seen::Params(32_000, 2), 320),
                "{tag}"
            );
            // … and kept its output at 48 kHz: the encoder never saw a
            // change, only 480-sample frames.
            assert_eq!(l.encoder, vec![Seen::Frame(480); PACKETS as usize], "{tag}");
            assert_eq!(l.sink, vec![Seen::Packet; PACKETS as usize], "{tag}");
        }
    }
}

// ───────────────────────── transcode_simple ─────────────────────────

struct NullMuxer;

impl Muxer for NullMuxer {
    fn format_name(&self) -> &str {
        "null"
    }
    fn write_header(&mut self) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _packet: &Packet) -> Result<()> {
        Ok(())
    }
    fn write_trailer(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn transcode_simple_follows_the_event_to_the_encoder() {
    for emits in [true, false] {
        let (_g, l) = begin(emits);
        transcode_simple(
            &mut Demux::new(false),
            |_| Ok(Box::new(NullMuxer) as Box<dyn Muxer>),
            &codecs(),
            |_| {
                Ok(StreamPlan::Reencode {
                    output_codec: ENC_FOLLOW.to_string(),
                })
            },
        )
        .unwrap();
        let l = l.lock().unwrap();
        assert_eq!(
            l.encoder,
            switch_log(48, Seen::Params(8, 8), 192),
            "emits={emits}"
        );
        drop(l);
        drop(_g);

        let (_g, l) = begin(emits);
        let err = transcode_simple(
            &mut Demux::new(false),
            |_| Ok(Box::new(NullMuxer) as Box<dyn Muxer>),
            &codecs(),
            |_| {
                Ok(StreamPlan::Reencode {
                    output_codec: ENC_FIXED.to_string(),
                })
            },
        )
        .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)), "{err}");
        assert!(err.to_string().contains("8x8"), "{err}");
        assert_eq!(
            only_frames(&l.lock().unwrap().encoder).len(),
            SWITCH_AT as usize + 1
        );
    }
}

#[test]
fn transcode_simple_in_resamples_through_a_rate_change() {
    for emits in [true, false] {
        let (_g, l) = begin(emits);
        let ctx = runtime(true);
        transcode_simple_in(
            &mut Demux::new(true),
            |_| Ok(Box::new(NullMuxer) as Box<dyn Muxer>),
            &ctx,
            None,
            &Default::default(),
            |_| {
                Ok(StreamPlan::Reencode {
                    output_codec: AENC.to_string(),
                })
            },
        )
        .unwrap();
        let l = l.lock().unwrap();
        assert_eq!(l.filter, switch_log(441, Seen::Params(32_000, 2), 320));
        assert_eq!(l.encoder, vec![Seen::Frame(480); PACKETS as usize]);
    }
}

// ───────────────────────── Pipeline routes ─────────────────────────

struct RouteSink {
    target: &'static str,
    log: Arc<Mutex<Log>>,
}

impl Sink for RouteSink {
    fn accepts(&self, src: &CodecParameters) -> SinkAcceptance {
        let mut target = src.clone();
        target.codec_id = CodecId::new(self.target);
        target.pixel_format = Some(PixelFormat::Yuv444P);
        SinkAcceptance::Transcode { target }
    }
    fn write_packet(&mut self, _p: &Packet) -> Result<()> {
        push(&self.log, |l| &mut l.sink, Seen::Packet);
        Ok(())
    }
    fn write_frame(&mut self, f: &Frame) -> Result<()> {
        assert!(!f.is_stream_params(), "the event reached write_frame");
        push(&self.log, |l| &mut l.sink, frame_size(f));
        Ok(())
    }
    fn stream_params(&mut self, p: &CodecParameters) -> Result<()> {
        push(&self.log, |l| &mut l.sink, describe(p));
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn pipeline_routes_hand_the_change_to_the_sink_once() {
    for emits in [true, false] {
        // Decode route: frames + one hook call.
        let (_g, l) = begin(emits);
        let mut pl = Pipeline::new(Box::new(Demux::new(false)), codecs());
        pl.bind(
            0,
            Box::new(RouteSink {
                target: VCODEC,
                log: l.clone(),
            }),
        )
        .unwrap();
        pl.run().unwrap();
        assert_eq!(pl.stream_params(0).unwrap().width, Some(8));
        assert_eq!(
            l.lock().unwrap().sink,
            switch_log(16, Seen::Params(8, 8), 64),
            "emits={emits}"
        );
        drop(_g);

        // Transcode route to an encoder that re-configures (no
        // conversion here: the encoder takes the decoder's frames).
        let (_g, l) = begin(emits);
        let mut pl = Pipeline::new(Box::new(Demux::new(false)), codecs());
        pl.bind(
            0,
            Box::new(RouteSink {
                target: ENC_FOLLOW,
                log: l.clone(),
            }),
        )
        .unwrap();
        pl.run().unwrap();
        let l = l.lock().unwrap();
        assert_eq!(l.encoder, switch_log(16, Seen::Params(8, 8), 64));
        let mut want = vec![Seen::Packet; SWITCH_AT as usize];
        want.push(Seen::Params(8, 8));
        want.extend(vec![Seen::Packet; (PACKETS - SWITCH_AT) as usize]);
        assert_eq!(l.sink, want, "emits={emits}");
    }
}
