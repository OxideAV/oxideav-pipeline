//! Decoder-reported stream parameters reach every consumer.
//!
//! Frames carry no width / height / pixel format by design: what a
//! decoder emits is described by [`Decoder::output_params`], merged
//! into the stream's parameters, and a mid-stream change is announced
//! by [`Decoder::output_params_generation`]. The mock decoder here
//! reports Gray8 4×4 from construction (the container claimed 4×6 and
//! no layout) and switches mid-stream — to 4:2:0 at the same size
//! (absorbed by the conversion in front of the encoder) or to 8×8
//! (which an encoder opened for 4×4 cannot follow). Each entry point is
//! driven: `transcode_simple`, `Pipeline` routes, and the executor
//! (serial and pipelined).

use std::sync::{Arc, Mutex, MutexGuard};

use oxideav_core::registry::CodecInfo;
use oxideav_core::{
    BytesSource, CodecCapabilities, CodecId, CodecParameters, CodecRegistry, CodecResolver,
    Decoder, DecoderFactory, Demuxer, Encoder, EncoderFactory, Error, Frame, MediaType, Muxer,
    OpenDemuxerFn, Packet, PixelFormat, ReadSeek, Result, RuntimeContext, StreamInfo, TimeBase,
    VideoFrame, VideoPlane, packet::PacketFlags,
};
use oxideav_pipeline::{
    Executor, Job, JobSink, Pipeline, Sink, SinkAcceptance, StreamPlan, transcode_simple,
};

const CODEC: &str = "dop_raw";
const ENC: &str = "dop_enc";
const CONTAINER: &str = "dop_container";
const SCHEME: &str = "dop";
const PACKETS: u32 = 12;
/// Index of the first packet decoded in the new shape.
const SWITCH_AT: u32 = 5;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Mode {
    /// Gray8 4×4 throughout.
    Fixed,
    /// Gray8 4×4, then Yuv420P 4×4 (chroma 200).
    NewLayout,
    /// Gray8 4×4, then Gray8 8×8.
    NewSize,
}

/// Shared state: the mode the next decoder runs in, and what the
/// decoder / encoder / sinks observed.
#[derive(Default)]
struct Log {
    mode: Option<Mode>,
    /// Frames the decoder had returned at each `output_params` call.
    queries: Vec<u32>,
    /// Per encoded frame: (plane lengths, first byte of plane 1).
    encoded: Vec<(Vec<usize>, Option<u8>)>,
    /// Per raw frame a sink received: plane lengths.
    raw: Vec<Vec<usize>>,
    /// Streams the job sink was started with.
    started: Vec<StreamInfo>,
}

static LOG: Mutex<Option<Arc<Mutex<Log>>>> = Mutex::new(None);
static SERIAL: Mutex<()> = Mutex::new(());

fn log() -> Arc<Mutex<Log>> {
    LOG.lock().unwrap().clone().expect("test installs LOG")
}

/// Serialise the tests (they share LOG) and install a fresh log.
fn begin(mode: Mode) -> (MutexGuard<'static, ()>, Arc<Mutex<Log>>) {
    let guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let l = Arc::new(Mutex::new(Log {
        mode: Some(mode),
        ..Log::default()
    }));
    *LOG.lock().unwrap() = Some(l.clone());
    (guard, l)
}

fn container_params() -> CodecParameters {
    let mut params = CodecParameters::video(CodecId::new(CODEC));
    // The container's claim: a coded height, no layout.
    params.width = Some(4);
    params.height = Some(6);
    params.bit_rate = Some(1234);
    params
}

fn stream() -> StreamInfo {
    StreamInfo {
        index: 0,
        time_base: TimeBase::new(1, 25),
        duration: Some(PACKETS as i64),
        start_time: Some(0),
        params: container_params(),
    }
}

struct Demux {
    streams: Vec<StreamInfo>,
    next: u32,
}

impl Demux {
    fn new() -> Self {
        Self {
            streams: vec![stream()],
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
    fn seek_to(&mut self, _s: u32, pts: i64) -> Result<i64> {
        Ok(pts.max(0))
    }
}

fn open_bytes(_uri: &str) -> Result<Box<dyn BytesSource>> {
    Ok(Box::new(std::io::Cursor::new(vec![0u8; 16])))
}

fn open_demuxer(_input: Box<dyn ReadSeek>, _c: &dyn CodecResolver) -> Result<Box<dyn Demuxer>> {
    Ok(Box::new(Demux::new()))
}

/// Decodes packet `n` into a frame of the shape the mode gives it;
/// reports that shape, bumping the generation when it returns the
/// first frame of a new one.
struct ShapeDecoder {
    mode: Mode,
    pending: Option<Packet>,
    returned: u32,
    switched: bool,
    log: Arc<Mutex<Log>>,
}

fn make_decoder(_p: &CodecParameters) -> Result<Box<dyn Decoder>> {
    let log = log();
    let mode = log.lock().unwrap().mode.expect("mode");
    Ok(Box::new(ShapeDecoder {
        mode,
        pending: None,
        returned: 0,
        switched: false,
        log,
    }))
}

impl Decoder for ShapeDecoder {
    fn codec_id(&self) -> &CodecId {
        static ID: std::sync::OnceLock<CodecId> = std::sync::OnceLock::new();
        ID.get_or_init(|| CodecId::new(CODEC))
    }
    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        self.pending = Some(packet.clone());
        Ok(())
    }
    fn receive_frame(&mut self) -> Result<Frame> {
        let p = self.pending.take().ok_or(Error::NeedMore)?;
        let n = p.data[0] as u32;
        self.returned += 1;
        let new_shape = self.mode != Mode::Fixed && n >= SWITCH_AT;
        self.switched = new_shape;
        let planes = match (self.mode, new_shape) {
            (Mode::NewLayout, true) => vec![
                VideoPlane {
                    stride: 4,
                    data: vec![100; 16],
                },
                VideoPlane {
                    stride: 2,
                    data: vec![200; 4],
                },
                VideoPlane {
                    stride: 2,
                    data: vec![200; 4],
                },
            ],
            (Mode::NewSize, true) => vec![VideoPlane {
                stride: 8,
                data: vec![100; 64],
            }],
            _ => vec![VideoPlane {
                stride: 4,
                data: vec![100; 16],
            }],
        };
        Ok(Frame::Video(VideoFrame { pts: p.pts, planes }))
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn output_params(&self) -> Option<CodecParameters> {
        self.log.lock().unwrap().queries.push(self.returned);
        let mut p = CodecParameters::video(CodecId::new(CODEC));
        p.width = Some(4);
        p.height = Some(4);
        p.pixel_format = Some(PixelFormat::Gray8);
        if self.switched {
            match self.mode {
                Mode::NewLayout => p.pixel_format = Some(PixelFormat::Yuv420P),
                Mode::NewSize => {
                    p.width = Some(8);
                    p.height = Some(8);
                }
                Mode::Fixed => {}
            }
        }
        Some(p)
    }
    fn output_params_generation(&self) -> u64 {
        u64::from(self.switched)
    }
    fn output_pixel_format(&self) -> Option<PixelFormat> {
        self.output_params().and_then(|p| p.pixel_format)
    }
}

/// Takes Yuv444P only; records every frame's planes. Refuses to be
/// built for the container's 4×6 claim.
struct RecordingEncoder {
    params: CodecParameters,
    log: Arc<Mutex<Log>>,
}

fn make_encoder(p: &CodecParameters) -> Result<Box<dyn Encoder>> {
    if p.pixel_format.is_none() || p.height != Some(4) {
        return Err(Error::invalid(format!(
            "dop encoder: wants a 4x4 layout, got {:?}x{:?} {:?}",
            p.width, p.height, p.pixel_format
        )));
    }
    Ok(Box::new(RecordingEncoder {
        params: p.clone(),
        log: log(),
    }))
}

impl Encoder for RecordingEncoder {
    fn codec_id(&self) -> &CodecId {
        &self.params.codec_id
    }
    fn output_params(&self) -> &CodecParameters {
        &self.params
    }
    fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        let Frame::Video(v) = frame else {
            return Err(Error::invalid("video expected"));
        };
        self.log.lock().unwrap().encoded.push((
            v.planes.iter().map(|p| p.data.len()).collect(),
            v.planes.get(1).map(|p| p.data[0]),
        ));
        Ok(())
    }
    fn receive_packet(&mut self) -> Result<Packet> {
        Err(Error::NeedMore)
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn codecs() -> CodecRegistry {
    let mut codecs = CodecRegistry::new();
    codecs.register(
        CodecInfo::new(CodecId::new(CODEC))
            .capabilities(CodecCapabilities::video("dop_dec").with_decode())
            .decoder(make_decoder as DecoderFactory),
    );
    codecs.register(
        CodecInfo::new(CodecId::new(ENC))
            .capabilities(
                CodecCapabilities::video("dop_enc")
                    .with_encode()
                    .with_pixel_formats(vec![PixelFormat::Yuv444P]),
            )
            .encoder(make_encoder as EncoderFactory),
    );
    codecs
}

fn runtime() -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    ctx.codecs = codecs();
    ctx.containers
        .register_demuxer(CONTAINER, open_demuxer as OpenDemuxerFn);
    ctx.sources.register_bytes(SCHEME, open_bytes);
    ctx.containers.register_extension(SCHEME, CONTAINER);
    ctx
}

/// `output_params` is read up front and then only when the generation
/// moved: never once per frame.
fn assert_queries_bounded(l: &Log, change: bool) {
    let after_frames: Vec<u32> = l.queries.iter().copied().filter(|&n| n > 0).collect();
    let want = if change {
        vec![1, SWITCH_AT + 1]
    } else {
        vec![1]
    };
    assert_eq!(after_frames, want, "queries: {:?}", l.queries);
}

fn assert_layout_switch_converted(l: &Log) {
    assert_eq!(l.encoded.len(), PACKETS as usize);
    for (i, (planes, chroma)) in l.encoded.iter().enumerate() {
        assert_eq!(planes, &vec![16, 16, 16], "frame {i}");
        // Gray8 converts to neutral chroma; the 4:2:0 frames carry 200
        // — only a conversion re-planned for the new layout keeps it.
        let want = if (i as u32) < SWITCH_AT { 128 } else { 200 };
        assert_eq!(*chroma, Some(want), "frame {i}");
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

fn run_transcode_simple() -> Result<Vec<StreamInfo>> {
    let mut demuxer = Demux::new();
    let codecs = codecs();
    let opened = Arc::new(Mutex::new(Vec::new()));
    let o = opened.clone();
    transcode_simple(
        &mut demuxer,
        move |streams| {
            *o.lock().unwrap() = streams.to_vec();
            Ok(Box::new(NullMuxer) as Box<dyn Muxer>)
        },
        &codecs,
        |_| {
            Ok(StreamPlan::Reencode {
                output_codec: ENC.to_string(),
            })
        },
    )?;
    Ok(opened.lock().unwrap().clone())
}

#[test]
fn transcode_simple_plans_from_and_follows_the_decoder() {
    let (_g, l) = begin(Mode::Fixed);
    // The encoder refuses the container's 4×6: it was built from the
    // decoder's 4×4 Gray8, converted to its Yuv444P.
    let opened = run_transcode_simple().unwrap();
    assert_eq!(opened[0].params.height, Some(4));
    assert_eq!(opened[0].params.pixel_format, Some(PixelFormat::Yuv444P));
    assert_queries_bounded(&l.lock().unwrap(), false);
    drop(_g);

    let (_g, l) = begin(Mode::NewLayout);
    run_transcode_simple().unwrap();
    let l = l.lock().unwrap();
    assert_layout_switch_converted(&l);
    assert_queries_bounded(&l, true);
}

#[test]
fn transcode_simple_refuses_a_size_change_the_encoder_cannot_follow() {
    let (_g, l) = begin(Mode::NewSize);
    let err = run_transcode_simple().unwrap_err();
    assert!(matches!(err, Error::Unsupported(_)), "{err}");
    assert!(err.to_string().contains("8x8"), "{err}");
    // Nothing of the new shape reached the encoder.
    assert_eq!(l.lock().unwrap().encoded.len(), SWITCH_AT as usize);
}

// ───────────────────────── Pipeline routes ─────────────────────────

struct RawSink {
    log: Arc<Mutex<Log>>,
    target: &'static str,
}

impl Sink for RawSink {
    fn accepts(&self, src: &CodecParameters) -> SinkAcceptance {
        let mut target = src.clone();
        target.codec_id = CodecId::new(self.target);
        target.pixel_format = Some(PixelFormat::Yuv444P);
        target.height = Some(4);
        SinkAcceptance::Transcode { target }
    }
    fn write_packet(&mut self, _p: &Packet) -> Result<()> {
        Ok(())
    }
    fn write_frame(&mut self, f: &Frame) -> Result<()> {
        if let Frame::Video(v) = f {
            self.log
                .lock()
                .unwrap()
                .raw
                .push(v.planes.iter().map(|p| p.data.len()).collect());
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn pipeline_decode_route_exposes_the_merged_stream_params() {
    let (_g, l) = begin(Mode::NewSize);
    let mut pl = Pipeline::new(Box::new(Demux::new()), codecs());
    pl.bind(
        0,
        Box::new(RawSink {
            log: l.clone(),
            target: CODEC,
        }),
    )
    .unwrap();
    // Primed at bind: the decoder's 4×4 Gray8 over the container's 4×6,
    // container-side fields kept.
    let p = pl.stream_params(0).unwrap();
    assert_eq!((p.width, p.height), (Some(4), Some(4)));
    assert_eq!(p.pixel_format, Some(PixelFormat::Gray8));
    assert_eq!(p.bit_rate, Some(1234));
    assert!(pl.stream_params(7).is_none());
    pl.run().unwrap();
    // A raw-frame sink gets the new-shape frames; the stream says so.
    let p = pl.stream_params(0).unwrap();
    assert_eq!((p.width, p.height), (Some(8), Some(8)));
    let l = l.lock().unwrap();
    assert_eq!(l.raw.len(), PACKETS as usize);
    assert_eq!(l.raw[SWITCH_AT as usize], vec![64]);
    assert_queries_bounded(&l, true);
}

#[test]
fn pipeline_transcode_route_refuses_a_size_change() {
    let (_g, l) = begin(Mode::NewSize);
    let mut pl = Pipeline::new(Box::new(Demux::new()), codecs());
    pl.bind(
        0,
        Box::new(RawSink {
            log: l.clone(),
            target: ENC,
        }),
    )
    .unwrap();
    let err = pl.run().unwrap_err();
    assert!(matches!(err, Error::Unsupported(_)), "{err}");
}

// ───────────────────────── Executor ─────────────────────────

struct StartSink {
    log: Arc<Mutex<Log>>,
}

impl JobSink for StartSink {
    fn start(&mut self, streams: &[StreamInfo]) -> Result<()> {
        self.log.lock().unwrap().started = streams.to_vec();
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, f: &Frame) -> Result<()> {
        if let Frame::Video(v) = f {
            self.log
                .lock()
                .unwrap()
                .raw
                .push(v.planes.iter().map(|p| p.data.len()).collect());
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

fn run_executor(encode: bool, threads: usize) -> Result<()> {
    let ctx = runtime();
    let job = if encode {
        format!(
            r#"{{"@null":{{"video":[{{"from":"{SCHEME}://x/in.{SCHEME}","codec":"{ENC}"}}]}}}}"#
        )
    } else {
        format!(r#"{{"@display":{{"video":[{{"from":"{SCHEME}://x/in.{SCHEME}"}}]}}}}"#)
    };
    let job = Job::from_json(&job).unwrap();
    Executor::new(&job, &ctx)
        .with_threads(threads)
        .with_sink_override("@display", Box::new(StartSink { log: log() }))
        .run()
        .map(|_| ())
}

#[test]
fn executor_follows_a_layout_switch_through_the_conversion() {
    for threads in [1, 2] {
        let (_g, l) = begin(Mode::NewLayout);
        run_executor(true, threads).unwrap();
        let l = l.lock().unwrap();
        assert_layout_switch_converted(&l);
        assert_queries_bounded(&l, true);
    }
}

#[test]
fn executor_refuses_a_size_change_the_encoder_cannot_follow() {
    for threads in [1, 2] {
        let (_g, l) = begin(Mode::NewSize);
        let err = run_executor(true, threads).unwrap_err();
        assert!(err.to_string().contains("8x8"), "threads={threads}: {err}");
        assert!(l.lock().unwrap().encoded.len() <= SWITCH_AT as usize);
    }
}

#[test]
fn executor_sink_starts_with_the_decoder_reported_stream() {
    for threads in [1, 2] {
        let (_g, l) = begin(Mode::NewSize);
        run_executor(false, threads).unwrap();
        let l = l.lock().unwrap();
        let p = &l.started[0].params;
        assert_eq!((p.width, p.height), (Some(4), Some(4)), "threads={threads}");
        assert_eq!(p.pixel_format, Some(PixelFormat::Gray8));
        assert_eq!(l.raw.len(), PACKETS as usize);
        assert_eq!(l.raw[SWITCH_AT as usize], vec![64]);
        assert_queries_bounded(&l, true);
    }
}
