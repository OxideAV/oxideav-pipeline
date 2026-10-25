//! Every codec the framework builds on the caller's behalf receives a
//! thread budget before its first packet / frame.
//!
//! `oxideav-core` codecs run serial until `set_execution_context` is
//! called, so a driver that forgets the call silently pins a 1080p
//! decode to one core. Probe codecs here record the budget they were
//! given into their output: the probe decoder writes its thread count
//! into every frame, the probe encoder appends its own, and the test
//! reads both back off the muxed / sunk packets for each entry point —
//! `transcode_simple` (default = auto), `transcode_simple_in_with`
//! (explicit), `Pipeline` (default + `with_threads`) and the
//! `Executor` (`with_threads`). The split rule (`stream_budget`) gives
//! a video codec the whole budget and an audio codec next to it a
//! serial one.

mod common;

use std::sync::{Arc, Mutex};

use oxideav_core::registry::CodecInfo;
use oxideav_core::{
    AudioFrame, CodecCapabilities, CodecId, CodecParameters, CodecRegistry, Decoder,
    DecoderFactory, Demuxer, Encoder, EncoderFactory, Error, ExecutionContext, Frame, MediaType,
    Muxer, Packet, PixelFormat, Result, SampleFormat, StreamInfo, TimeBase, VideoFrame, VideoPlane,
    packet::PacketFlags,
};
use oxideav_pipeline::{
    Executor, Job, JobSink, Pipeline, Sink, SinkAcceptance, StreamPlan, transcode_simple,
    transcode_simple_in_with,
};

const V_IN: &str = "probe_budget_video";
const V_OUT: &str = "probe_budget_video_out";
const A_IN: &str = "probe_budget_audio";
const A_OUT: &str = "probe_budget_audio_out";

// ---- probe codecs ----

struct ProbeDecoder {
    id: CodecId,
    media: MediaType,
    threads: usize,
    pending: Option<Packet>,
}

impl Decoder for ProbeDecoder {
    fn codec_id(&self) -> &CodecId {
        &self.id
    }
    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        self.pending = Some(packet.clone());
        Ok(())
    }
    fn receive_frame(&mut self) -> Result<Frame> {
        let p = self.pending.take().ok_or(Error::NeedMore)?;
        let t = self.threads as u8;
        Ok(match self.media {
            MediaType::Video => Frame::Video(VideoFrame {
                pts: p.pts,
                planes: vec![
                    VideoPlane {
                        stride: 2,
                        data: vec![t; 4],
                    },
                    VideoPlane {
                        stride: 1,
                        data: vec![t],
                    },
                    VideoPlane {
                        stride: 1,
                        data: vec![t],
                    },
                ],
            }),
            _ => Frame::Audio(AudioFrame {
                samples: 1,
                pts: p.pts,
                data: vec![vec![t, 0]],
            }),
        })
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn reset(&mut self) -> Result<()> {
        self.pending = None;
        Ok(())
    }
    fn set_execution_context(&mut self, ctx: &ExecutionContext) {
        self.threads = ctx.threads;
    }
}

struct ProbeEncoder {
    params: CodecParameters,
    threads: usize,
    queue: Vec<Packet>,
}

impl Encoder for ProbeEncoder {
    fn codec_id(&self) -> &CodecId {
        &self.params.codec_id
    }
    fn output_params(&self) -> &CodecParameters {
        &self.params
    }
    fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        let dec = match frame {
            Frame::Video(v) => v.planes[0].data[0],
            Frame::Audio(a) => a.data[0][0],
            _ => return Err(Error::invalid("probe encoder: audio / video only")),
        };
        let mut pkt = Packet::new(0, TimeBase::new(1, 25), vec![dec, self.threads as u8]);
        pkt.flags.keyframe = true;
        self.queue.push(pkt);
        Ok(())
    }
    fn receive_packet(&mut self) -> Result<Packet> {
        if self.queue.is_empty() {
            Err(Error::NeedMore)
        } else {
            Ok(self.queue.remove(0))
        }
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn set_execution_context(&mut self, ctx: &ExecutionContext) {
        self.threads = ctx.threads;
    }
}

fn dec(id: &str, media: MediaType) -> Box<dyn Decoder> {
    Box::new(ProbeDecoder {
        id: CodecId::new(id),
        media,
        threads: 1,
        pending: None,
    })
}
fn make_v_dec(_: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(dec(V_IN, MediaType::Video))
}
fn make_a_dec(_: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(dec(A_IN, MediaType::Audio))
}
/// The in-tree stub container's codec id, decoded by the probe.
fn make_stub_dec(_: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(dec(common::stub::CODEC_ID, MediaType::Audio))
}
fn make_enc(p: &CodecParameters) -> Result<Box<dyn Encoder>> {
    Ok(Box::new(ProbeEncoder {
        params: p.clone(),
        threads: 1,
        queue: Vec::new(),
    }))
}

fn registry() -> CodecRegistry {
    let mut reg = CodecRegistry::default();
    for (id, f) in [(V_IN, make_v_dec as DecoderFactory), (A_IN, make_a_dec)] {
        let caps = if id == V_IN {
            CodecCapabilities::video(id)
        } else {
            CodecCapabilities::audio(id)
        };
        reg.register(
            CodecInfo::new(CodecId::new(id))
                .capabilities(caps.with_decode())
                .decoder(f),
        );
    }
    reg.register(
        CodecInfo::new(CodecId::new(V_OUT))
            .capabilities(CodecCapabilities::video(V_OUT).with_encode())
            .encoder(make_enc as EncoderFactory),
    );
    reg.register(
        CodecInfo::new(CodecId::new(A_OUT))
            .capabilities(CodecCapabilities::audio(A_OUT).with_encode())
            .encoder(make_enc as EncoderFactory),
    );
    reg
}

// ---- video + audio source ----

struct AvDemuxer {
    streams: Vec<StreamInfo>,
    left: Vec<u32>,
}

impl AvDemuxer {
    fn new(with_video: bool) -> Self {
        let mut v = CodecParameters::video(CodecId::new(V_IN));
        v.width = Some(2);
        v.height = Some(2);
        v.pixel_format = Some(PixelFormat::Yuv420P);
        let mut a = CodecParameters::audio(CodecId::new(A_IN));
        a.sample_rate = Some(8_000);
        a.channels = Some(1);
        a.sample_format = Some(SampleFormat::S16);
        let mut streams = vec![StreamInfo {
            index: 0,
            time_base: TimeBase::new(1, 8_000),
            duration: None,
            start_time: Some(0),
            params: a,
        }];
        if with_video {
            streams.push(StreamInfo {
                index: 1,
                time_base: TimeBase::new(1, 25),
                duration: None,
                start_time: Some(0),
                params: v,
            });
        }
        let n = streams.len() as u32;
        let left = (0..6).map(|i| i % n).rev().collect();
        Self { streams, left }
    }
}

impl Demuxer for AvDemuxer {
    fn format_name(&self) -> &str {
        "probe_av"
    }
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }
    fn next_packet(&mut self) -> Result<Packet> {
        let stream_index = self.left.pop().ok_or(Error::Eof)?;
        Ok(Packet {
            stream_index,
            time_base: self.streams[stream_index as usize].time_base,
            pts: Some(0),
            dts: Some(0),
            duration: Some(1),
            flags: PacketFlags::default(),
            data: vec![0, 0],
        })
    }
    fn seek_to(&mut self, _: u32, _: i64) -> Result<i64> {
        Err(Error::unsupported("probe: no seek"))
    }
}

// ---- capture ----

type Seen = Arc<Mutex<Vec<(u32, Vec<u8>)>>>;

struct CaptureMuxer(Seen);

impl Muxer for CaptureMuxer {
    fn format_name(&self) -> &str {
        "capture"
    }
    fn write_header(&mut self) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, p: &Packet) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .push((p.stream_index, p.data.clone()));
        Ok(())
    }
    fn write_trailer(&mut self) -> Result<()> {
        Ok(())
    }
}

fn reencode(s: &StreamInfo) -> Result<StreamPlan> {
    Ok(StreamPlan::Reencode {
        output_codec: match s.params.media_type {
            MediaType::Video => V_OUT.into(),
            _ => A_OUT.into(),
        },
    })
}

/// `(decoder threads, encoder threads)` per output stream, checking
/// every packet of a stream agrees.
fn budgets(seen: &Seen) -> Vec<(u32, (u8, u8))> {
    let mut out: Vec<(u32, (u8, u8))> = Vec::new();
    for (idx, data) in seen.lock().unwrap().iter() {
        let b = (data[0], data[1]);
        match out.iter().find(|(i, _)| i == idx) {
            Some((_, prev)) => assert_eq!(*prev, b, "stream {idx} budget changed mid-run"),
            None => out.push((*idx, b)),
        }
    }
    out.sort();
    out
}

fn auto() -> u8 {
    ExecutionContext::auto().threads.min(255) as u8
}

#[test]
fn transcode_simple_defaults_to_the_auto_budget() {
    let seen: Seen = Default::default();
    let s2 = seen.clone();
    let mut dmx = AvDemuxer::new(true);
    transcode_simple(
        &mut dmx,
        move |_| Ok(Box::new(CaptureMuxer(s2)) as Box<dyn Muxer>),
        &registry(),
        reencode,
    )
    .unwrap();
    // Out 0 = audio (serial next to video), out 1 = video (all of it).
    assert_eq!(budgets(&seen), vec![(0, (1, 1)), (1, (auto(), auto()))]);
}

#[test]
fn transcode_simple_audio_only_gets_the_whole_budget() {
    let seen: Seen = Default::default();
    let s2 = seen.clone();
    let mut dmx = AvDemuxer::new(false);
    transcode_simple(
        &mut dmx,
        move |_| Ok(Box::new(CaptureMuxer(s2)) as Box<dyn Muxer>),
        &registry(),
        reencode,
    )
    .unwrap();
    assert_eq!(budgets(&seen), vec![(0, (auto(), auto()))]);
}

#[test]
fn transcode_simple_in_with_honours_an_explicit_budget() {
    let mut ctx = oxideav_core::RuntimeContext::new();
    ctx.codecs = registry();
    for (budget, want) in [
        (ExecutionContext::with_threads(3), 3u8),
        (ExecutionContext::serial(), 1),
    ] {
        let seen: Seen = Default::default();
        let s2 = seen.clone();
        let mut dmx = AvDemuxer::new(true);
        transcode_simple_in_with(
            &mut dmx,
            move |_| Ok(Box::new(CaptureMuxer(s2)) as Box<dyn Muxer>),
            &ctx,
            None,
            &Default::default(),
            &budget,
            reencode,
        )
        .unwrap();
        assert_eq!(budgets(&seen), vec![(0, (1, 1)), (1, (want, want))]);
    }
}

struct CaptureSink {
    target: &'static str,
    index: u32,
    seen: Seen,
}

impl Sink for CaptureSink {
    fn accepts(&self, src: &CodecParameters) -> SinkAcceptance {
        let mut target = src.clone();
        target.codec_id = CodecId::new(self.target);
        SinkAcceptance::Transcode { target }
    }
    fn write_packet(&mut self, p: &Packet) -> Result<()> {
        self.seen.lock().unwrap().push((self.index, p.data.clone()));
        Ok(())
    }
    fn write_frame(&mut self, _: &Frame) -> Result<()> {
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn run_pipeline(threads: Option<usize>) -> Vec<(u32, (u8, u8))> {
    let seen: Seen = Default::default();
    let mut pl = Pipeline::new(Box::new(AvDemuxer::new(true)), registry());
    if let Some(n) = threads {
        pl = pl.with_threads(n);
    }
    for (index, (stream, target)) in [(0u32, A_OUT), (1, V_OUT)].into_iter().enumerate() {
        pl.bind(
            stream,
            Box::new(CaptureSink {
                target,
                index: index as u32,
                seen: seen.clone(),
            }),
        )
        .unwrap();
    }
    pl.run().unwrap();
    budgets(&seen)
}

#[test]
fn pipeline_routes_get_the_budget() {
    assert_eq!(run_pipeline(None), vec![(0, (1, 1)), (1, (auto(), auto()))]);
    assert_eq!(run_pipeline(Some(5)), vec![(0, (1, 1)), (1, (5, 5))]);
    assert_eq!(run_pipeline(Some(1)), vec![(0, (1, 1)), (1, (1, 1))]);
}

struct CaptureJobSink(Seen);

impl JobSink for CaptureJobSink {
    fn start(&mut self, _: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _: MediaType, p: &Packet) -> Result<()> {
        self.0.lock().unwrap().push((0, p.data.clone()));
        Ok(())
    }
    fn write_frame(&mut self, _: MediaType, _: &Frame) -> Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn executor_codecs_get_the_job_budget() {
    let src = common::stub::touch("codec_thread_budget");
    let mut ctx = oxideav_core::RuntimeContext::new();
    let mut stub_codecs = CodecRegistry::default();
    common::stub::register(&mut stub_codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);
    ctx.codecs = registry();
    ctx.codecs.register(
        CodecInfo::new(CodecId::new(common::stub::CODEC_ID))
            .capabilities(CodecCapabilities::audio(common::stub::CODEC_ID).with_decode())
            .decoder(make_stub_dec as DecoderFactory),
    );
    let job_json = format!(
        r#"{{
            "@in":  {{"all": [{{"from": "{}"}}]}},
            "@out": {{"audio": [{{"from": "@in", "codec": "{A_OUT}"}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Job::from_json(&job_json).unwrap();
    for (threads, want) in [(4usize, 4u8), (1, 1)] {
        let seen: Seen = Default::default();
        Executor::new(&job, &ctx)
            .with_sink_override("@out", Box::new(CaptureJobSink(seen.clone())))
            .with_threads(threads)
            .run()
            .unwrap();
        assert_eq!(
            budgets(&seen),
            vec![(0, (want, want))],
            "threads = {threads}"
        );
    }
}
