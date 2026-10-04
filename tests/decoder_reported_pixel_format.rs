//! A container that declares no pixel layout leaves the decoder as the
//! only authority on it: [`Decoder::output_pixel_format`] feeds the
//! executor's conversion into the encoder's accepted layouts (without
//! it, a 4:4:4 source muxed without a declared format reached a 4:2:0
//! encoder unconverted).

use std::sync::{Arc, Mutex};

use oxideav_core::registry::CodecInfo;
use oxideav_core::{
    packet::PacketFlags, BytesSource, CodecCapabilities, CodecId, CodecParameters, CodecResolver,
    Decoder, DecoderFactory, Demuxer, Encoder, EncoderFactory, Error, Frame, OpenDemuxerFn, Packet,
    PixelFormat, ReadSeek, Result, RuntimeContext, StreamInfo, TimeBase, VideoFrame, VideoPlane,
};
use oxideav_pipeline::{Executor, Job};

const CODEC: &str = "drpf_raw";
const ENC: &str = "drpf_enc";
const CONTAINER: &str = "drpf_container";
const SCHEME: &str = "drpf";
const PACKETS: u32 = 3;

fn open_bytes(_uri: &str) -> Result<Box<dyn BytesSource>> {
    Ok(Box::new(std::io::Cursor::new(vec![0u8; 64])))
}

fn open_demuxer(_input: Box<dyn ReadSeek>, _c: &dyn CodecResolver) -> Result<Box<dyn Demuxer>> {
    let mut params = CodecParameters::video(CodecId::new(CODEC));
    params.width = Some(2);
    params.height = Some(2);
    // Deliberately no pixel_format: the container does not know it.
    Ok(Box::new(Demux {
        streams: vec![StreamInfo {
            index: 0,
            time_base: TimeBase::new(1, 25),
            duration: Some(PACKETS as i64),
            start_time: Some(0),
            params,
        }],
        next: 0,
    }))
}

struct Demux {
    streams: Vec<StreamInfo>,
    next: u32,
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
            data: vec![100; 4],
        })
    }
    fn seek_to(&mut self, _s: u32, pts: i64) -> Result<i64> {
        Ok(pts.max(0))
    }
}

/// Emits 2×2 Gray8 frames; reports the layout only when `reports`.
struct GrayDecoder {
    reports: bool,
    pending: Option<Packet>,
}

fn make_reporting(_p: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(GrayDecoder {
        reports: true,
        pending: None,
    }))
}

fn make_silent(_p: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(GrayDecoder {
        reports: false,
        pending: None,
    }))
}

impl Decoder for GrayDecoder {
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
        Ok(Frame::Video(VideoFrame {
            pts: p.pts,
            planes: vec![VideoPlane {
                stride: 2,
                data: p.data,
            }],
        }))
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    fn output_pixel_format(&self) -> Option<PixelFormat> {
        self.reports.then_some(PixelFormat::Gray8)
    }
}

/// Records the plane count of every frame it is fed.
struct PlaneCountingEncoder {
    params: CodecParameters,
    seen: Arc<Mutex<Vec<usize>>>,
}

static SEEN: Mutex<Option<Arc<Mutex<Vec<usize>>>>> = Mutex::new(None);

fn make_encoder(p: &CodecParameters) -> Result<Box<dyn Encoder>> {
    let seen = SEEN.lock().unwrap().clone().expect("test installs SEEN");
    Ok(Box::new(PlaneCountingEncoder {
        params: p.clone(),
        seen,
    }))
}

impl Encoder for PlaneCountingEncoder {
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
        self.seen.lock().unwrap().push(v.planes.len());
        Ok(())
    }
    fn receive_packet(&mut self) -> Result<Packet> {
        Err(Error::NeedMore)
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn ctx(decoder: DecoderFactory) -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    ctx.codecs.register(
        CodecInfo::new(CodecId::new(CODEC))
            .capabilities(CodecCapabilities::video("drpf_dec").with_decode())
            .decoder(decoder),
    );
    ctx.codecs.register(
        CodecInfo::new(CodecId::new(ENC))
            .capabilities(
                CodecCapabilities::video("drpf_enc")
                    .with_encode()
                    .with_pixel_formats(vec![PixelFormat::Yuv444P]),
            )
            .encoder(make_encoder as EncoderFactory),
    );
    ctx.containers
        .register_demuxer(CONTAINER, open_demuxer as OpenDemuxerFn);
    ctx.sources.register_bytes(SCHEME, open_bytes);
    ctx.containers.register_extension(SCHEME, CONTAINER);
    ctx
}

/// Plane counts the encoder saw, per frame.
fn run(decoder: DecoderFactory, threads: usize) -> Vec<usize> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    *SEEN.lock().unwrap() = Some(seen.clone());
    let ctx = ctx(decoder);
    let job = Job::from_json(&format!(
        r#"{{"@null":{{"video":[{{"from":"{SCHEME}://x/in.{SCHEME}","codec":"{ENC}"}}]}}}}"#
    ))
    .unwrap();
    Executor::new(&job, &ctx)
        .with_threads(threads)
        .run()
        .unwrap();
    let v = seen.lock().unwrap().clone();
    v
}

#[test]
fn decoder_reported_layout_is_converted_for_the_encoder() {
    // One test drives both cases so the shared SEEN slot is not raced.
    for threads in [1, 2] {
        // Reported Gray8, encoder takes only Yuv444P: converted to
        // three planes.
        let planes = run(make_reporting as DecoderFactory, threads);
        assert_eq!(planes, vec![3; PACKETS as usize], "threads={threads}");
        // Unknown layout: nothing to convert from, frames pass as-is.
        let planes = run(make_silent as DecoderFactory, threads);
        assert_eq!(planes, vec![1; PACKETS as usize], "threads={threads}");
    }
}
