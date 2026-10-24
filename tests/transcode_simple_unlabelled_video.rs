//! `transcode_simple` on a video stream whose pixel layout nobody
//! states up front — the container declares none (WebM, AVI, MPEG-TS,
//! MOV do not) and the decoder answers `None` to
//! `output_pixel_format` — re-encoded into an encoder that cannot be
//! built without one (`rawvideo` into Y4M: "pixel_format is
//! required").
//!
//! The pipeline pre-rolls the decoder to its first picture, reads the
//! layout off its planes, and replays the packets it read. Checked
//! here with an interleaved audio copy stream and a decoder with a
//! reorder delay (the first picture needs three packets): every packet
//! of both streams reaches the muxer exactly once and in source order,
//! and the encoder is built with the inferred layout.

use std::sync::{Arc, Mutex};

use oxideav_core::registry::CodecInfo;
use oxideav_core::{
    CodecCapabilities, CodecId, CodecParameters, CodecRegistry, Decoder, DecoderFactory, Demuxer,
    Encoder, EncoderFactory, Error, Frame, Muxer, Packet, PixelFormat, Result, StreamInfo,
    TimeBase, VideoFrame, VideoPlane, packet::PacketFlags,
};
use oxideav_pipeline::{StreamPlan, transcode_simple};

const VIDEO: &str = "tsuv_video";
const AUDIO: &str = "tsuv_audio";
const RAW: &str = "tsuv_raw";
const W: u32 = 4;
const H: u32 = 2;
const PICTURES: i64 = 6;

struct Interleaved {
    streams: Vec<StreamInfo>,
    remaining: std::collections::VecDeque<(u32, i64)>,
}

impl Interleaved {
    fn new() -> Self {
        let mut video = CodecParameters::video(CodecId::new(VIDEO));
        video.width = Some(W);
        video.height = Some(H);
        // No pixel_format: the container does not know it.
        let mut audio = CodecParameters::audio(CodecId::new(AUDIO));
        audio.sample_rate = Some(8_000);
        audio.channels = Some(1);
        let streams = vec![
            StreamInfo {
                index: 0,
                time_base: TimeBase::new(1, 25),
                duration: Some(PICTURES),
                start_time: Some(0),
                params: video,
            },
            StreamInfo {
                index: 1,
                time_base: TimeBase::new(1, 8_000),
                duration: None,
                start_time: Some(0),
                params: audio,
            },
        ];
        let remaining = (0..PICTURES).flat_map(|i| [(1, i), (0, i)]).collect();
        Self { streams, remaining }
    }
}

impl Demuxer for Interleaved {
    fn format_name(&self) -> &str {
        "tsuv"
    }
    fn streams(&self) -> &[StreamInfo] {
        &self.streams
    }
    fn next_packet(&mut self) -> Result<Packet> {
        let (stream_index, pts) = self.remaining.pop_front().ok_or(Error::Eof)?;
        Ok(Packet {
            stream_index,
            time_base: self.streams[stream_index as usize].time_base,
            pts: Some(pts),
            dts: Some(pts),
            duration: Some(1),
            flags: PacketFlags::default(),
            data: vec![stream_index as u8, pts as u8],
        })
    }
}

/// Decodes a tight 4:2:0 picture per packet after a two-packet
/// reorder delay; never states its layout.
struct Reordering {
    id: CodecId,
    queue: std::collections::VecDeque<i64>,
    flushed: bool,
}

fn make_decoder(_p: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(Reordering {
        id: CodecId::new(VIDEO),
        queue: Default::default(),
        flushed: false,
    }))
}

impl Decoder for Reordering {
    fn codec_id(&self) -> &CodecId {
        &self.id
    }
    fn send_packet(&mut self, p: &Packet) -> Result<()> {
        self.queue.push_back(p.pts.unwrap());
        Ok(())
    }
    fn receive_frame(&mut self) -> Result<Frame> {
        if self.queue.len() <= 2 && !self.flushed {
            return Err(Error::NeedMore);
        }
        let pts = self.queue.pop_front().ok_or(Error::NeedMore)?;
        let plane = |w: usize, h: usize| VideoPlane {
            stride: w,
            data: vec![pts as u8; w * h],
        };
        Ok(Frame::Video(VideoFrame {
            pts: Some(pts),
            planes: vec![plane(4, 2), plane(2, 1), plane(2, 1)],
        }))
    }
    fn flush(&mut self) -> Result<()> {
        self.flushed = true;
        Ok(())
    }
}

/// Needs a layout to be built; emits one packet per picture carrying
/// the picture's pts.
struct Raw(CodecParameters, std::collections::VecDeque<Packet>);

fn make_encoder(p: &CodecParameters) -> Result<Box<dyn Encoder>> {
    if p.pixel_format.is_none() {
        return Err(Error::invalid("tsuv_raw encoder: pixel_format is required"));
    }
    BUILT_WITH.lock().unwrap().push(p.pixel_format.unwrap());
    Ok(Box::new(Raw(p.clone(), Default::default())))
}

static BUILT_WITH: Mutex<Vec<PixelFormat>> = Mutex::new(Vec::new());

impl Encoder for Raw {
    fn codec_id(&self) -> &CodecId {
        &self.0.codec_id
    }
    fn output_params(&self) -> &CodecParameters {
        &self.0
    }
    fn send_frame(&mut self, frame: &Frame) -> Result<()> {
        let Frame::Video(v) = frame else {
            return Err(Error::invalid("video expected"));
        };
        assert_eq!(v.planes.len(), 3, "the encoder sees the decoder's planes");
        self.1.push_back(Packet::new(
            0,
            TimeBase::new(1, 25),
            v.planes[0].data.clone(),
        ));
        Ok(())
    }
    fn receive_packet(&mut self) -> Result<Packet> {
        self.1.pop_front().ok_or(Error::NeedMore)
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct Capture {
    streams: Vec<StreamInfo>,
    /// `(stream_index, first payload byte)` of every packet written.
    packets: Vec<(u32, u8)>,
}

struct CaptureMuxer(Arc<Mutex<Capture>>);

impl Muxer for CaptureMuxer {
    fn format_name(&self) -> &str {
        "capture"
    }
    fn write_header(&mut self) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, packet: &Packet) -> Result<()> {
        self.0
            .lock()
            .unwrap()
            .packets
            .push((packet.stream_index, packet.data[0]));
        Ok(())
    }
    fn write_trailer(&mut self) -> Result<()> {
        Ok(())
    }
}

fn codecs() -> CodecRegistry {
    let mut reg = CodecRegistry::new();
    reg.register(
        CodecInfo::new(CodecId::new(VIDEO))
            .capabilities(CodecCapabilities::video("tsuv_dec"))
            .decoder(make_decoder as DecoderFactory),
    );
    reg.register(
        CodecInfo::new(CodecId::new(RAW))
            .capabilities(CodecCapabilities::video("tsuv_raw"))
            .encoder(make_encoder as EncoderFactory),
    );
    reg
}

#[test]
fn an_unlabelled_video_stream_is_pre_rolled_and_every_packet_lands_once() {
    let capture = Arc::new(Mutex::new(Capture::default()));
    let mut demuxer = Interleaved::new();
    let codecs = codecs();
    let sink = capture.clone();
    let stats = transcode_simple(
        &mut demuxer,
        move |streams| {
            sink.lock().unwrap().streams = streams.to_vec();
            Ok(Box::new(CaptureMuxer(sink)) as Box<dyn Muxer>)
        },
        &codecs,
        |s| {
            Ok(if s.index == 0 {
                StreamPlan::Reencode {
                    output_codec: RAW.to_string(),
                }
            } else {
                StreamPlan::Copy
            })
        },
    )
    .expect("transcode");

    // The encoder was built with the layout read off the first
    // picture, and the muxer was told about it.
    assert_eq!(
        BUILT_WITH.lock().unwrap().as_slice(),
        &[PixelFormat::Yuv420P]
    );
    let cap = capture.lock().unwrap();
    assert_eq!(
        cap.streams[0].params.pixel_format,
        Some(PixelFormat::Yuv420P)
    );
    // Every packet of the source was read exactly once …
    assert_eq!(stats.packets_in, 2 * PICTURES as u64);
    assert_eq!(stats.frames_decoded, PICTURES as u64);
    // … every picture came out once, and the copied audio stream is
    // intact and in order — the three video packets the pre-roll
    // consumed, and the three audio packets it read past, included.
    let video: Vec<u8> = cap
        .packets
        .iter()
        .filter(|(s, _)| *s == 0)
        .map(|(_, b)| *b)
        .collect();
    let audio: Vec<u8> = cap
        .packets
        .iter()
        .filter(|(s, _)| *s == 1)
        .map(|(_, b)| *b)
        .collect();
    assert_eq!(video, (0..PICTURES as u8).collect::<Vec<_>>());
    assert_eq!(audio, vec![1; PICTURES as usize]);
    assert_eq!(cap.packets.len(), 2 * PICTURES as usize);
}
