//! Executor-side codec construction:
//!
//! * the encoder never inherits the demuxer's `options` bag — MP4
//!   tracks carry `elst_*` / `vmhd_*` / `btrt_*` container metadata
//!   there, and a strict encoder option parser rejected it
//!   (`oxideav convert in.mp4 out.png` → "unknown option
//!   'elst_entry_count'");
//! * [`Executor::with_codec_preferences`] reaches decoder selection:
//!   a hardware decoder that outranks the software one is skipped
//!   under `no_hardware` (the CLI's `--no-hwaccel` used to be ignored
//!   by `run` / `convert`).

use std::sync::{Arc, Mutex};

use oxideav_core::registry::CodecInfo;
use oxideav_core::{
    packet::PacketFlags, BytesSource, CodecCapabilities, CodecId, CodecParameters, CodecResolver,
    Decoder, DecoderFactory, Demuxer, Encoder, EncoderFactory, Error, Frame, MediaType,
    OpenDemuxerFn, Packet, PixelFormat, ReadSeek, Result, RuntimeContext, StreamInfo, TimeBase,
    VideoFrame, VideoPlane,
};
use oxideav_pipeline::{CodecPreferences, Executor, Job, JobSink};

const CODEC: &str = "csep_raw";
const ENC: &str = "csep_enc";
const CONTAINER: &str = "csep_container";
const SCHEME: &str = "csep";
const PACKETS: u32 = 6;

fn open_bytes(_uri: &str) -> Result<Box<dyn BytesSource>> {
    Ok(Box::new(std::io::Cursor::new(vec![0u8; 64])))
}

fn open_demuxer(_input: Box<dyn ReadSeek>, _c: &dyn CodecResolver) -> Result<Box<dyn Demuxer>> {
    let mut params = CodecParameters::video(CodecId::new(CODEC));
    params.width = Some(2);
    params.height = Some(2);
    params.pixel_format = Some(PixelFormat::Gray8);
    // What the MP4 demuxer surfaces for an edited track.
    params.options.insert("elst_entry_count", "1");
    params.options.insert("vmhd_graphicsmode", "0");
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
            data: vec![pts as u8; 4],
        })
    }
    fn seek_to(&mut self, _s: u32, pts: i64) -> Result<i64> {
        Ok(pts.max(0))
    }
}

/// Software decoder: one 2×2 gray frame per packet.
struct SwDecoder {
    pending: Option<Packet>,
}

fn make_sw(_p: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(SwDecoder { pending: None }))
}

impl Decoder for SwDecoder {
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
}

/// "Hardware" decoder that accepts every packet and emits nothing —
/// the shape of a backend fed a bitstream framing it cannot parse.
struct MuteDecoder;

fn make_mute(_p: &CodecParameters) -> Result<Box<dyn Decoder>> {
    Ok(Box::new(MuteDecoder))
}

impl Decoder for MuteDecoder {
    fn codec_id(&self) -> &CodecId {
        static ID: std::sync::OnceLock<CodecId> = std::sync::OnceLock::new();
        ID.get_or_init(|| CodecId::new(CODEC))
    }
    fn send_packet(&mut self, _packet: &Packet) -> Result<()> {
        Ok(())
    }
    fn receive_frame(&mut self) -> Result<Frame> {
        Err(Error::NeedMore)
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Encoder whose construction parses its options strictly (no key is
/// declared), like every `parse_options`-based encoder.
struct StrictEncoder {
    params: CodecParameters,
    out: Vec<Packet>,
}

fn make_strict(p: &CodecParameters) -> Result<Box<dyn Encoder>> {
    if let Some((k, _)) = p.options.iter().next() {
        return Err(Error::invalid(format!("unknown option '{k}'")));
    }
    Ok(Box::new(StrictEncoder {
        params: p.clone(),
        out: Vec::new(),
    }))
}

impl Encoder for StrictEncoder {
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
        let mut p = Packet::new(0, TimeBase::new(1, 25), v.planes[0].data.clone());
        p.pts = v.pts;
        self.out.push(p);
        Ok(())
    }
    fn receive_packet(&mut self) -> Result<Packet> {
        if self.out.is_empty() {
            Err(Error::NeedMore)
        } else {
            Ok(self.out.remove(0))
        }
    }
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

fn ctx() -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    ctx.codecs.register(
        CodecInfo::new(CodecId::new(CODEC))
            .capabilities(CodecCapabilities::video("csep_sw").with_decode())
            .decoder(make_sw as DecoderFactory),
    );
    ctx.codecs.register(
        CodecInfo::new(CodecId::new(CODEC))
            .capabilities(
                CodecCapabilities::video("csep_hw")
                    .with_decode()
                    .with_hardware(true)
                    .with_priority(5),
            )
            .decoder(make_mute as DecoderFactory),
    );
    ctx.codecs.register(
        CodecInfo::new(CodecId::new(ENC))
            .capabilities(CodecCapabilities::video("csep_enc").with_encode())
            .encoder(make_strict as EncoderFactory),
    );
    ctx.containers
        .register_demuxer(CONTAINER, open_demuxer as OpenDemuxerFn);
    ctx.sources.register_bytes(SCHEME, open_bytes);
    ctx.containers.register_extension(SCHEME, CONTAINER);
    ctx
}

#[derive(Default)]
struct Seen {
    packets: usize,
}

struct CountingSink(Arc<Mutex<Seen>>);

impl JobSink for CountingSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        self.0.lock().unwrap().packets += 1;
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// `(frames decoded, packets the sink received)`.
fn run(prefs: CodecPreferences, threads: usize) -> Result<(u64, usize)> {
    let ctx = ctx();
    let job = Job::from_json(&format!(
        r#"{{"@out":{{"video":[{{"from":"{SCHEME}://x/in.{SCHEME}","codec":"{ENC}"}}]}}}}"#
    ))?;
    let seen = Arc::new(Mutex::new(Seen::default()));
    let stats = Executor::new(&job, &ctx)
        .with_threads(threads)
        .with_codec_preferences(prefs)
        .with_sink_override("@out", Box::new(CountingSink(seen.clone())))
        .run()?;
    let packets = seen.lock().unwrap().packets;
    Ok((stats.frames_decoded, packets))
}

fn software_only() -> CodecPreferences {
    CodecPreferences {
        no_hardware: true,
        ..Default::default()
    }
}

#[test]
fn encoder_is_built_without_the_demuxers_option_bag() {
    for threads in [1, 2] {
        let (frames, packets) =
            run(software_only(), threads).unwrap_or_else(|e| panic!("threads={threads}: {e}"));
        assert_eq!(frames, PACKETS as u64, "threads={threads}");
        assert_eq!(packets, PACKETS as usize, "threads={threads}");
    }
}

#[test]
fn codec_preferences_reach_decoder_selection() {
    for threads in [1, 2] {
        // Default preferences: the priority-5 hardware decoder wins
        // and (being mute) decodes nothing.
        let (frames, _) = run(CodecPreferences::default(), threads).unwrap();
        assert_eq!(frames, 0, "threads={threads}: hardware decoder selected");
        // no_hardware: the software decoder is used.
        let (frames, _) = run(software_only(), threads).unwrap();
        assert_eq!(frames, PACKETS as u64, "threads={threads}");
    }
}
