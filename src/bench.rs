//! Per-codec micro-benchmarks.
//!
//! For a given codec id, walks every registered implementation and
//! times encode + decode through each backend. Used by `oxideav bench`
//! to compare HW (videotoolbox / audiotoolbox / future VAAPI / NVENC)
//! against the pure-Rust path and to spot codecs whose SW
//! implementation is too slow for real-time use.
//!
//! Methodology:
//! - Synthesise a small test signal (gradient frames for video, sine
//!   wave for audio).
//! - **Encode side**: feed the signal directly through each
//!   encoder-side impl, measure wall-clock time, report
//!   frames/second.
//! - **Decode side**: encode the signal once through the *first*
//!   available encoder (so every decoder sees the same bitstream),
//!   then feed those packets through each decoder-side impl. If no
//!   encoder is registered the decode bench is skipped with an
//!   "encode-only path unavailable" note.
//! - HW init failures (`VTCompressionSessionCreate` returning a
//!   non-zero `OSStatus`, etc.) are caught and surfaced in the
//!   `error` field instead of crashing the bench.
//!
//! `run_bench_all` walks every codec id with at least one registered
//! impl, grouping by `MediaType` (video first, then audio).

use std::time::Instant;

use oxideav_core::{
    AudioFrame, CodecId, CodecParameters, CodecRegistry, Frame, MediaType, Packet, PixelFormat,
    Rational, SampleFormat, VideoFrame, VideoPlane,
};

/// Tunables for a single bench run.
#[derive(Clone, Debug)]
pub struct BenchOpts {
    /// Number of UNIQUE source frames synthesised once, up-front, in
    /// the preparation step. The bench loop cycles through this set
    /// until the time budget runs out. 500 frames covers ≈ 16.7 s
    /// of video at 30 fps / ≈ 10.7 s of audio (1024-sample frames at
    /// 48 kHz) — plenty of variety so a single short bench window
    /// rarely repeats the same input.
    pub prep_frames: u32,
    /// Wall-clock seconds the bench loop runs for. The session is
    /// created once, then frames cycle through until elapsed >=
    /// `bench_duration_secs`; total frames processed / elapsed gives
    /// the reported fps. 3 s is a good default — fast HW encoders
    /// converge well before then; SW encoders stragglers stop early
    /// on the first cycle. Bigger = more stable numbers but slower
    /// `--all` runs.
    pub bench_duration_secs: f64,
    pub width: u32,
    pub height: u32,
    /// Video pixel format. Defaults to `Yuv420P`. Encoders that don't
    /// accept this format are skipped with a clear error.
    pub pix_fmt: PixelFormat,
    /// Frame rate stored on the encoder's parameters (affects rate
    /// control). Doesn't affect bench loop timing.
    pub fps_num: i32,
    pub fps_den: i32,
    pub bitrate_video: u64,
    pub sample_rate: u32,
    pub channels: u16,
    pub sample_format: SampleFormat,
    pub bitrate_audio: u64,
    /// `Side::Decode` skips the encode bench, etc.
    pub side: Side,
}

impl Default for BenchOpts {
    fn default() -> Self {
        Self {
            prep_frames: 500,
            bench_duration_secs: 3.0,
            width: 1920,
            height: 1080,
            pix_fmt: PixelFormat::Yuv420P,
            fps_num: 30,
            fps_den: 1,
            bitrate_video: 5_000_000,
            sample_rate: 48_000,
            channels: 2,
            sample_format: SampleFormat::F32,
            bitrate_audio: 128_000,
            side: Side::Both,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Decode,
    Encode,
    Both,
}

/// Result of one (codec_id × backend × side) bench cell.
#[derive(Clone, Debug)]
pub struct BenchResult {
    pub codec_id: String,
    pub backend: String,
    pub side: BenchSide,
    pub media_type: MediaType,
    pub hw: bool,
    pub priority: i32,
    /// Frames per second measured on the bench loop. `None` when
    /// `error` is set.
    pub fps: Option<f64>,
    /// Realtime ratio: `fps / nominal_fps`. Video uses `fps_num/fps_den`;
    /// audio computes per-codec-frame-equivalent assuming ~1024 samples
    /// per frame at `sample_rate`. `None` when `fps` is `None`.
    pub realtime: Option<f64>,
    /// Set when the impl couldn't be benched (init failure, format
    /// mismatch, no encoder available for the decode-side stream, etc).
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchSide {
    Decode,
    Encode,
}

impl std::fmt::Display for BenchSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            BenchSide::Decode => "decode",
            BenchSide::Encode => "encode",
        })
    }
}

/// Bench every implementation of `codec_id`. Returns a vector of
/// per-(backend, side) results. An empty vector means the codec id
/// isn't registered.
pub fn run_bench(reg: &CodecRegistry, codec_id: &str, opts: &BenchOpts) -> Vec<BenchResult> {
    let id = CodecId::new(codec_id);
    let impls = reg.implementations(&id);
    if impls.is_empty() {
        return Vec::new();
    }
    let media_type = impls[0].caps.media_type;
    let mut results = Vec::new();

    match media_type {
        MediaType::Video => bench_video_codec(&id, impls, opts, &mut results),
        MediaType::Audio => bench_audio_codec(&id, impls, opts, &mut results),
        _ => {
            // Subtitles / data — no time-domain throughput meaningful.
            results.push(BenchResult {
                codec_id: codec_id.to_owned(),
                backend: String::new(),
                side: BenchSide::Decode,
                media_type,
                hw: false,
                priority: 0,
                fps: None,
                realtime: None,
                error: Some(format!("bench unsupported for {media_type:?} codecs")),
            });
        }
    }

    results
}

/// Bench every video + audio codec id registered.
pub fn run_bench_all(reg: &CodecRegistry, opts: &BenchOpts) -> Vec<BenchResult> {
    let mut seen = std::collections::BTreeSet::new();
    let mut ids: Vec<(MediaType, String)> = Vec::new();
    for (id, im) in reg.all_implementations() {
        let mt = im.caps.media_type;
        if mt != MediaType::Video && mt != MediaType::Audio {
            continue;
        }
        if seen.insert(id.as_str().to_owned()) {
            ids.push((mt, id.as_str().to_owned()));
        }
    }
    // Video first, then audio; alphabetical inside each group.
    ids.sort_by(|a, b| match (a.0, b.0) {
        (MediaType::Video, MediaType::Audio) => std::cmp::Ordering::Less,
        (MediaType::Audio, MediaType::Video) => std::cmp::Ordering::Greater,
        _ => a.1.cmp(&b.1),
    });

    let mut all = Vec::new();
    for (_, id) in ids {
        all.extend(run_bench(reg, &id, opts));
    }
    all
}

// --- video ---

fn bench_video_codec(
    id: &CodecId,
    impls: &[oxideav_core::CodecImplementation],
    opts: &BenchOpts,
    results: &mut Vec<BenchResult>,
) {
    let params = video_params(id, opts);
    let frames = video_frames(opts);

    let video_frames_owned: Vec<Frame> = frames.iter().cloned().map(Frame::Video).collect();
    let nominal = opts.fps_num as f64 / opts.fps_den as f64;
    if opts.side != Side::Decode {
        for imp in impls.iter().filter(|i| i.caps.encode) {
            results.push(time_encode(
                id,
                imp,
                &params,
                &video_frames_owned,
                opts.bench_duration_secs,
                nominal,
            ));
        }
    }

    if opts.side != Side::Encode {
        let stream = match self_encode_video(impls, &params, &frames) {
            Ok(s) => s,
            Err(e) => {
                results.push(BenchResult {
                    codec_id: id.as_str().to_owned(),
                    backend: String::new(),
                    side: BenchSide::Decode,
                    media_type: MediaType::Video,
                    hw: false,
                    priority: 0,
                    fps: None,
                    realtime: None,
                    error: Some(format!("can't synth decode stream: {e}")),
                });
                return;
            }
        };
        for imp in impls.iter().filter(|i| i.caps.decode) {
            results.push(time_decode(
                id,
                imp,
                &params,
                &stream,
                opts.bench_duration_secs,
                nominal,
            ));
        }
    }
}

fn video_params(id: &CodecId, opts: &BenchOpts) -> CodecParameters {
    let mut p = CodecParameters::video(id.clone());
    p.width = Some(opts.width);
    p.height = Some(opts.height);
    p.pixel_format = Some(opts.pix_fmt);
    p.bit_rate = Some(opts.bitrate_video);
    p.frame_rate = Some(Rational::new(opts.fps_num as i64, opts.fps_den as i64));
    p
}

fn video_frames(opts: &BenchOpts) -> Vec<VideoFrame> {
    let w = opts.width as usize;
    let h = opts.height as usize;
    let cw = w / 2;
    let ch = h / 2;
    let mut out = Vec::with_capacity(opts.prep_frames as usize);
    for t in 0..opts.prep_frames {
        // Y plane: animated gradient — pixels move across rows each frame.
        let mut y = vec![0u8; w * h];
        for row in 0..h {
            for col in 0..w {
                y[row * w + col] = ((col + row + t as usize * 4) & 0xFF) as u8;
            }
        }
        let mut u = vec![128u8; cw * ch];
        let mut v = vec![128u8; cw * ch];
        for row in 0..ch {
            for col in 0..cw {
                u[row * cw + col] = ((col + t as usize) & 0xFF) as u8;
                v[row * cw + col] = ((row + t as usize) & 0xFF) as u8;
            }
        }
        out.push(VideoFrame {
            pts: Some(t as i64),
            planes: vec![
                VideoPlane { stride: w, data: y },
                VideoPlane {
                    stride: cw,
                    data: u,
                },
                VideoPlane {
                    stride: cw,
                    data: v,
                },
            ],
        });
    }
    out
}

fn self_encode_video(
    impls: &[oxideav_core::CodecImplementation],
    params: &CodecParameters,
    frames: &[VideoFrame],
) -> Result<Vec<Packet>, oxideav_core::Error> {
    let imp = impls
        .iter()
        .find(|i| i.caps.encode && i.make_encoder.is_some())
        .ok_or_else(|| {
            oxideav_core::Error::CodecNotFound(format!(
                "decode bench needs at least one encoder for {}",
                params.codec_id
            ))
        })?;
    let mut enc = (imp.make_encoder.unwrap())(params)?;
    let mut packets = Vec::new();
    for f in frames {
        enc.send_frame(&Frame::Video(f.clone()))?;
        loop {
            match enc.receive_packet() {
                Ok(p) => packets.push(p),
                Err(oxideav_core::Error::NeedMore) | Err(oxideav_core::Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }
    }
    enc.flush()?;
    loop {
        match enc.receive_packet() {
            Ok(p) => packets.push(p),
            Err(oxideav_core::Error::NeedMore) | Err(oxideav_core::Error::Eof) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(packets)
}

// --- audio ---

fn bench_audio_codec(
    id: &CodecId,
    impls: &[oxideav_core::CodecImplementation],
    opts: &BenchOpts,
    results: &mut Vec<BenchResult>,
) {
    let params = audio_params(id, opts);
    let frames = audio_frames(opts);

    let nominal_fps =
        opts.sample_rate as f64 / frames.first().map(|f| f.samples).unwrap_or(1024) as f64;
    let audio_frames_owned: Vec<Frame> = frames.iter().cloned().map(Frame::Audio).collect();

    if opts.side != Side::Decode {
        for imp in impls.iter().filter(|i| i.caps.encode) {
            results.push(time_encode(
                id,
                imp,
                &params,
                &audio_frames_owned,
                opts.bench_duration_secs,
                nominal_fps,
            ));
        }
    }

    if opts.side != Side::Encode {
        let stream = match self_encode_audio(impls, &params, &frames) {
            Ok(s) => s,
            Err(e) => {
                results.push(BenchResult {
                    codec_id: id.as_str().to_owned(),
                    backend: String::new(),
                    side: BenchSide::Decode,
                    media_type: MediaType::Audio,
                    hw: false,
                    priority: 0,
                    fps: None,
                    realtime: None,
                    error: Some(format!("can't synth decode stream: {e}")),
                });
                return;
            }
        };
        for imp in impls.iter().filter(|i| i.caps.decode) {
            results.push(time_decode(
                id,
                imp,
                &params,
                &stream,
                opts.bench_duration_secs,
                nominal_fps,
            ));
        }
    }
}

fn audio_params(id: &CodecId, opts: &BenchOpts) -> CodecParameters {
    let mut p = CodecParameters::audio(id.clone());
    p.sample_rate = Some(opts.sample_rate);
    p.channels = Some(opts.channels);
    p.sample_format = Some(opts.sample_format);
    p.bit_rate = Some(opts.bitrate_audio);
    p
}

fn audio_frames(opts: &BenchOpts) -> Vec<AudioFrame> {
    // 1024-sample frames (close to AAC's natural size) for `frames`
    // total. Sine wave at 440 Hz, F32 interleaved.
    const FRAME_SAMPLES: u32 = 1024;
    let bytes_per_sample = match opts.sample_format {
        SampleFormat::U8 => 1,
        SampleFormat::S16 => 2,
        SampleFormat::S32 | SampleFormat::F32 => 4,
        SampleFormat::F64 => 8,
        _ => 4,
    };
    let mut out = Vec::with_capacity(opts.prep_frames as usize);
    let mut t: f32 = 0.0;
    let dt = 1.0 / opts.sample_rate as f32;
    for fr in 0..opts.prep_frames {
        let mut buf =
            vec![0u8; (FRAME_SAMPLES as usize) * (opts.channels as usize) * bytes_per_sample];
        for s in 0..FRAME_SAMPLES as usize {
            let v = (2.0 * std::f32::consts::PI * 440.0 * t).sin() * 0.25;
            for ch in 0..opts.channels as usize {
                let idx = (s * opts.channels as usize + ch) * bytes_per_sample;
                match opts.sample_format {
                    SampleFormat::F32 => {
                        buf[idx..idx + 4].copy_from_slice(&v.to_le_bytes());
                    }
                    SampleFormat::S16 => {
                        let i = (v * i16::MAX as f32) as i16;
                        buf[idx..idx + 2].copy_from_slice(&i.to_le_bytes());
                    }
                    SampleFormat::S32 => {
                        let i = (v * i32::MAX as f32) as i32;
                        buf[idx..idx + 4].copy_from_slice(&i.to_le_bytes());
                    }
                    SampleFormat::U8 => {
                        buf[idx] = ((v + 1.0) * 127.5) as u8;
                    }
                    _ => {}
                }
            }
            t += dt;
        }
        out.push(AudioFrame {
            samples: FRAME_SAMPLES,
            pts: Some(fr as i64 * FRAME_SAMPLES as i64),
            data: vec![buf],
        });
    }
    out
}

fn self_encode_audio(
    impls: &[oxideav_core::CodecImplementation],
    params: &CodecParameters,
    frames: &[AudioFrame],
) -> Result<Vec<Packet>, oxideav_core::Error> {
    let imp = impls
        .iter()
        .find(|i| i.caps.encode && i.make_encoder.is_some())
        .ok_or_else(|| {
            oxideav_core::Error::CodecNotFound(format!(
                "decode bench needs at least one encoder for {}",
                params.codec_id
            ))
        })?;
    let mut enc = (imp.make_encoder.unwrap())(params)?;
    let mut packets = Vec::new();
    for f in frames {
        enc.send_frame(&Frame::Audio(f.clone()))?;
        loop {
            match enc.receive_packet() {
                Ok(p) => packets.push(p),
                Err(oxideav_core::Error::NeedMore) | Err(oxideav_core::Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }
    }
    enc.flush()?;
    loop {
        match enc.receive_packet() {
            Ok(p) => packets.push(p),
            Err(oxideav_core::Error::NeedMore) | Err(oxideav_core::Error::Eof) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(packets)
}

// --- timing ---

fn time_encode(
    id: &CodecId,
    imp: &oxideav_core::CodecImplementation,
    params: &CodecParameters,
    prep: &[Frame],
    duration_secs: f64,
    nominal_fps: f64,
) -> BenchResult {
    let factory = match imp.make_encoder {
        Some(f) => f,
        None => {
            return error_result(id, imp, BenchSide::Encode, "no encoder factory".into());
        }
    };
    let mut enc = match factory(params) {
        Ok(e) => e,
        Err(e) => return error_result(id, imp, BenchSide::Encode, format!("init: {e}")),
    };
    if prep.is_empty() {
        return error_result(id, imp, BenchSide::Encode, "no prep frames".into());
    }
    let mut sent: u64 = 0;
    let start = Instant::now();
    while start.elapsed().as_secs_f64() < duration_secs {
        let f = &prep[(sent as usize) % prep.len()];
        if let Err(e) = enc.send_frame(f) {
            return error_result(id, imp, BenchSide::Encode, format!("send_frame: {e}"));
        }
        sent += 1;
        loop {
            match enc.receive_packet() {
                Ok(_) => {}
                Err(oxideav_core::Error::NeedMore) | Err(oxideav_core::Error::Eof) => break,
                Err(e) => return error_result(id, imp, BenchSide::Encode, format!("recv: {e}")),
            }
        }
    }
    let _ = enc.flush();
    while enc.receive_packet().is_ok() {}
    let elapsed = start.elapsed().as_secs_f64();
    let fps = if elapsed > 0.0 {
        sent as f64 / elapsed
    } else {
        0.0
    };
    BenchResult {
        codec_id: id.as_str().to_owned(),
        backend: imp.caps.implementation.clone(),
        side: BenchSide::Encode,
        media_type: imp.caps.media_type,
        hw: imp.caps.hardware_accelerated,
        priority: imp.caps.priority,
        fps: Some(fps),
        realtime: if nominal_fps > 0.0 {
            Some(fps / nominal_fps)
        } else {
            None
        },
        error: None,
    }
}

fn time_decode(
    id: &CodecId,
    imp: &oxideav_core::CodecImplementation,
    params: &CodecParameters,
    stream: &[Packet],
    duration_secs: f64,
    nominal_fps: f64,
) -> BenchResult {
    let factory = match imp.make_decoder {
        Some(f) => f,
        None => {
            return error_result(id, imp, BenchSide::Decode, "no decoder factory".into());
        }
    };
    let mut dec = match factory(params) {
        Ok(d) => d,
        Err(e) => return error_result(id, imp, BenchSide::Decode, format!("init: {e}")),
    };
    if stream.is_empty() {
        return error_result(id, imp, BenchSide::Decode, "empty prep stream".into());
    }
    let mut frames_out: u64 = 0;
    let mut sent: u64 = 0;
    let start = Instant::now();
    while start.elapsed().as_secs_f64() < duration_secs {
        let p = &stream[(sent as usize) % stream.len()];
        if let Err(e) = dec.send_packet(p) {
            return error_result(id, imp, BenchSide::Decode, format!("send_packet: {e}"));
        }
        sent += 1;
        loop {
            match dec.receive_frame() {
                Ok(_) => frames_out += 1,
                Err(oxideav_core::Error::NeedMore) | Err(oxideav_core::Error::Eof) => break,
                Err(e) => return error_result(id, imp, BenchSide::Decode, format!("recv: {e}")),
            }
        }
    }
    let _ = dec.flush();
    while dec.receive_frame().is_ok() {
        frames_out += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let fps = if elapsed > 0.0 {
        frames_out as f64 / elapsed
    } else {
        0.0
    };
    BenchResult {
        codec_id: id.as_str().to_owned(),
        backend: imp.caps.implementation.clone(),
        side: BenchSide::Decode,
        media_type: imp.caps.media_type,
        hw: imp.caps.hardware_accelerated,
        priority: imp.caps.priority,
        fps: Some(fps),
        realtime: if nominal_fps > 0.0 {
            Some(fps / nominal_fps)
        } else {
            None
        },
        error: None,
    }
}

fn error_result(
    id: &CodecId,
    imp: &oxideav_core::CodecImplementation,
    side: BenchSide,
    msg: String,
) -> BenchResult {
    BenchResult {
        codec_id: id.as_str().to_owned(),
        backend: imp.caps.implementation.clone(),
        side,
        media_type: imp.caps.media_type,
        hw: imp.caps.hardware_accelerated,
        priority: imp.caps.priority,
        fps: None,
        realtime: None,
        error: Some(msg),
    }
}
