//! Pixel-format negotiation between a decoded video stream and the
//! encoder + muxer it is written through.
//!
//! A conversion is planned whenever the layout the frames arrive in
//! cannot be taken as-is by either end:
//!
//! * the **encoder**, through the accepted layouts its implementation
//!   declares ([`CodecCapabilities::accepted_pixel_formats`](oxideav_core::CodecCapabilities));
//! * the **muxer**, which may only be able to describe some layouts —
//!   a raw-picture container (Y4M carries planar YUV / gray only, no
//!   RGB) learns the layout from the encoder's output parameters.
//!   Muxers declare nothing up front, so acceptance is discovered the
//!   way [`crate::stream_fit`] discovers stream capacity: open the
//!   muxer on a scratch buffer with the stream the encoder would
//!   publish and see whether it refuses.
//!
//! Candidates are tried in order and the first one both ends take
//! wins:
//!
//! * an encoder that declares accepted layouts keeps its own preference
//!   order (the historical "convert to the first accepted layout"
//!   behaviour, unchanged whenever the muxer takes that layout);
//! * an encoder that takes anything (rawvideo) is offered every layout
//!   the pixel-format converter can reach from the source, cheapest
//!   loss first — RGB → planar 4:4:4 YUV before 4:2:0, 10-bit before
//!   8-bit for a 10-bit source, gray only for gray sources, …
//!
//! When no candidate satisfies both, the encoder's constraint alone
//! decides (so the muxer reports its own refusal).

use std::io::Cursor;

use oxideav_core::{
    CodecId, CodecParameters, CodecRegistry, ContainerRegistry, PixelFormat, StreamInfo, TimeBase,
    WriteSeek,
};
use oxideav_pixfmt::FormatInfo;

use crate::dag::codec_accepted_pixel_formats;
use crate::selection::{make_encoder_with, CodecPreferences};

/// The container a track is muxed into, for acceptance probing.
#[derive(Clone, Copy)]
pub(crate) struct MuxProbe<'a> {
    pub(crate) containers: &'a ContainerRegistry,
    pub(crate) container: &'a str,
}

/// Every layout considered as a conversion target when the encoder
/// takes any layout.
const ALL: &[PixelFormat] = {
    use PixelFormat::*;
    &[
        Yuv420P,
        Yuv422P,
        Yuv444P,
        Rgb24,
        Rgba,
        Gray8,
        Pal8,
        Bgr24,
        Bgra,
        Argb,
        Abgr,
        Rgb48Le,
        Rgba64Le,
        Gray16Le,
        Gray10Le,
        Gray12Le,
        Yuv420P10Le,
        Yuv422P10Le,
        Yuv444P10Le,
        Yuv420P12Le,
        Yuv422P12Le,
        Yuv444P12Le,
        YuvJ420P,
        YuvJ422P,
        YuvJ444P,
        Nv12,
        Nv21,
        Ya8,
        Yuva420P,
        MonoBlack,
        MonoWhite,
        Yuyv422,
        Uyvy422,
        Cmyk,
        Yuv411P,
        Gbrp10Le,
        Gbrap10Le,
        Gbrp12Le,
        Gbrap12Le,
        Gbrp14Le,
        Gbrap14Le,
        Yuv420P16Le,
        Yuv422P16Le,
        Yuv444P16Le,
        Yuva422P,
        Yuva444P,
        Yuva422P10Le,
        Yuva422P12Le,
        Yuva444P10Le,
        Yuva444P12Le,
        Yuva422P16Le,
        Yuva444P16Le,
        Gbrp8,
        Gbrp16Le,
        Gbrap16Le,
        Yuva420P10Le,
        Yuva420P12Le,
        Yuva420P16Le,
        Gbrap8,
        Ya16Le,
        CmykInverted,
        Yuv440P,
        Yuv440P10Le,
        Yuv440P12Le,
        Yuv440P16Le,
        GrayF32Le,
        RgbF32Le,
        RgbaF32Le,
        GbrpF32Le,
        GbrapF32Le,
    ]
};

/// What a layout can carry, as far as target ranking cares.
#[derive(Clone, Copy)]
struct Shape {
    mono: bool,
    gray: bool,
    palette: bool,
    alpha: bool,
    depth: u8,
    /// Chroma samples per luma sample, inverted: 1 for 4:4:4 / RGB,
    /// 2 for 4:2:2, 4 for 4:2:0 / 4:1:1.
    chroma_div: u8,
    rgb: bool,
}

fn shape(f: PixelFormat) -> Shape {
    use PixelFormat::*;
    let info = FormatInfo::of(f);
    let mono = matches!(f, MonoBlack | MonoWhite);
    let gray = mono
        || matches!(
            f,
            Gray8 | Gray10Le | Gray12Le | Gray16Le | GrayF32Le | Ya8 | Ya16Le
        );
    let rgb = matches!(
        f,
        Rgb24
            | Rgba
            | Bgr24
            | Bgra
            | Argb
            | Abgr
            | Rgb48Le
            | Rgba64Le
            | Gbrp8
            | Gbrap8
            | Gbrp10Le
            | Gbrap10Le
            | Gbrp12Le
            | Gbrap12Le
            | Gbrp14Le
            | Gbrap14Le
            | Gbrp16Le
            | Gbrap16Le
            | RgbF32Le
            | RgbaF32Le
            | GbrpF32Le
            | GbrapF32Le
            | Cmyk
            | CmykInverted
    );
    let chroma_div = if gray || rgb || info.is_palette {
        1
    } else {
        info.chroma_w_sub.max(1) * info.chroma_h_sub.max(1)
    };
    Shape {
        mono,
        gray,
        palette: info.is_palette,
        alpha: info.has_alpha,
        depth: if mono { 1 } else { info.bit_depth },
        chroma_div,
        rgb,
    }
}

/// Information lost converting `src` to `dst`, as an ordered penalty:
/// colour → bilevel ≫ colour → palette ≫ colour → gray ≫ alpha ≫
/// bits of depth ≫ chroma resolution ≫ a change of colour model.
fn loss(src: Shape, dst: Shape) -> u32 {
    let mut l = 0;
    if dst.mono && !src.mono {
        l += 40_000;
    } else if dst.palette && !src.palette {
        l += 20_000;
    }
    if dst.gray && !src.gray {
        l += 10_000;
    }
    if src.alpha && !dst.alpha {
        l += 1_000;
    }
    if dst.depth < src.depth {
        l += 50 * u32::from(src.depth - dst.depth);
    }
    if !src.gray && !dst.gray && dst.chroma_div > src.chroma_div {
        l += 10 * u32::from(dst.chroma_div - src.chroma_div);
    }
    if !src.gray && !dst.gray && src.rgb != dst.rgb {
        l += 2;
    }
    l
}

/// Every layout reachable from `src`, cheapest loss first (then the
/// smallest storage, then table order). `src` itself is excluded.
fn ranked_targets(src: PixelFormat) -> Vec<PixelFormat> {
    let s = shape(src);
    let mut v: Vec<(u32, u32, usize, PixelFormat)> = ALL
        .iter()
        .enumerate()
        .filter(|(_, &f)| f != src && oxideav_pixfmt::supports(src, f))
        .map(|(i, &f)| (loss(s, shape(f)), f.bits_per_pixel_approx(), i, f))
        .collect();
    v.sort_unstable_by_key(|&(l, bpp, i, _)| (l, bpp, i));
    v.into_iter().map(|(.., f)| f).collect()
}

/// Whether `probe`'s muxer opens with the stream the `codec` encoder
/// publishes when fed `running`-shaped frames in layout `fmt`. A
/// software encoder is used (a probe must not open hardware sessions);
/// an encoder that cannot be built counts as a refusal.
fn muxer_takes(
    codecs: &CodecRegistry,
    probe: MuxProbe<'_>,
    codec: &str,
    running: &CodecParameters,
    time_base: TimeBase,
    fmt: PixelFormat,
) -> bool {
    let mut params = running.clone();
    params.codec_id = CodecId::new(codec);
    params.pixel_format = Some(fmt);
    params.options = Default::default();
    params.extradata = Vec::new();
    params.bit_rate = None;
    let software = CodecPreferences {
        no_hardware: true,
        ..Default::default()
    };
    let Ok(enc) = make_encoder_with(codecs, &params, &software) else {
        return false;
    };
    let stream = StreamInfo {
        index: 0,
        time_base,
        duration: None,
        start_time: Some(0),
        params: enc.output_params().clone(),
    };
    let sink: Box<dyn WriteSeek> = Box::new(Cursor::new(Vec::new()));
    probe
        .containers
        .open_muxer(probe.container, sink, &[stream])
        .is_ok()
}

/// The layout the `codec` encoder should be fed for a video stream
/// currently shaped like `running` (whose `pixel_format` is known),
/// muxed into `mux` when given. Returns `running.pixel_format` itself
/// when no conversion is needed, `None` when the running layout is
/// unknown. See the module docs for the candidate order.
pub(crate) fn encoder_input_format(
    codecs: &CodecRegistry,
    codec: &str,
    running: &CodecParameters,
    time_base: TimeBase,
    mux: Option<MuxProbe<'_>>,
) -> Option<PixelFormat> {
    let src = running.pixel_format?;
    let accepted = codec_accepted_pixel_formats(codecs, codec).unwrap_or_default();
    let enc_ok = |f: PixelFormat| accepted.is_empty() || accepted.contains(&f);
    let mux_ok = |f: PixelFormat| match mux {
        None => true,
        Some(m) => muxer_takes(codecs, m, codec, running, time_base, f),
    };
    if enc_ok(src) && mux_ok(src) {
        return Some(src);
    }
    let candidates: Vec<PixelFormat> = if accepted.is_empty() {
        ranked_targets(src)
    } else {
        accepted
            .iter()
            .copied()
            .filter(|&f| f != src && oxideav_pixfmt::supports(src, f))
            .collect()
    };
    if let Some(&f) = candidates.iter().find(|&&f| enc_ok(f) && mux_ok(f)) {
        return Some(f);
    }
    // Nothing satisfies both ends: honour the encoder alone and let
    // the muxer report its refusal.
    if enc_ok(src) {
        Some(src)
    } else {
        Some(accepted[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxideav_core::{
        CodecCapabilities, CodecInfo, Encoder, Error, Frame, MediaType, Muxer, Packet, Result,
    };

    /// An encoder that publishes its input parameters unchanged (the
    /// rawvideo shape).
    struct PassThrough(CodecParameters);
    impl Encoder for PassThrough {
        fn codec_id(&self) -> &CodecId {
            &self.0.codec_id
        }
        fn output_params(&self) -> &CodecParameters {
            &self.0
        }
        fn send_frame(&mut self, _: &Frame) -> Result<()> {
            Ok(())
        }
        fn receive_packet(&mut self) -> Result<Packet> {
            Err(Error::NeedMore)
        }
        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct NullMuxer;
    impl Muxer for NullMuxer {
        fn format_name(&self) -> &str {
            "yuvonly"
        }
        fn write_header(&mut self) -> Result<()> {
            Ok(())
        }
        fn write_packet(&mut self, _: &Packet) -> Result<()> {
            Ok(())
        }
        fn write_trailer(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// A raw-picture container that can only describe planar 4:2:0 /
    /// 4:4:4 YUV.
    fn yuv_only(_: Box<dyn WriteSeek>, streams: &[StreamInfo]) -> Result<Box<dyn Muxer>> {
        match streams[0].params.pixel_format {
            Some(PixelFormat::Yuv420P | PixelFormat::Yuv444P) => Ok(Box::new(NullMuxer)),
            other => Err(Error::unsupported(format!("{other:?}"))),
        }
    }

    fn setup(accepted: Vec<PixelFormat>) -> (CodecRegistry, ContainerRegistry) {
        let mut codecs = CodecRegistry::new();
        codecs.register(
            CodecInfo::new(CodecId::new("raw"))
                .capabilities(
                    CodecCapabilities::video("raw_sw")
                        .with_encode()
                        .with_pixel_formats(accepted),
                )
                .encoder(|p| Ok(Box::new(PassThrough(p.clone())))),
        );
        let mut containers = ContainerRegistry::new();
        containers.register_muxer("yuvonly", yuv_only);
        (codecs, containers)
    }

    fn running(fmt: PixelFormat) -> CodecParameters {
        let mut p = CodecParameters::video(CodecId::new("ffv1"));
        p.media_type = MediaType::Video;
        p.width = Some(16);
        p.height = Some(16);
        p.pixel_format = Some(fmt);
        p
    }

    #[test]
    fn the_muxer_rules_out_layouts_the_encoder_would_take() {
        let (codecs, containers) = setup(Vec::new());
        let mux = Some(MuxProbe {
            containers: &containers,
            container: "yuvonly",
        });
        let tb = TimeBase::new(1, 25);
        // RGB → full-chroma YUV, the least lossy layout the muxer takes.
        let got = encoder_input_format(&codecs, "raw", &running(PixelFormat::Gbrp8), tb, mux);
        assert_eq!(got, Some(PixelFormat::Yuv444P));
        // A layout the muxer takes is kept.
        let got = encoder_input_format(&codecs, "raw", &running(PixelFormat::Yuv420P), tb, mux);
        assert_eq!(got, Some(PixelFormat::Yuv420P));
        // Without a muxer to satisfy nothing changes.
        let got = encoder_input_format(&codecs, "raw", &running(PixelFormat::Gbrp8), tb, None);
        assert_eq!(got, Some(PixelFormat::Gbrp8));
    }

    #[test]
    fn the_encoder_preference_order_is_kept_among_muxable_layouts() {
        let (codecs, containers) = setup(vec![
            PixelFormat::Rgb24,
            PixelFormat::Yuv420P,
            PixelFormat::Yuv444P,
        ]);
        let mux = Some(MuxProbe {
            containers: &containers,
            container: "yuvonly",
        });
        let tb = TimeBase::new(1, 25);
        // Rgb24 is the encoder's first choice but the muxer refuses it;
        // Yuv420P is the next accepted layout.
        let got = encoder_input_format(&codecs, "raw", &running(PixelFormat::Gbrp8), tb, mux);
        assert_eq!(got, Some(PixelFormat::Yuv420P));
        // Encoder-only negotiation: its first accepted layout.
        let got = encoder_input_format(&codecs, "raw", &running(PixelFormat::Gbrp8), tb, None);
        assert_eq!(got, Some(PixelFormat::Rgb24));
    }

    #[test]
    fn rgb_prefers_full_chroma_yuv_over_subsampled() {
        let r = ranked_targets(PixelFormat::Gbrp8);
        let pos = |f| r.iter().position(|&x| x == f).unwrap();
        assert!(pos(PixelFormat::Yuv444P) < pos(PixelFormat::Yuv422P));
        assert!(pos(PixelFormat::Yuv422P) < pos(PixelFormat::Yuv420P));
        assert!(pos(PixelFormat::Yuv420P) < pos(PixelFormat::Gray8));
        // Same-model RGB layouts come before any YUV.
        assert!(pos(PixelFormat::Rgb24) < pos(PixelFormat::Yuv444P));
    }

    #[test]
    fn deep_sources_keep_their_depth_first() {
        let r = ranked_targets(PixelFormat::Yuv420P10Le);
        let pos = |f| r.iter().position(|&x| x == f).unwrap();
        assert!(pos(PixelFormat::Yuv444P10Le) < pos(PixelFormat::Yuv420P));
        assert!(pos(PixelFormat::Yuv420P12Le) < pos(PixelFormat::Yuv420P));
    }

    #[test]
    fn gray_sources_may_stay_gray() {
        let r = ranked_targets(PixelFormat::Gray8);
        assert!(!r.contains(&PixelFormat::Gray8));
        let pos = |f| r.iter().position(|&x| x == f).unwrap();
        assert!(pos(PixelFormat::Gray16Le) < pos(PixelFormat::MonoBlack));
    }
}
