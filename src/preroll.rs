//! First-picture pre-roll for video streams whose pixel layout nobody
//! has stated yet.
//!
//! A pipeline plans its stages (pixel conversion in front of the
//! encoder, the muxer's stream table) before the first packet flows,
//! from the stream parameters the container declared. Containers
//! that carry no layout (WebM, AVI, MPEG-TS, MOV for most codecs)
//! leave the decoder as the only authority — and
//! [`Decoder::output_pixel_format`] is allowed to answer `None` until
//! the decoder has seen its first packet. Without a layout, an
//! encoder that needs one (`rawvideo` into Y4M: "pixel_format is
//! required") cannot even be built.
//!
//! [`prime_video_decoder`] closes the gap: it reads packets from the
//! source until the decoder produces its first picture, then asks the
//! decoder again and, when it still says nothing, reads the layout
//! off the picture's plane geometry ([`infer_pixel_format`]). Every
//! packet read — of every stream — is kept, so the source can be
//! replayed from its first packet ([`ReplayDemuxer`] /
//! [`ReplayPackets`]); the decoder comes back wrapped in a
//! [`PrimedDecoder`] that skips the packets it already consumed and
//! hands out the pictures it already produced. The pumping loop then
//! runs unchanged, sees every packet exactly once, and the output is
//! byte-identical to a run whose container had declared the layout.

use std::collections::VecDeque;

use oxideav_core::{
    AttachedPicture, Attachment, Chapter, CodecId, CodecParameters, CodecRegistry, Decoder,
    Demuxer, Error, ExecutionContext, Frame, MediaType, Packet, PacketSource, PixelFormat, Result,
    StreamInfo, VideoFrame, VideoPlane, signal::ColorRange,
};
use oxideav_pixfmt::FormatInfo;

use crate::executor::{SourcePump, StageSpec, TrackRuntime};
use crate::selection::{CodecPreferences, make_decoder_with};

/// Packets of the primed stream fed to the decoder before the pre-roll
/// gives up waiting for a first picture. Reorder depth, a stream that
/// opens mid-GOP, and a hardware session's own buffering all fit well
/// within it; a decoder that stays silent past it is treated as
/// having no opinion on its layout (the planning continues without
/// one, as before the pre-roll existed).
pub(crate) const PREROLL_PACKET_BUDGET: usize = 256;

/// What [`prime_video_decoder`] hands back.
pub(crate) struct Primed {
    /// The decoder, wrapped so the source can be replayed from its
    /// first packet without the decoder seeing anything twice.
    pub(crate) decoder: Box<dyn Decoder>,
    /// The layout of the pictures to come — the decoder's own answer
    /// once it had seen a packet, else the first picture's geometry.
    /// `None` when neither told (no picture within the budget, or a
    /// geometry no planar layout explains).
    pub(crate) pixel_format: Option<PixelFormat>,
    /// The source hit end-of-stream during the pre-roll. Every packet
    /// is in `held`; the caller must not read the source again.
    pub(crate) eof: bool,
}

/// Feed `decoder` (for stream `stream_index`, described by `params`)
/// from `next` until its first picture appears, the budget runs out,
/// or `next` reports end-of-stream. Every packet `next` returns —
/// every stream's — is pushed onto `held` for replay.
///
/// Decoder errors on individual packets are tolerated the way the
/// pumping loops tolerate them (logged, the packet counts as fed);
/// source errors other than [`Error::Eof`] abort.
pub(crate) fn prime_video_decoder(
    mut decoder: Box<dyn Decoder>,
    params: &CodecParameters,
    stream_index: u32,
    next: &mut dyn FnMut() -> Result<Packet>,
    held: &mut Vec<Packet>,
) -> Result<Primed> {
    let mut pending: VecDeque<Frame> = VecDeque::new();
    let mut fed = 0usize;
    let mut eof = false;
    while pending.is_empty() && fed < PREROLL_PACKET_BUDGET {
        let pkt = match next() {
            Ok(p) => p,
            Err(Error::Eof) => {
                eof = true;
                if let Err(e) = decoder.flush() {
                    eprintln!("pipeline: decoder flush error during pre-roll: {e}");
                }
                collect_frames(&mut *decoder, &mut pending);
                break;
            }
            Err(e) => return Err(e),
        };
        let mine = pkt.stream_index == stream_index;
        held.push(pkt);
        if !mine {
            continue;
        }
        fed += 1;
        let pkt = held.last().expect("just pushed");
        match decoder.send_packet(pkt) {
            Ok(()) | Err(Error::NeedMore) => {}
            Err(e) => {
                eprintln!(
                    "pipeline: decoder skipped packet during pre-roll (stream {}, pts {:?}): {}",
                    stream_index, pkt.pts, e
                );
                continue;
            }
        }
        collect_frames(&mut *decoder, &mut pending);
    }
    let mut pixel_format = decoder.output_pixel_format();
    if pixel_format.is_none()
        && let Some(Frame::Video(v)) = pending.front()
    {
        pixel_format = infer_pixel_format(v, params.width.unwrap_or(0), params.height.unwrap_or(0));
    }
    Ok(Primed {
        decoder: Box::new(PrimedDecoder {
            codec_id: decoder.codec_id().clone(),
            inner: decoder,
            pending,
            skip: fed,
            flushed: eof,
            pixel_format,
        }),
        pixel_format,
        eof,
    })
}

/// Move every picture `d` has ready into `out`; a decoder error ends
/// the drain (the pumping loops treat it the same way).
fn collect_frames(d: &mut dyn Decoder, out: &mut VecDeque<Frame>) {
    loop {
        match d.receive_frame() {
            Ok(f) => out.push_back(f),
            Err(Error::NeedMore) | Err(Error::Eof) => return,
            Err(e) => {
                eprintln!("pipeline: decoder receive_frame error during pre-roll: {e}");
                return;
            }
        }
    }
}

/// A decoder that has already consumed the first `skip` packets of its
/// stream and holds the pictures they produced. When the source is
/// replayed from its first packet, the replayed packets are swallowed
/// and the held pictures come out first; past that every call goes
/// straight to the decoder. States the layout the pre-roll
/// established, so planning downstream of a decoder that reports none
/// still has one.
struct PrimedDecoder {
    inner: Box<dyn Decoder>,
    codec_id: CodecId,
    pending: VecDeque<Frame>,
    skip: usize,
    /// The pre-roll flushed the decoder (the source ended during it);
    /// the pumping loop's own flush must not flush it again.
    flushed: bool,
    pixel_format: Option<PixelFormat>,
}

impl Decoder for PrimedDecoder {
    fn codec_id(&self) -> &CodecId {
        &self.codec_id
    }

    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        if self.skip > 0 {
            self.skip -= 1;
            return Ok(());
        }
        self.inner.send_packet(packet)
    }

    fn receive_frame(&mut self) -> Result<Frame> {
        if let Some(f) = self.pending.pop_front() {
            return Ok(f);
        }
        if self.flushed && self.skip > 0 {
            // Replaying a source that ended during the pre-roll: the
            // decoder was flushed then and has nothing more until the
            // replay is over.
            return Err(Error::NeedMore);
        }
        self.inner.receive_frame()
    }

    fn flush(&mut self) -> Result<()> {
        if self.flushed {
            return Ok(());
        }
        self.inner.flush()
    }

    fn reset(&mut self) -> Result<()> {
        // A seek: the replay is void (the source dropped its held
        // packets too) and so are the pictures decoded before it.
        self.pending.clear();
        self.skip = 0;
        self.flushed = false;
        self.inner.reset()
    }

    fn set_execution_context(&mut self, ctx: &ExecutionContext) {
        self.inner.set_execution_context(ctx);
    }

    fn output_pixel_format(&self) -> Option<PixelFormat> {
        self.pixel_format
            .or_else(|| self.inner.output_pixel_format())
    }
}

/// A demuxer whose first packets are the ones a pre-roll already read
/// from `inner`, in their original order; after those it reads on.
/// A seek drops the held packets.
pub(crate) struct ReplayDemuxer {
    inner: Box<dyn Demuxer>,
    held: VecDeque<Packet>,
}

impl ReplayDemuxer {
    pub(crate) fn new(inner: Box<dyn Demuxer>, held: Vec<Packet>) -> Self {
        Self {
            inner,
            held: held.into(),
        }
    }
}

impl Demuxer for ReplayDemuxer {
    fn format_name(&self) -> &str {
        self.inner.format_name()
    }
    fn streams(&self) -> &[StreamInfo] {
        self.inner.streams()
    }
    fn next_packet(&mut self) -> Result<Packet> {
        match self.held.pop_front() {
            Some(p) => Ok(p),
            None => self.inner.next_packet(),
        }
    }
    fn set_active_streams(&mut self, indices: &[u32]) {
        self.inner.set_active_streams(indices);
    }
    fn seek_to(&mut self, stream_index: u32, pts: i64) -> Result<i64> {
        let landed = self.inner.seek_to(stream_index, pts)?;
        self.held.clear();
        Ok(landed)
    }
    fn metadata(&self) -> &[(String, String)] {
        self.inner.metadata()
    }
    fn duration_micros(&self) -> Option<i64> {
        self.inner.duration_micros()
    }
    fn attached_pictures(&self) -> &[AttachedPicture] {
        self.inner.attached_pictures()
    }
    fn chapters(&self) -> &[Chapter] {
        self.inner.chapters()
    }
    fn attachments(&self) -> &[Attachment] {
        self.inner.attachments()
    }
}

/// [`ReplayDemuxer`] for a [`PacketSource`].
pub(crate) struct ReplayPackets {
    inner: Box<dyn PacketSource>,
    held: VecDeque<Packet>,
}

impl ReplayPackets {
    pub(crate) fn new(inner: Box<dyn PacketSource>, held: Vec<Packet>) -> Self {
        Self {
            inner,
            held: held.into(),
        }
    }
}

impl PacketSource for ReplayPackets {
    fn streams(&self) -> &[StreamInfo] {
        self.inner.streams()
    }
    fn next_packet(&mut self) -> Result<Packet> {
        match self.held.pop_front() {
            Some(p) => Ok(p),
            None => self.inner.next_packet(),
        }
    }
    fn metadata(&self) -> &[(String, String)] {
        self.inner.metadata()
    }
    fn duration_micros(&self) -> Option<i64> {
        self.inner.duration_micros()
    }
}

/// Stand-in while a source is being wrapped; never read.
struct Detached;

impl Demuxer for Detached {
    fn format_name(&self) -> &str {
        "detached"
    }
    fn streams(&self) -> &[StreamInfo] {
        &[]
    }
    fn next_packet(&mut self) -> Result<Packet> {
        Err(Error::Eof)
    }
}

impl PacketSource for Detached {
    fn streams(&self) -> &[StreamInfo] {
        &[]
    }
    fn next_packet(&mut self) -> Result<Packet> {
        Err(Error::Eof)
    }
}

/// Whether `pl` is a re-encoded video track whose layout nobody has
/// stated: the planning in front of its encoder needs the pre-roll.
fn needs_priming(pl: &TrackRuntime) -> bool {
    !pl.copy
        && pl.decoder.is_none()
        && pl.input_params.media_type == MediaType::Video
        && pl.input_params.pixel_format.is_none()
        && pl.stages.iter().any(|s| matches!(s, StageSpec::Decode))
        && pl
            .stages
            .iter()
            .any(|s| matches!(s, StageSpec::Encode { .. }))
}

/// Executor hook: for every track [`needs_priming`] selects, build its
/// decoder (under `prefs`, announced `ctx`), pre-roll it from its
/// source and record the layout on the track's input parameters so the
/// pixel-format auto-insert pass and `instantiate` plan from it. The
/// source is replaced by its replaying wrapper. Frame-shape sources
/// carry their own parameters and are left alone.
pub(crate) fn prime_unlabelled_video(
    pipelines: &mut [TrackRuntime],
    sources_by_uri: &mut std::collections::HashMap<String, SourcePump>,
    codecs: &CodecRegistry,
    prefs: &CodecPreferences,
    ctx: &ExecutionContext,
) -> Result<()> {
    for pl in pipelines.iter_mut() {
        if !needs_priming(pl) {
            continue;
        }
        let Some(pump) = sources_by_uri.get_mut(&pl.source_uri) else {
            continue;
        };
        let mut decoder = make_decoder_with(codecs, &pl.input_params, prefs)?;
        decoder.set_execution_context(ctx);
        if decoder.output_pixel_format().is_some() {
            // The decoder knows from its configuration record: no
            // packet needs reading. `instantiate` reads the layout off
            // the decoder it finds installed.
            pl.decoder = Some(decoder);
            continue;
        }
        let mut held = Vec::new();
        let primed = match pump {
            SourcePump::Demuxer(dmx) => {
                let primed = prime_video_decoder(
                    decoder,
                    &pl.input_params,
                    pl.source_stream,
                    &mut || dmx.next_packet(),
                    &mut held,
                )?;
                let inner = std::mem::replace(dmx, Box::new(Detached));
                *dmx = Box::new(ReplayDemuxer::new(inner, held));
                primed
            }
            SourcePump::Packets(src) => {
                let primed = prime_video_decoder(
                    decoder,
                    &pl.input_params,
                    pl.source_stream,
                    &mut || src.next_packet(),
                    &mut held,
                )?;
                let inner = std::mem::replace(src, Box::new(Detached));
                *src = Box::new(ReplayPackets::new(inner, held));
                primed
            }
            SourcePump::Frames { .. } => continue,
        };
        if let Some(fmt) = primed.pixel_format {
            pl.input_params.pixel_format = Some(fmt);
        }
        pl.decoder = Some(primed.decoder);
    }
    Ok(())
}

/// The planar layouts a first picture's geometry is matched against,
/// by image-plane count. Packed layouts are left out on purpose: a
/// single two-byte-per-pixel plane is as much `Yuyv422` or `Ya8` as
/// `Gray16Le`, and a three-byte one `Bgr24` as much as `Rgb24` — a
/// decoder emitting those has a fixed layout and states it. Planar
/// RGB (`Gbrp8`, `Gbrap8`) shares its geometry with 4:4:4 YUV for the
/// same reason and is read as YUV: codecs that decode to planar RGB
/// (FFV1, PNG, …) declare it.
const THREE_PLANES: &[PixelFormat] = &[
    PixelFormat::Yuv420P,
    PixelFormat::Yuv422P,
    PixelFormat::Yuv444P,
    PixelFormat::Yuv440P,
    PixelFormat::Yuv411P,
    PixelFormat::Yuv420P10Le,
    PixelFormat::Yuv422P10Le,
    PixelFormat::Yuv444P10Le,
    PixelFormat::Yuv440P10Le,
    PixelFormat::Yuv420P12Le,
    PixelFormat::Yuv422P12Le,
    PixelFormat::Yuv444P12Le,
    PixelFormat::Yuv440P12Le,
    PixelFormat::Yuv420P16Le,
    PixelFormat::Yuv422P16Le,
    PixelFormat::Yuv444P16Le,
    PixelFormat::Yuv440P16Le,
];
const FOUR_PLANES: &[PixelFormat] = &[
    PixelFormat::Yuva420P,
    PixelFormat::Yuva422P,
    PixelFormat::Yuva444P,
    PixelFormat::Yuva420P10Le,
    PixelFormat::Yuva422P10Le,
    PixelFormat::Yuva444P10Le,
    PixelFormat::Yuva420P12Le,
    PixelFormat::Yuva422P12Le,
    PixelFormat::Yuva444P12Le,
    PixelFormat::Yuva420P16Le,
    PixelFormat::Yuva422P16Le,
    PixelFormat::Yuva444P16Le,
];
const TWO_PLANES: &[PixelFormat] = &[PixelFormat::Nv12];
const ONE_PLANE: &[PixelFormat] = &[
    PixelFormat::Gray8,
    PixelFormat::Gray10Le,
    PixelFormat::Gray12Le,
    PixelFormat::Gray16Le,
];

/// The layout of a `width` × `height` picture read off its plane
/// geometry, for decoders that state none. The picture's image planes
/// must be tightly packed (`stride` equal to the row's byte count)
/// and exactly one planar layout must explain them:
///
/// * the plane count and each plane's row length / row count pick the
///   chroma arrangement (4:2:0 / 4:2:2 / 4:4:4 / 4:4:0 / 4:1:1, 4:2:0
///   with interleaved chroma, grey, grey with a palette record);
/// * two bytes per sample leaves the bit depth open — a
///   significant-bits side-channel record settles it, otherwise the
///   largest luma sample does (a picture whose samples stay under
///   1024 is 10-bit, under 4096 12-bit, else 16-bit; a very dark
///   12-bit first picture would read as 10-bit, which is why
///   decoders should state their layout);
/// * a full-range colour-signal record turns 8-bit 4:2:0 / 4:2:2 /
///   4:4:4 into the `YuvJ` variants.
///
/// `None` for packed layouts, padded strides, a two-byte grey plane
/// without a significant-bits record (as much `Yuyv422` as
/// `Gray16Le`), unknown dimensions, or any other geometry.
pub(crate) fn infer_pixel_format(
    frame: &VideoFrame,
    width: u32,
    height: u32,
) -> Option<PixelFormat> {
    if width == 0 || height == 0 {
        return None;
    }
    let planes = frame.image_planes();
    let candidates: &[PixelFormat] = match planes.len() {
        1 if frame.palette().is_some() => &[PixelFormat::Pal8],
        1 => ONE_PLANE,
        2 => TWO_PLANES,
        3 => THREE_PLANES,
        4 => FOUR_PLANES,
        _ => return None,
    };
    let matching: Vec<PixelFormat> = candidates
        .iter()
        .copied()
        .filter(|f| tight_geometry(*f, planes, width, height))
        .collect();
    let picked = match matching.as_slice() {
        [] => return None,
        [one] => *one,
        several => {
            // Same geometry, several bit depths: two bytes per sample.
            let depth = frame.plane_significant_bits(0).or_else(|| {
                // A lone two-byte plane is not necessarily grey.
                (planes.len() > 1).then(|| sample_depth(&planes[0]))
            })?;
            *several
                .iter()
                .find(|f| FormatInfo::of(**f).bit_depth == depth)?
        }
    };
    let full_range = matches!(
        frame.color_signal().map(|s| s.range),
        Some(ColorRange::Full)
    );
    Some(match (picked, full_range) {
        (PixelFormat::Yuv420P, true) => PixelFormat::YuvJ420P,
        (PixelFormat::Yuv422P, true) => PixelFormat::YuvJ422P,
        (PixelFormat::Yuv444P, true) => PixelFormat::YuvJ444P,
        (f, _) => f,
    })
}

/// Whether `planes` are exactly the tightly packed planes of a
/// `width` × `height` picture in `fmt`: stride equal to the row's byte
/// count and data equal to the plane's rows, nothing more (a tolerance
/// on either would make 4:2:0 and 4:2:2 chroma, or a padded 8-bit row
/// and a two-byte one, indistinguishable).
fn tight_geometry(fmt: PixelFormat, planes: &[VideoPlane], width: u32, height: u32) -> bool {
    if fmt.plane_count() != planes.len() {
        return false;
    }
    planes.iter().enumerate().all(|(i, p)| {
        let Some(row) = fmt.plane_row_bytes(i, width) else {
            return false;
        };
        let Some((_, rows)) = fmt.plane_dimensions(i, width, height) else {
            return false;
        };
        row > 0 && p.stride == row && p.data.len() == row * rows as usize
    })
}

/// Bit depth the largest little-endian 16-bit sample of `plane`
/// implies: 10, 12 or 16.
fn sample_depth(plane: &VideoPlane) -> u8 {
    let max = plane
        .data
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u16::from_le_bytes(*b))
        .max()
        .unwrap_or(0);
    match max {
        0..=1023 => 10,
        1024..=4095 => 12,
        _ => 16,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxideav_core::{TimeBase, signal::ColorSignal};

    fn plane(stride: usize, rows: usize, fill: u8) -> VideoPlane {
        VideoPlane {
            stride,
            data: vec![fill; stride * rows],
        }
    }

    fn frame(planes: Vec<VideoPlane>) -> VideoFrame {
        VideoFrame { pts: None, planes }
    }

    /// Two-byte samples, all equal to `v`.
    fn plane16(w: usize, rows: usize, v: u16) -> VideoPlane {
        VideoPlane {
            stride: w * 2,
            data: v.to_le_bytes().repeat(w * rows),
        }
    }

    #[test]
    fn chroma_arrangements_are_read_off_the_plane_geometry() {
        let f = frame(vec![plane(8, 6, 0), plane(4, 3, 0), plane(4, 3, 0)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuv420P));
        let f = frame(vec![plane(8, 6, 0), plane(4, 6, 0), plane(4, 6, 0)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuv422P));
        let f = frame(vec![plane(8, 6, 0), plane(8, 6, 0), plane(8, 6, 0)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuv444P));
        let f = frame(vec![plane(8, 6, 0), plane(8, 3, 0), plane(8, 3, 0)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuv440P));
        let f = frame(vec![plane(8, 6, 0), plane(8, 3, 0)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Nv12));
        let f = frame(vec![plane(8, 6, 0)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Gray8));
        let f = frame(vec![
            plane(8, 6, 0),
            plane(4, 3, 0),
            plane(4, 3, 0),
            plane(8, 6, 0),
        ]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuva420P));
        // Odd dimensions round the chroma planes up.
        let f = frame(vec![plane(7, 5, 0), plane(4, 3, 0), plane(4, 3, 0)]);
        assert_eq!(infer_pixel_format(&f, 7, 5), Some(PixelFormat::Yuv420P));
    }

    #[test]
    fn two_byte_samples_take_their_depth_from_the_record_or_the_samples() {
        // No record: the largest luma sample decides.
        let f = frame(vec![
            plane16(8, 6, 1000),
            plane16(4, 3, 0),
            plane16(4, 3, 0),
        ]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuv420P10Le));
        let f = frame(vec![
            plane16(8, 6, 3000),
            plane16(4, 6, 0),
            plane16(4, 6, 0),
        ]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuv422P12Le));
        let f = frame(vec![
            plane16(8, 6, 50_000),
            plane16(8, 6, 0),
            plane16(8, 6, 0),
        ]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuv444P16Le));
        // A significant-bits record outranks dark samples.
        let f = frame(vec![plane16(8, 6, 100), plane16(4, 3, 0), plane16(4, 3, 0)])
            .with_significant_bits(vec![12, 12, 12]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuv420P12Le));
        // A lone two-byte plane is only grey when a record says so.
        let f = frame(vec![plane16(8, 6, 1000)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), None);
        let f = frame(vec![plane16(8, 6, 1000)]).with_significant_bits(vec![10]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Gray10Le));
    }

    #[test]
    fn side_channels_pick_palette_and_full_range_variants() {
        let f = frame(vec![plane(8, 6, 0)]).with_palette(vec![0; 768]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Pal8));
        let mut full = ColorSignal::unspecified();
        full.range = ColorRange::Full;
        let f = frame(vec![plane(8, 6, 0), plane(4, 3, 0), plane(4, 3, 0)]).with_color_signal(full);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::YuvJ420P));
        // Side-channel records are not image planes.
        let f = frame(vec![plane(8, 6, 0), plane(4, 3, 0), plane(4, 3, 0)])
            .with_significant_bits(vec![8, 8, 8]);
        assert_eq!(infer_pixel_format(&f, 8, 6), Some(PixelFormat::Yuv420P));
    }

    #[test]
    fn geometry_nobody_explains_stays_unknown() {
        // Padded stride.
        let f = frame(vec![plane(12, 6, 0), plane(6, 3, 0), plane(6, 3, 0)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), None);
        // Trailing bytes past the last row.
        let f = frame(vec![plane(8, 7, 0), plane(4, 3, 0), plane(4, 3, 0)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), None);
        // Packed RGB.
        let f = frame(vec![plane(24, 6, 0)]);
        assert_eq!(infer_pixel_format(&f, 8, 6), None);
        // Unknown dimensions.
        let f = frame(vec![plane(8, 6, 0), plane(4, 3, 0), plane(4, 3, 0)]);
        assert_eq!(infer_pixel_format(&f, 0, 6), None);
        // No planes at all.
        assert_eq!(infer_pixel_format(&frame(Vec::new()), 8, 6), None);
    }

    /// Decodes a 4×2 4:2:0 picture per packet, after a reorder delay
    /// of `delay` packets; states no layout.
    struct Delayed {
        id: CodecId,
        delay: usize,
        queue: VecDeque<Option<i64>>,
        flushed: bool,
    }

    impl Decoder for Delayed {
        fn codec_id(&self) -> &CodecId {
            &self.id
        }
        fn send_packet(&mut self, p: &Packet) -> Result<()> {
            self.queue.push_back(p.pts);
            Ok(())
        }
        fn receive_frame(&mut self) -> Result<Frame> {
            if self.queue.len() <= self.delay && !self.flushed {
                return Err(Error::NeedMore);
            }
            let pts = self.queue.pop_front().ok_or(Error::NeedMore)?;
            Ok(Frame::Video(VideoFrame {
                pts,
                planes: vec![plane(4, 2, 1), plane(2, 1, 2), plane(2, 1, 3)],
            }))
        }
        fn flush(&mut self) -> Result<()> {
            self.flushed = true;
            Ok(())
        }
    }

    fn delayed(delay: usize) -> Box<dyn Decoder> {
        Box::new(Delayed {
            id: CodecId::new("v"),
            delay,
            queue: VecDeque::new(),
            flushed: false,
        })
    }

    fn packet(stream: u32, pts: i64) -> Packet {
        let mut p = Packet::new(stream, TimeBase::new(1, 25), vec![stream as u8]);
        p.pts = Some(pts);
        p
    }

    fn video_params() -> CodecParameters {
        let mut p = CodecParameters::video(CodecId::new("v"));
        p.width = Some(4);
        p.height = Some(2);
        p
    }

    /// Interleaved source: a0 v0 a1 v1 a2 v2 … then Eof.
    fn interleaved(n: i64) -> Vec<Packet> {
        (0..n).flat_map(|i| [packet(1, i), packet(0, i)]).collect()
    }

    fn feed(src: Vec<Packet>) -> impl FnMut() -> Result<Packet> {
        let mut q: VecDeque<Packet> = src.into();
        move || q.pop_front().ok_or(Error::Eof)
    }

    /// Frame pts a primed decoder yields when the whole source is
    /// replayed through it, feeding it only stream-0 packets.
    fn replay(primed: &mut dyn Decoder, src: &[Packet]) -> Vec<Option<i64>> {
        let mut out = Vec::new();
        for p in src.iter().filter(|p| p.stream_index == 0) {
            primed.send_packet(p).unwrap();
            while let Ok(Frame::Video(v)) = primed.receive_frame() {
                out.push(v.pts);
            }
        }
        primed.flush().unwrap();
        while let Ok(Frame::Video(v)) = primed.receive_frame() {
            out.push(v.pts);
        }
        out
    }

    #[test]
    fn the_first_picture_fixes_the_layout_and_the_replay_sees_nothing_twice() {
        let src = interleaved(5);
        let mut next = feed(src.clone());
        let mut held = Vec::new();
        let primed =
            prime_video_decoder(delayed(2), &video_params(), 0, &mut next, &mut held).unwrap();
        assert_eq!(primed.pixel_format, Some(PixelFormat::Yuv420P));
        assert!(!primed.eof);
        // Three video packets were needed (delay 2); every packet up to
        // there — audio included — is held, in order.
        assert_eq!(held.len(), 6);
        assert_eq!(
            held.iter()
                .map(|p| (p.stream_index, p.pts))
                .collect::<Vec<_>>(),
            vec![
                (1, Some(0)),
                (0, Some(0)),
                (1, Some(1)),
                (0, Some(1)),
                (1, Some(2)),
                (0, Some(2))
            ]
        );
        let mut d = primed.decoder;
        assert_eq!(d.output_pixel_format(), Some(PixelFormat::Yuv420P));
        // Replaying the held packets then the rest yields every
        // picture exactly once, in order.
        let mut rest: Vec<Packet> = Vec::new();
        while let Ok(p) = next() {
            rest.push(p);
        }
        let mut all = held;
        all.extend(rest);
        assert_eq!(all.len(), src.len());
        assert_eq!(replay(&mut *d, &all), (0..5).map(Some).collect::<Vec<_>>());
    }

    #[test]
    fn a_source_that_ends_during_the_pre_roll_is_flushed_once() {
        // Two video packets, delay 5: the picture only comes on flush.
        let src = interleaved(2);
        let mut next = feed(src.clone());
        let mut held = Vec::new();
        let primed =
            prime_video_decoder(delayed(5), &video_params(), 0, &mut next, &mut held).unwrap();
        assert!(primed.eof);
        assert_eq!(primed.pixel_format, Some(PixelFormat::Yuv420P));
        assert_eq!(held.len(), src.len());
        let mut d = primed.decoder;
        assert_eq!(replay(&mut *d, &held), vec![Some(0), Some(1)]);
    }

    #[test]
    fn a_decoder_that_states_its_layout_is_believed_over_the_geometry() {
        struct Stated(Delayed, bool);
        impl Decoder for Stated {
            fn codec_id(&self) -> &CodecId {
                self.0.codec_id()
            }
            fn send_packet(&mut self, p: &Packet) -> Result<()> {
                self.1 = true;
                self.0.send_packet(p)
            }
            fn receive_frame(&mut self) -> Result<Frame> {
                self.0.receive_frame()
            }
            fn flush(&mut self) -> Result<()> {
                self.0.flush()
            }
            fn output_pixel_format(&self) -> Option<PixelFormat> {
                // Known only after the first packet, say.
                self.1.then_some(PixelFormat::YuvJ420P)
            }
        }
        let d = Box::new(Stated(
            Delayed {
                id: CodecId::new("v"),
                delay: 0,
                queue: VecDeque::new(),
                flushed: false,
            },
            false,
        ));
        let mut held = Vec::new();
        let primed =
            prime_video_decoder(d, &video_params(), 0, &mut feed(interleaved(3)), &mut held)
                .unwrap();
        assert_eq!(primed.pixel_format, Some(PixelFormat::YuvJ420P));
    }

    #[test]
    fn the_budget_bounds_a_silent_decoder() {
        let src: Vec<Packet> = (0..400).map(|i| packet(0, i)).collect();
        let mut held = Vec::new();
        let primed = prime_video_decoder(
            delayed(1_000),
            &video_params(),
            0,
            &mut feed(src),
            &mut held,
        )
        .unwrap();
        assert_eq!(primed.pixel_format, None);
        assert!(!primed.eof);
        assert_eq!(held.len(), PREROLL_PACKET_BUDGET);
    }

    #[test]
    fn the_replaying_demuxer_hands_out_the_held_packets_first_and_forgets_them_on_seek() {
        struct Src(VecDeque<Packet>, Vec<StreamInfo>);
        impl Demuxer for Src {
            fn format_name(&self) -> &str {
                "src"
            }
            fn streams(&self) -> &[StreamInfo] {
                &self.1
            }
            fn next_packet(&mut self) -> Result<Packet> {
                self.0.pop_front().ok_or(Error::Eof)
            }
            fn seek_to(&mut self, _: u32, pts: i64) -> Result<i64> {
                Ok(pts)
            }
        }
        let inner = Box::new(Src(vec![packet(0, 2), packet(0, 3)].into(), Vec::new()));
        let mut d = ReplayDemuxer::new(inner, vec![packet(0, 0), packet(0, 1)]);
        let mut seen = Vec::new();
        while let Ok(p) = d.next_packet() {
            seen.push(p.pts);
        }
        assert_eq!(seen, vec![Some(0), Some(1), Some(2), Some(3)]);
        let mut d = ReplayDemuxer::new(
            Box::new(Src(vec![packet(0, 9)].into(), Vec::new())),
            vec![packet(0, 0)],
        );
        assert_eq!(d.seek_to(0, 9).unwrap(), 9);
        assert_eq!(d.next_packet().unwrap().pts, Some(9));
    }
}
