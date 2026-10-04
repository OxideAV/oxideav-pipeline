//! Which input streams an output container can hold.
//!
//! Writing a file that carries video + audio into an audio-only
//! container (WAV, FLAC, MP3, …) or a picture-only one (Y4M, an image
//! format) should drop what does not fit instead of failing, the way
//! media tools conventionally behave: keep the best stream of every
//! media type the output can store and say what was dropped.
//!
//! Nothing here is container-specific. Capability is discovered from
//! the registered muxer itself: [`select_streams`] opens it on a
//! scratch buffer with candidate stream lists and keeps what it
//! accepts —
//!
//! 1. per media type, whether the muxer opens with one stream of that
//!    type (the input stream's own parameters first, then a few
//!    representative codecs, so a type is not rejected just because
//!    the source codec needs re-encoding);
//! 2. per accepted type, whether it opens with *all* the input's
//!    streams of that type, else only the best one is kept (largest
//!    picture / most channels, then highest sample rate, then input
//!    order).
//!
//! A single-codec output (`out.flac`, `out.png`: the extension names
//! one encoder) holds exactly one stream of that encoder's media type.

use std::io::Cursor;

use oxideav_core::{
    CodecId, CodecParameters, CodecRegistry, ContainerRegistry, MediaType, PixelFormat, Rational,
    SampleFormat, StreamInfo,
};

/// The outcome of [`select_streams`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamSelection {
    /// Input stream indices (`StreamInfo::index`) to write, in input
    /// order.
    pub keep: Vec<u32>,
    /// Input stream indices left out, in input order.
    pub dropped: Vec<u32>,
    /// One human-readable line per decision (`note: …`), for the
    /// caller to print.
    pub notes: Vec<String>,
}

impl StreamSelection {
    /// `true` when `index` is to be written.
    pub fn keeps(&self, index: u32) -> bool {
        self.keep.contains(&index)
    }
}

/// What the output is.
#[derive(Clone, Copy, Debug)]
pub enum OutputKind<'a> {
    /// A container muxer registered under this name; its stream
    /// capacity is probed.
    Container(&'a str),
    /// A single-codec output written by this encoder (image formats,
    /// raw audio formats): one stream of the encoder's media type.
    SingleCodec(&'a str),
}

/// Choose the streams of `streams` that `output` can hold. See the
/// module docs. Streams of a media type the output cannot store at all
/// are dropped; when no stream fits, `keep` is empty and the caller
/// reports the error (the muxer's own message is the best one).
pub fn select_streams(
    containers: &ContainerRegistry,
    codecs: &CodecRegistry,
    output: OutputKind<'_>,
    streams: &[StreamInfo],
) -> StreamSelection {
    let mut sel = StreamSelection::default();
    let kinds = kinds_in_order(streams);
    let single_kind = match output {
        OutputKind::SingleCodec(codec) => Some(encoder_media_type(codecs, codec)),
        OutputKind::Container(_) => None,
    };
    for kind in kinds {
        let of_kind: Vec<&StreamInfo> = streams
            .iter()
            .filter(|s| s.params.media_type == kind)
            .collect();
        let (accepts_kind, accepts_all) = match (output, single_kind) {
            (OutputKind::SingleCodec(_), Some(k)) => (k == Some(kind), false),
            (OutputKind::Container(name), _) => {
                let one = muxer_accepts_kind(containers, codecs, name, of_kind[0]);
                let all = one
                    && (of_kind.len() == 1
                        || muxer_accepts_all(containers, codecs, name, &of_kind));
                (one, all)
            }
            _ => (false, false),
        };
        if !accepts_kind {
            for s in &of_kind {
                sel.dropped.push(s.index);
                sel.notes.push(format!(
                    "note: dropping {} stream #{} ({}): the output cannot hold {} streams",
                    kind_name(kind),
                    s.index,
                    s.params.codec_id,
                    kind_name(kind)
                ));
            }
            continue;
        }
        if accepts_all {
            sel.keep.extend(of_kind.iter().map(|s| s.index));
            continue;
        }
        let best = best_stream(&of_kind);
        sel.keep.push(best.index);
        for s in of_kind.iter().filter(|s| s.index != best.index) {
            sel.dropped.push(s.index);
            sel.notes.push(format!(
                "note: dropping {} stream #{} ({}): the output holds one {} stream; keeping #{}",
                kind_name(kind),
                s.index,
                s.params.codec_id,
                kind_name(kind),
                best.index
            ));
        }
    }
    sel.keep.sort_unstable();
    sel.dropped.sort_unstable();
    sel
}

fn kinds_in_order(streams: &[StreamInfo]) -> Vec<MediaType> {
    let mut kinds = Vec::new();
    for s in streams {
        if !kinds.contains(&s.params.media_type) {
            kinds.push(s.params.media_type);
        }
    }
    kinds
}

fn kind_name(kind: MediaType) -> &'static str {
    match kind {
        MediaType::Video => "video",
        MediaType::Audio => "audio",
        MediaType::Subtitle => "subtitle",
        MediaType::Data => "data",
        _ => "unknown",
    }
}

/// The media type `codec`'s preferred encoder produces, `None` when
/// nothing encodes it.
fn encoder_media_type(codecs: &CodecRegistry, codec: &str) -> Option<MediaType> {
    codecs
        .implementations(&CodecId::new(codec))
        .iter()
        .find(|i| i.make_encoder.is_some())
        .map(|i| i.caps.media_type)
}

/// Representative codecs tried per media type when the input stream's
/// own codec is not storable — the question is whether the output
/// holds the media type at all, not this codec.
const VIDEO_PROBES: &[&str] = &["rawvideo", "h264", "vp9", "av1", "ffv1", "mjpeg", "png"];
const AUDIO_PROBES: &[&str] = &["pcm_s16le", "flac", "opus", "vorbis", "aac", "mp3"];

/// A stream of `codec` shaped like `like`. When `codec` has an
/// encoder, its output parameters are used (muxers often need the
/// configuration record an encoder publishes, e.g. FLAC's STREAMINFO).
fn representative(
    codecs: &CodecRegistry,
    kind: MediaType,
    codec: &str,
    like: &StreamInfo,
) -> StreamInfo {
    let mut params = match kind {
        MediaType::Audio => {
            let mut p = CodecParameters::audio(CodecId::new(codec));
            p.sample_rate = Some(like.params.sample_rate.unwrap_or(48_000));
            p.channels = Some(like.params.channels.unwrap_or(2));
            p.sample_format = Some(SampleFormat::S16);
            p
        }
        _ => {
            let mut p = CodecParameters::video(CodecId::new(codec));
            p.width = Some(like.params.width.unwrap_or(64));
            p.height = Some(like.params.height.unwrap_or(48));
            p.pixel_format = Some(PixelFormat::Yuv420P);
            p.frame_rate = Some(like.params.frame_rate.unwrap_or(Rational::new(25, 1)));
            p
        }
    };
    params.media_type = kind;
    // Software encoders only: a probe must not open hardware sessions.
    let software = crate::CodecPreferences {
        no_hardware: true,
        ..Default::default()
    };
    if let Ok(enc) = crate::make_encoder_with(codecs, &params, &software) {
        params = enc.output_params().clone();
    }
    StreamInfo {
        index: 0,
        params,
        ..like.clone()
    }
}

fn opens(containers: &ContainerRegistry, name: &str, streams: &[StreamInfo]) -> bool {
    let sink: Box<dyn oxideav_core::WriteSeek> = Box::new(Cursor::new(Vec::new()));
    containers.open_muxer(name, sink, streams).is_ok()
}

fn renumbered(streams: impl IntoIterator<Item = StreamInfo>) -> Vec<StreamInfo> {
    streams
        .into_iter()
        .enumerate()
        .map(|(i, s)| StreamInfo {
            index: i as u32,
            ..s
        })
        .collect()
}

fn muxer_accepts_kind(
    containers: &ContainerRegistry,
    codecs: &CodecRegistry,
    name: &str,
    s: &StreamInfo,
) -> bool {
    let kind = s.params.media_type;
    if opens(containers, name, &renumbered([s.clone()])) {
        return true;
    }
    let probes: &[&str] = match kind {
        MediaType::Video => VIDEO_PROBES,
        MediaType::Audio => AUDIO_PROBES,
        _ => &[],
    };
    probes
        .iter()
        .any(|c| opens(containers, name, &[representative(codecs, kind, c, s)]))
}

fn muxer_accepts_all(
    containers: &ContainerRegistry,
    codecs: &CodecRegistry,
    name: &str,
    of_kind: &[&StreamInfo],
) -> bool {
    if opens(
        containers,
        name,
        &renumbered(of_kind.iter().map(|s| (*s).clone())),
    ) {
        return true;
    }
    let kind = of_kind[0].params.media_type;
    let probes: &[&str] = match kind {
        MediaType::Video => VIDEO_PROBES,
        MediaType::Audio => AUDIO_PROBES,
        _ => &[],
    };
    probes.iter().any(|c| {
        opens(
            containers,
            name,
            &renumbered(of_kind.iter().map(|s| representative(codecs, kind, c, s))),
        )
    })
}

/// Largest picture / most channels, then highest sample rate, then
/// the earliest stream.
fn best_stream<'a>(of_kind: &[&'a StreamInfo]) -> &'a StreamInfo {
    let score = |s: &StreamInfo| {
        let p = &s.params;
        let area = u64::from(p.width.unwrap_or(0)) * u64::from(p.height.unwrap_or(0));
        (
            area,
            u64::from(p.channels.unwrap_or(0)),
            u64::from(p.sample_rate.unwrap_or(0)),
        )
    };
    let mut best = of_kind[0];
    for s in &of_kind[1..] {
        if score(s) > score(best) {
            best = s;
        }
    }
    best
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxideav_core::{Error, Muxer, Packet, Result, TimeBase, WriteSeek};

    struct NullMuxer;
    impl Muxer for NullMuxer {
        fn format_name(&self) -> &str {
            "null"
        }
        fn write_header(&mut self) -> Result<()> {
            Ok(())
        }
        fn write_packet(&mut self, _p: &Packet) -> Result<()> {
            Ok(())
        }
        fn write_trailer(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// One audio stream, PCM only (the WAV shape).
    fn open_one_pcm(_o: Box<dyn WriteSeek>, s: &[StreamInfo]) -> Result<Box<dyn Muxer>> {
        match s {
            [one] if one.params.codec_id.as_str().starts_with("pcm_") => Ok(Box::new(NullMuxer)),
            _ => Err(Error::unsupported("one PCM audio stream")),
        }
    }

    /// Any number of video streams, no audio.
    fn open_video_only(_o: Box<dyn WriteSeek>, s: &[StreamInfo]) -> Result<Box<dyn Muxer>> {
        if s.iter().all(|s| s.params.media_type == MediaType::Video) {
            Ok(Box::new(NullMuxer))
        } else {
            Err(Error::unsupported("video only"))
        }
    }

    /// Everything.
    fn open_any(_o: Box<dyn WriteSeek>, _s: &[StreamInfo]) -> Result<Box<dyn Muxer>> {
        Ok(Box::new(NullMuxer))
    }

    fn containers() -> ContainerRegistry {
        let mut c = ContainerRegistry::new();
        c.register_muxer("onepcm", open_one_pcm);
        c.register_muxer("vonly", open_video_only);
        c.register_muxer("any", open_any);
        c
    }

    fn video(index: u32, codec: &str, w: u32, h: u32) -> StreamInfo {
        let mut p = CodecParameters::video(CodecId::new(codec));
        p.width = Some(w);
        p.height = Some(h);
        StreamInfo {
            index,
            time_base: TimeBase::new(1, 25),
            duration: None,
            start_time: Some(0),
            params: p,
        }
    }

    fn audio(index: u32, codec: &str, channels: u16, rate: u32) -> StreamInfo {
        let mut p = CodecParameters::audio(CodecId::new(codec));
        p.channels = Some(channels);
        p.sample_rate = Some(rate);
        StreamInfo {
            index,
            time_base: TimeBase::new(1, rate as i64),
            duration: None,
            start_time: Some(0),
            params: p,
        }
    }

    #[test]
    fn audio_only_output_keeps_the_best_audio_stream() {
        let streams = [
            video(0, "h264", 128, 96),
            audio(1, "flac", 1, 22_050),
            audio(2, "flac", 2, 44_100),
        ];
        let sel = select_streams(
            &containers(),
            &CodecRegistry::new(),
            OutputKind::Container("onepcm"),
            &streams,
        );
        assert_eq!(sel.keep, vec![2]);
        assert_eq!(sel.dropped, vec![0, 1]);
        assert_eq!(sel.notes.len(), 2);
        assert!(sel.notes[0].contains("video stream #0"), "{:?}", sel.notes);
        assert!(sel.notes[1].contains("keeping #2"), "{:?}", sel.notes);
    }

    #[test]
    fn video_only_output_keeps_every_video_stream_it_accepts() {
        let streams = [
            video(0, "h264", 64, 48),
            audio(1, "aac", 2, 48_000),
            video(2, "h264", 32, 24),
        ];
        let sel = select_streams(
            &containers(),
            &CodecRegistry::new(),
            OutputKind::Container("vonly"),
            &streams,
        );
        assert_eq!(sel.keep, vec![0, 2]);
        assert_eq!(sel.dropped, vec![1]);
    }

    #[test]
    fn a_container_that_holds_everything_drops_nothing() {
        let streams = [video(0, "h264", 64, 48), audio(1, "aac", 2, 48_000)];
        let sel = select_streams(
            &containers(),
            &CodecRegistry::new(),
            OutputKind::Container("any"),
            &streams,
        );
        assert_eq!(sel.keep, vec![0, 1]);
        assert!(sel.dropped.is_empty() && sel.notes.is_empty());
    }

    #[test]
    fn single_codec_output_keeps_one_stream_of_the_encoders_type() {
        use oxideav_core::{CodecCapabilities, CodecInfo};
        let mut codecs = CodecRegistry::new();
        codecs.register(
            CodecInfo::new(CodecId::new("flac"))
                .capabilities(CodecCapabilities::audio("flac_sw").with_encode())
                .encoder(|_| Err(Error::unsupported("stub"))),
        );
        let streams = [
            video(0, "h264", 64, 48),
            audio(1, "aac", 6, 48_000),
            audio(2, "aac", 2, 48_000),
        ];
        let sel = select_streams(
            &containers(),
            &codecs,
            OutputKind::SingleCodec("flac"),
            &streams,
        );
        assert_eq!(sel.keep, vec![1]);
        assert_eq!(sel.dropped, vec![0, 2]);
    }

    #[test]
    fn nothing_fits_leaves_keep_empty() {
        let streams = [video(0, "h264", 64, 48)];
        let sel = select_streams(
            &containers(),
            &CodecRegistry::new(),
            OutputKind::Container("onepcm"),
            &streams,
        );
        assert!(sel.keep.is_empty());
        assert_eq!(sel.dropped, vec![0]);
    }
}
