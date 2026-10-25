//! Decoder-reported stream parameters, merged into the stream.
//!
//! Frames carry no width / height / pixel format / sample format /
//! rate by design: those live in the stream's [`CodecParameters`]. The
//! container's claim is only a first guess — the decoder knows what it
//! actually emits ([`Decoder::output_params`]) and says when that
//! changes mid-stream ([`Decoder::output_params_generation`]). Every
//! place this crate builds a decoder keeps a [`DecodedStream`]:
//! [`prime`](DecodedStream::prime) right after construction, before any
//! stage downstream is planned, then
//! [`observe`](DecodedStream::observe) after every `receive_frame` —
//! one integer compare per frame while nothing changes.
//!
//! A change of the shape the downstream stages were planned for is
//! handed to them as a [`Reshape`]:
//!
//! * a pixel-format conversion stage re-plans its source side (new
//!   layout / size in, the same layout out), so whatever follows it is
//!   unaffected;
//! * a filter stage, and an encoder whose input shape would change,
//!   cannot follow: the run fails with [`Error::Unsupported`] naming
//!   the old and the new shape;
//! * a raw-frame sink receives the frames in their new shape (the sink
//!   contracts have no reconfigure call); the merged parameters stay
//!   readable from the stream.
//!
//! Filling a field the plan did not know (`None` → `Some`) re-plans the
//! conversion stages the same way but never fails.

use oxideav_core::{
    CodecParameters, Decoder, Error, MediaType, OutputParamsWatch, PixelFormat, Result,
    SampleFormat,
};

use crate::executor::{FrameStage, frame_stage_failure_kind};
use crate::failure::{FailureStage, StageResult, attribute};

/// The output-describing fields downstream stages are planned from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Shape {
    pub(crate) media_type: MediaType,
    pub(crate) pixel_format: Option<PixelFormat>,
    pub(crate) width: Option<u32>,
    pub(crate) height: Option<u32>,
    pub(crate) sample_rate: Option<u32>,
    pub(crate) channels: Option<u16>,
    pub(crate) sample_format: Option<SampleFormat>,
}

impl Shape {
    pub(crate) fn of(p: &CodecParameters) -> Self {
        Self {
            media_type: p.media_type,
            pixel_format: p.pixel_format,
            width: p.width,
            height: p.height,
            sample_rate: p.sample_rate,
            channels: p.channels,
            sample_format: p.sample_format,
        }
    }

    /// A field both shapes know and disagree on — a real change, as
    /// opposed to filling in what the plan did not know.
    pub(crate) fn conflicts(&self, other: &Shape) -> bool {
        fn differ<T: PartialEq>(a: Option<T>, b: Option<T>) -> bool {
            matches!((a, b), (Some(x), Some(y)) if x != y)
        }
        differ(self.pixel_format, other.pixel_format)
            || differ(self.width, other.width)
            || differ(self.height, other.height)
            || differ(self.sample_rate, other.sample_rate)
            || differ(self.channels, other.channels)
            || differ(self.sample_format, other.sample_format)
    }
}

impl std::fmt::Display for Shape {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        fn opt<T: std::fmt::Debug>(v: Option<T>) -> String {
            v.map_or_else(|| "?".to_string(), |v| format!("{v:?}"))
        }
        match self.media_type {
            MediaType::Audio => write!(
                f,
                "{} Hz, {} ch, {}",
                opt(self.sample_rate),
                opt(self.channels),
                opt(self.sample_format)
            ),
            _ => write!(
                f,
                "{}x{} {}",
                opt(self.width),
                opt(self.height),
                opt(self.pixel_format)
            ),
        }
    }
}

/// The decoded shape moved away from the one the stages downstream
/// were planned for: `old` is the plan, `new` the frames from now on.
/// Each stage rewrites both to its own output shape before handing the
/// pair on ([`reshape_stage`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Reshape {
    pub(crate) old: Shape,
    pub(crate) new: Shape,
}

/// A stream's parameters plus the watch that keeps them in step with
/// the decoder producing its frames.
#[derive(Clone, Debug)]
pub(crate) struct DecodedStream {
    /// The stream's parameters: the container's, with the decoder's
    /// answer merged over them.
    pub(crate) params: CodecParameters,
    watch: OutputParamsWatch,
    /// What the stages downstream were planned from.
    planned: Shape,
}

impl DecodedStream {
    pub(crate) fn new(params: CodecParameters) -> Self {
        let planned = Shape::of(&params);
        Self {
            params,
            watch: OutputParamsWatch::new(),
            planned,
        }
    }

    /// Merge what `decoder` already knows (before its first frame) and
    /// take the result as the planning baseline. Returns `true` when a
    /// field changed.
    pub(crate) fn prime(&mut self, decoder: &dyn Decoder) -> bool {
        let changed = self.watch.prime(decoder, &mut self.params);
        self.planned = Shape::of(&self.params);
        changed
    }

    /// Call after every successful `receive_frame`. `Some` when the
    /// shape downstream was planned for no longer describes the frame
    /// just returned; the caller hands it to the stages before the
    /// frame.
    pub(crate) fn observe(&mut self, decoder: &dyn Decoder) -> Option<Reshape> {
        if !self.watch.observe(decoder, &mut self.params) {
            return None;
        }
        let new = Shape::of(&self.params);
        if new == self.planned {
            return None;
        }
        let old = std::mem::replace(&mut self.planned, new);
        Some(Reshape { old, new })
    }
}

/// Carry `r` through one frame stage: a pixel-format conversion takes
/// the new source shape (and outputs what it did before); a filter
/// refuses a real change. On return `r` describes the stage's output.
pub(crate) fn reshape_stage(stage: &mut FrameStage, r: &mut Reshape, track: u32) -> Result<()> {
    match stage {
        FrameStage::PixConvert { src_info, target } => {
            if r.new.media_type != MediaType::Video {
                return Ok(());
            }
            if let Some(f) = r.new.pixel_format {
                src_info.format = f;
            }
            if let Some(w) = r.new.width {
                src_info.width = w;
            }
            if let Some(h) = r.new.height {
                src_info.height = h;
            }
            r.old.pixel_format = Some(*target);
            r.new.pixel_format = Some(*target);
            Ok(())
        }
        FrameStage::Filter(_) => {
            if r.old.conflicts(&r.new) {
                Err(change_error(track, "a filter", r))
            } else {
                Ok(())
            }
        }
    }
}

/// [`reshape_stage`] over a whole chain, then the encoder check when
/// one follows it.
/// A refusal is attributed to the stage that cannot follow.
pub(crate) fn reshape_chain(
    stages: &mut [FrameStage],
    mut r: Reshape,
    has_encoder: bool,
    track: u32,
) -> StageResult<()> {
    for stage in stages.iter_mut() {
        reshape_stage(stage, &mut r, track)
            .map_err(attribute(frame_stage_failure_kind(stage), Some(track)))?;
    }
    if has_encoder {
        check_encoder(&r, track).map_err(attribute(FailureStage::Encode, Some(track)))?;
    }
    Ok(())
}

/// An encoder was opened for `r.old`: a real change of its input
/// shape cannot be followed.
pub(crate) fn check_encoder(r: &Reshape, track: u32) -> Result<()> {
    if r.old.conflicts(&r.new) {
        Err(change_error(track, "the encoder", r))
    } else {
        Ok(())
    }
}

fn change_error(track: u32, what: &str, r: &Reshape) -> Error {
    Error::unsupported(format!(
        "track {track}: the decoder changed its output mid-stream from {} to {}; \
         {what} was set up for the former and cannot follow",
        r.old, r.new
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxideav_core::CodecId;

    fn video(w: u32, h: u32, f: Option<PixelFormat>) -> Shape {
        let mut p = CodecParameters::video(CodecId::new("x"));
        p.width = Some(w);
        p.height = Some(h);
        p.pixel_format = f;
        Shape::of(&p)
    }

    #[test]
    fn filling_is_not_a_conflict() {
        assert!(!video(4, 4, None).conflicts(&video(4, 4, Some(PixelFormat::Yuv420P))));
        assert!(video(4, 4, Some(PixelFormat::Yuv420P)).conflicts(&video(8, 4, None)));
    }

    #[test]
    fn convert_stage_absorbs_a_change() {
        let mut stage = FrameStage::PixConvert {
            src_info: oxideav_pixfmt::FrameInfo::new(PixelFormat::Yuv420P, 4, 4),
            target: PixelFormat::Rgb24,
        };
        let r = Reshape {
            old: video(4, 4, Some(PixelFormat::Yuv420P)),
            new: video(4, 4, Some(PixelFormat::Yuv444P)),
        };
        reshape_chain(std::slice::from_mut(&mut stage), r, true, 0).unwrap();
        let FrameStage::PixConvert { src_info, .. } = stage else {
            unreachable!()
        };
        assert_eq!(src_info.format, PixelFormat::Yuv444P);
        // A size change still reaches the encoder.
        let r = Reshape {
            old: video(4, 4, Some(PixelFormat::Yuv420P)),
            new: video(8, 8, Some(PixelFormat::Yuv420P)),
        };
        let fail = reshape_chain(&mut [], r, true, 3).unwrap_err();
        assert_eq!(fail.stage, FailureStage::Encode);
        let err = fail.error;
        assert!(matches!(err, Error::Unsupported(_)), "{err}");
        assert!(err.to_string().contains("4x4"), "{err}");
        // Without an encoder the new shape just flows on.
        reshape_chain(&mut [], r, false, 3).unwrap();
    }
}
