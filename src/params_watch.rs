//! Decoder-reported stream parameters, merged into the stream, and the
//! in-band [`Frame::StreamParams`] event that carries a mid-stream
//! change down a track.
//!
//! Frames carry no width / height / pixel format / sample format /
//! rate by design: those live in the stream's [`CodecParameters`]. The
//! container's claim is only a first guess — the decoder knows what it
//! actually emits ([`Decoder::output_params`]) and says when that
//! changes mid-stream, two ways that always agree: the
//! [`Decoder::output_params_generation`] counter moves, and a decoder
//! that emits it returns a `Frame::StreamParams` event immediately
//! before the first frame of the new shape.
//!
//! Every place this crate builds a decoder keeps a [`DecodedStream`]:
//! [`prime`](DecodedStream::prime) right after construction, before any
//! stage downstream is planned, then [`follow`](DecodedStream::follow)
//! on every frame `receive_frame` returns. `follow` folds the decoder's
//! event into the stream ([`OutputParamsWatch::observe_frame`]) — or,
//! for a decoder that does not emit one, notices the generation move
//! ([`OutputParamsWatch::observe`]) and synthesises the event itself —
//! so the stages downstream always see exactly one event per change,
//! in place, whichever kind of decoder sits upstream (one integer
//! compare per frame while nothing changes).
//!
//! Downstream of the decoder the event travels with the frames:
//!
//! * a pixel-format conversion stage re-plans its source side (new
//!   layout / size in, the same layout out) and forwards the event
//!   restated for its own output;
//! * a filter stage hands it to the [`StreamFilter`](oxideav_core::StreamFilter),
//!   which re-plans (forwarding its own event when its output changes)
//!   or refuses; a refusal of a change the filter's plan depends on
//!   fails the run with [`Error::Unsupported`] naming both shapes (see
//!   `RuntimeFilter` for the pass-through and fill-in cases);
//! * an encoder is handed the event only when its input shape really
//!   changes ([`EncoderFeed`]); one that re-configures carries on, one
//!   that refuses fails the run with [`Error::Unsupported`] naming both
//!   shapes — the pipeline neither re-opens the encoder nor inserts a
//!   scaler behind the caller's back;
//! * a raw-frame sink is told through its stream-parameters hook, never
//!   through its frame-writing call ([`SinkFeed`]); a sink downstream of
//!   an encoder that re-configured is told the encoder's new output
//!   parameters.
//!
//! Filling a field the plan did not know (`None` → `Some`) travels the
//! same way but never fails.

use oxideav_core::{
    CodecParameters, Decoder, Encoder, Error, Frame, MediaType, OutputParamsWatch, PixelFormat,
    Result, SampleFormat,
};

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

/// What [`DecodedStream::follow`] hands downstream for one frame the
/// decoder returned, in order: the change event (when the shape moved)
/// first, then the media frame (absent when the decoder's frame was
/// the event itself).
#[derive(Debug, Default)]
pub(crate) struct Followed {
    pub(crate) event: Option<Frame>,
    pub(crate) frame: Option<Frame>,
}

impl Followed {
    /// `true` when this carries a media frame (not only an event).
    pub(crate) fn has_media(&self) -> bool {
        self.frame.is_some()
    }
}

impl IntoIterator for Followed {
    type Item = Frame;
    type IntoIter = std::iter::Chain<std::option::IntoIter<Frame>, std::option::IntoIter<Frame>>;
    fn into_iter(self) -> Self::IntoIter {
        self.event.into_iter().chain(self.frame)
    }
}

/// A stream's parameters plus the watch that keeps them in step with
/// the decoder producing its frames.
#[derive(Clone, Debug)]
pub(crate) struct DecodedStream {
    /// The stream's parameters: the container's, with the decoder's
    /// answer merged over them.
    pub(crate) params: CodecParameters,
    watch: OutputParamsWatch,
    /// What the stages downstream were planned from (or last told).
    planned: Shape,
    /// The watch has read the decoder after a frame at least once:
    /// from then on every change it reports is a mid-stream one.
    read: bool,
}

impl DecodedStream {
    pub(crate) fn new(params: CodecParameters) -> Self {
        let planned = Shape::of(&params);
        Self {
            params,
            watch: OutputParamsWatch::new(),
            planned,
            read: false,
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

    /// Fold one frame `decoder` just returned into the stream and say
    /// what goes downstream.
    ///
    /// * The decoder's own [`Frame::StreamParams`] event is merged
    ///   ([`OutputParamsWatch::observe_frame`]); the generation it has
    ///   already moved is absorbed in the same step, so the media frame
    ///   after it reports nothing a second time.
    /// * A media frame from a decoder that moved its generation without
    ///   an event gets one synthesised in front of it.
    ///
    /// The event handed on carries the stream's whole merged
    /// parameters. On the first read after the decoder's first frame
    /// (the description may still be refined up to there) an event is
    /// produced only when a planning field moved; a mid-stream change
    /// of anything the decoder describes always produces one.
    pub(crate) fn follow(&mut self, decoder: &dyn Decoder, frame: Frame) -> Followed {
        let is_event = frame.is_stream_params();
        let changed = if is_event {
            let merged = self.watch.observe_frame(&frame, &mut self.params);
            // The decoder bumped its counter together with the event:
            // record that read now (it merges the same description,
            // so it reports no change of its own).
            self.watch.observe(decoder, &mut self.params) | merged
        } else {
            self.watch.observe(decoder, &mut self.params)
        };
        let first_read = !self.read;
        if !is_event {
            self.read = true;
        }
        let frame = (!is_event).then_some(frame);
        if !changed {
            return Followed { event: None, frame };
        }
        let new = Shape::of(&self.params);
        let moved = new != self.planned;
        self.planned = new;
        if !moved && first_read && !is_event {
            return Followed { event: None, frame };
        }
        Followed {
            event: Some(Frame::StreamParams(Box::new(self.params.clone()))),
            frame,
        }
    }
}

/// The input side of an encoder: the shape it was opened for, kept in
/// step with the [`Frame::StreamParams`] events reaching it.
#[derive(Clone, Debug)]
pub(crate) struct EncoderFeed {
    input: CodecParameters,
    output: CodecParameters,
}

impl EncoderFeed {
    /// `input` is the shape of the frames the encoder was opened for
    /// (the end of the frame-stage chain), `encoder` the one built for
    /// it.
    pub(crate) fn new(input: CodecParameters, encoder: &dyn Encoder) -> Self {
        Self {
            input,
            output: encoder.output_params().clone(),
        }
    }

    /// Hand `frame` to `encoder`.
    ///
    /// A media frame goes straight to `send_frame`. A
    /// [`Frame::StreamParams`] event is merged into the planned input:
    /// when that only fills in what the plan did not know (or touches
    /// nothing the encoder was opened from) the encoder is not
    /// bothered; a real change is handed to the encoder, which either
    /// re-configures (`Ok`) or cannot follow — any error then fails
    /// with [`Error::Unsupported`] naming both shapes.
    ///
    /// Returns the encoder's new output parameters when it
    /// re-configured into a different output description (for the sink
    /// / muxer side), `None` otherwise.
    pub(crate) fn send(
        &mut self,
        encoder: &mut dyn Encoder,
        frame: &Frame,
        track: u32,
    ) -> Result<Option<CodecParameters>> {
        let Some(p) = frame.stream_params() else {
            encoder.send_frame(frame)?;
            return Ok(None);
        };
        let mut next = self.input.clone();
        if !next.merge_output_params(p) {
            return Ok(None);
        }
        let (old, new) = (Shape::of(&self.input), Shape::of(&next));
        if !old.conflicts(&new) {
            self.input = next;
            return Ok(None);
        }
        if let Err(why) = encoder.send_frame(frame) {
            return Err(change_error(track, "the encoder", &old, &new, &why));
        }
        self.input = next;
        let mut out = self.output.clone();
        if out.merge_output_params(encoder.output_params()) {
            self.output = out.clone();
            Ok(Some(out))
        } else {
            Ok(None)
        }
    }
}

/// The parameters a raw-frame sink was started with, kept in step with
/// the [`Frame::StreamParams`] events reaching the end of the chain so
/// the sink hears of each actual change once.
#[derive(Clone, Debug)]
pub(crate) struct SinkFeed {
    params: CodecParameters,
}

impl SinkFeed {
    pub(crate) fn new(params: CodecParameters) -> Self {
        Self { params }
    }

    /// Merge `event`'s parameters; `Some` (the sink's new stream
    /// parameters) when that changed something the sink was told.
    pub(crate) fn update(&mut self, event: &CodecParameters) -> Option<&CodecParameters> {
        self.params
            .merge_output_params(event)
            .then_some(&self.params)
    }
}

/// The run-failing error for a change a stage cannot follow.
pub(crate) fn change_error(track: u32, what: &str, old: &Shape, new: &Shape, why: &Error) -> Error {
    Error::unsupported(format!(
        "track {track}: the stream changed its shape mid-stream from {old} to {new}; \
         {what} was set up for the former and cannot follow ({why})"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxideav_core::{CodecId, Packet, VideoFrame, VideoPlane};

    fn video(w: u32, h: u32, f: Option<PixelFormat>) -> CodecParameters {
        let mut p = CodecParameters::video(CodecId::new("x"));
        p.width = Some(w);
        p.height = Some(h);
        p.pixel_format = f;
        p
    }

    #[test]
    fn filling_is_not_a_conflict() {
        let s = |w, h, f| Shape::of(&video(w, h, f));
        assert!(!s(4, 4, None).conflicts(&s(4, 4, Some(PixelFormat::Yuv420P))));
        assert!(s(4, 4, Some(PixelFormat::Yuv420P)).conflicts(&s(8, 4, None)));
    }

    /// A decoder whose output description the test drives by hand.
    struct Dec {
        id: CodecId,
        params: CodecParameters,
        generation: u64,
    }

    impl Decoder for Dec {
        fn codec_id(&self) -> &CodecId {
            &self.id
        }
        fn send_packet(&mut self, _: &Packet) -> Result<()> {
            Ok(())
        }
        fn receive_frame(&mut self) -> Result<Frame> {
            Err(Error::NeedMore)
        }
        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
        fn output_params(&self) -> Option<CodecParameters> {
            Some(self.params.clone())
        }
        fn output_params_generation(&self) -> u64 {
            self.generation
        }
    }

    fn picture() -> Frame {
        Frame::Video(VideoFrame {
            pts: Some(0),
            planes: vec![VideoPlane {
                stride: 1,
                data: vec![0],
            }],
        })
    }

    #[test]
    fn follow_reports_each_change_once_either_way() {
        let p4 = video(4, 4, Some(PixelFormat::Yuv420P));
        let p8 = video(8, 8, Some(PixelFormat::Yuv420P));
        let mut dec = Dec {
            id: CodecId::new("x"),
            params: p4.clone(),
            generation: 0,
        };
        let mut s = DecodedStream::new(CodecParameters::video(CodecId::new("x")));
        s.prime(&dec);
        // Constant shape: nothing but the frame.
        let f = s.follow(&dec, picture());
        assert!(f.event.is_none() && f.has_media());
        // Generation-only change: one synthesised event, then quiet.
        dec.params = p8.clone();
        dec.generation = 1;
        let f = s.follow(&dec, picture());
        assert_eq!(
            f.event
                .as_ref()
                .and_then(|e| e.stream_params())
                .and_then(|p| p.width),
            Some(8)
        );
        assert!(s.follow(&dec, picture()).event.is_none());
        // Event + generation (an emitting decoder): the event is handed
        // on once, the frame after it reports nothing.
        dec.params = p4.clone();
        dec.generation = 2;
        let f = s.follow(&dec, Frame::StreamParams(Box::new(p4.clone())));
        assert!(f.event.is_some() && !f.has_media());
        assert!(s.follow(&dec, picture()).event.is_none());
        assert_eq!(s.params.width, Some(4));
    }
}
