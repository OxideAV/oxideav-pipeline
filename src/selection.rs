//! Codec-selection policy.
//!
//! `oxideav-core::CodecRegistry` is registration-only plus a couple of
//! "first-match" lookups (`first_decoder` / `decoder_by_impl` and
//! their encoder counterparts) for single-impl test scenarios. It
//! does NOT bias the choice across multiple candidates — that policy
//! lives here.
//!
//! Free functions [`make_decoder`] / [`make_decoder_with`] /
//! [`make_encoder`] / [`make_encoder_with`] take `&CodecRegistry` as
//! the first argument. Each walks every implementation registered for
//! `params.codec_id` in increasing priority order, filters by the
//! impl's `caps.fits_params` restrictions and the preferences'
//! exclude rules, then tries each factory until one returns `Ok`.
//!
//! Init-time fallback ("hardware session creation failed → try the SW
//! path") is implemented here: on `Err` from the highest-priority
//! factory, the next candidate is attempted; the last error is
//! surfaced if every candidate fails. To opt out of the SW fallback
//! when the pipeline truly needs hardware, set
//! `CodecPreferences { require_hardware: true, .. }`.

use std::collections::VecDeque;

use oxideav_core::{
    CodecCapabilities, CodecId, CodecImplementation, CodecParameters, CodecRegistry, Decoder,
    DecoderFactory, Encoder, Error, ExecutionContext, Frame, Packet, PixelFormat, Result,
};

/// User preferences for codec selection — pass to `make_decoder_with`
/// / `make_encoder_with` (free functions or via the
/// [`CodecRegistryExt`] trait) to bias the choice.
#[derive(Clone, Debug, Default)]
pub struct CodecPreferences {
    /// Implementation names to prefer (boost their priority by `boost`).
    pub prefer: Vec<String>,
    /// Implementation names to skip entirely.
    pub exclude: Vec<String>,
    /// Forbid hardware-accelerated impls. Mutually exclusive with
    /// `require_hardware` — if both are set, no impl is selectable.
    pub no_hardware: bool,
    /// Forbid software-only impls — only hardware-accelerated factories
    /// are considered. The init-time fallback (try next priority on
    /// factory `Err`) still applies *within* the HW candidate set, but
    /// it will NOT silently degrade to the SW path if every HW factory
    /// fails. Use this when the pipeline needs hardware (real-time
    /// low-latency capture, energy budget, etc.) — the resulting
    /// `make_decoder_with` / `make_encoder_with` will surface the
    /// underlying `OSStatus` / device error.
    pub require_hardware: bool,
    /// Boost amount for `prefer` impls (subtracted from priority).
    pub boost: i32,
}

impl CodecPreferences {
    pub fn excludes(&self, caps: &CodecCapabilities) -> bool {
        self.exclude.iter().any(|n| n == &caps.implementation)
            || (self.no_hardware && caps.hardware_accelerated)
            || (self.require_hardware && !caps.hardware_accelerated)
    }

    pub fn effective_priority(&self, caps: &CodecCapabilities) -> i32 {
        if self.prefer.iter().any(|n| n == &caps.implementation) {
            caps.priority - self.boost.max(0)
        } else {
            caps.priority
        }
    }
}

/// Which factory of a [`CodecImplementation`] a lookup is about.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Side {
    Decode,
    Encode,
}

/// The implementations registered for `params.codec_id` that
/// [`make_decoder_with`] / [`make_encoder_with`] (per `side`) would
/// try, in the order they would try them: the ones with the matching
/// factory that `prefs` does not exclude — and, with `fit`, whose
/// restrictions (`caps.fits_params`) take `params` — sorted by
/// [`CodecPreferences::effective_priority`] (registration order breaks
/// ties). The registry itself keeps registration order, which on a
/// host with hardware bridges puts e.g. `opus_audiotoolbox` ahead of
/// the pure-Rust `opus`; every capability lookup in this crate goes
/// through this ranking so it describes the implementation that is
/// actually instantiated.
fn ranked_implementations<'r>(
    reg: &'r CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
    side: Side,
    fit: bool,
) -> Vec<&'r CodecImplementation> {
    let mut ranked: Vec<&CodecImplementation> = reg
        .implementations(&params.codec_id)
        .iter()
        .filter(|i| match side {
            Side::Decode => i.make_decoder.is_some(),
            Side::Encode => i.make_encoder.is_some(),
        })
        .filter(|i| !prefs.excludes(&i.caps))
        .filter(|i| !fit || i.caps.fits_params(params, side == Side::Encode))
        .collect();
    ranked.sort_by_key(|i| prefs.effective_priority(&i.caps));
    ranked
}

/// The implementation `make_*_with` would try first for `params`
/// under `prefs`. When no candidate's restrictions take `params` as
/// they stand, the best-ranked candidate regardless of fit — a
/// pipeline reads capabilities *before* it negotiates the stream shape
/// (resample, downmix, pixel conversion), and the negotiation is what
/// makes the parameters fit.
fn preferred_implementation<'r>(
    reg: &'r CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
    side: Side,
) -> Option<&'r CodecImplementation> {
    ranked_implementations(reg, params, prefs, side, true)
        .into_iter()
        .next()
        .or_else(|| {
            ranked_implementations(reg, params, prefs, side, false)
                .into_iter()
                .next()
        })
}

/// Capabilities of the encoder [`make_encoder_with`] would build for
/// `params` under `prefs` — the same ranking, so a pipeline planning
/// its resample / downmix / pixel-conversion stages from
/// `accepted_sample_rates` / `accepted_sample_formats` /
/// `accepted_pixel_formats` reads the implementation that is going to
/// be fed, not the first one registered. `None` when nothing
/// registered under `prefs` encodes `params.codec_id`.
///
/// When no candidate's `max_*` restrictions take `params` as they
/// stand, the best-ranked candidate's capabilities are returned
/// anyway: the caller is about to reshape the stream.
pub fn preferred_encoder_caps(
    reg: &CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
) -> Option<CodecCapabilities> {
    preferred_implementation(reg, params, prefs, Side::Encode).map(|i| i.caps.clone())
}

/// Capabilities of the decoder [`make_decoder_with`] would build for
/// `params` under `prefs`. See [`preferred_encoder_caps`].
pub fn preferred_decoder_caps(
    reg: &CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
) -> Option<CodecCapabilities> {
    preferred_implementation(reg, params, prefs, Side::Decode).map(|i| i.caps.clone())
}

/// Free-function shorthand: `make_decoder(reg, params)` with default
/// preferences. Equivalent to
/// `make_decoder_with(reg, params, &CodecPreferences::default())`.
pub fn make_decoder(reg: &CodecRegistry, params: &CodecParameters) -> Result<Box<dyn Decoder>> {
    make_decoder_with(reg, params, &CodecPreferences::default())
}

/// Build a decoder for `params` honouring `prefs`. Walks every
/// implementation registered for `params.codec_id` in increasing
/// effective-priority order, skipping any excluded by the prefs, then
/// tries each factory until one returns `Ok`. The last error is
/// surfaced if every candidate fails.
pub fn make_decoder_with(
    reg: &CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
) -> Result<Box<dyn Decoder>> {
    if reg.implementations(&params.codec_id).is_empty() {
        return Err(Error::CodecNotFound(params.codec_id.to_string()));
    }
    let ranked = ranked_implementations(reg, params, prefs, Side::Decode, true);
    let mut last_err: Option<Error> = None;
    for (n, imp) in ranked.iter().enumerate() {
        match (imp.make_decoder.unwrap())(params) {
            Ok(d) => {
                let rest: Vec<DecoderFactory> = ranked[n + 1..]
                    .iter()
                    .filter_map(|i| i.make_decoder)
                    .collect();
                if imp.caps.hardware_accelerated && !rest.is_empty() {
                    return Ok(Box::new(FallbackDecoder::new(d, rest, params.clone())));
                }
                return Ok(d);
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        Error::CodecNotFound(format!(
            "no decoder for {} accepts the requested parameters",
            params.codec_id
        ))
    }))
}

/// Packets a [`FallbackDecoder`] keeps for replay before its decoder
/// has produced a frame; past this the decoder is trusted as-is.
const FALLBACK_REPLAY_PACKETS: usize = 64;

/// Decode-time fallback for hardware decoders.
///
/// Hardware decoders typically open their device session lazily, on
/// the first packet, once the stream's sequence header is known — so a
/// session the hardware refuses (a picture below the engine's minimum
/// size, an unsupported profile or level) fails in `send_packet`, after
/// [`make_decoder_with`]'s construction-time fallback has already
/// picked the implementation. Until the decoder has produced its first
/// frame this wrapper keeps the packets it was fed; an error then
/// moves on to the next candidate implementation (in the same
/// preference order) and replays them, so the stream still decodes.
/// After the first frame every call goes straight to the decoder.
struct FallbackDecoder {
    current: Box<dyn Decoder>,
    /// Remaining candidates, in preference order.
    rest: VecDeque<DecoderFactory>,
    params: CodecParameters,
    codec_id: CodecId,
    ctx: Option<ExecutionContext>,
    /// Packets fed since the start, while no frame has come out.
    replay: Vec<Packet>,
    /// Frames a replacement decoder produced while being replayed.
    pending: VecDeque<Frame>,
    /// A frame came out (or the replay budget ran out): no fallback.
    committed: bool,
    flushed: bool,
}

impl FallbackDecoder {
    fn new(current: Box<dyn Decoder>, rest: Vec<DecoderFactory>, params: CodecParameters) -> Self {
        Self {
            codec_id: current.codec_id().clone(),
            current,
            rest: rest.into(),
            params,
            ctx: None,
            replay: Vec::new(),
            pending: VecDeque::new(),
            committed: false,
            flushed: false,
        }
    }

    /// Switch to the next candidate that accepts every buffered packet
    /// (and the flush, when one was signalled). `Err(first)` when no
    /// candidate is left — the caller surfaces the original error.
    fn fall_back(&mut self, first: Error) -> Result<()> {
        let mut err = first;
        while let Some(factory) = self.rest.pop_front() {
            let mut d = match factory(&self.params) {
                Ok(d) => d,
                Err(e) => {
                    err = e;
                    continue;
                }
            };
            if let Some(ctx) = &self.ctx {
                d.set_execution_context(ctx);
            }
            let mut pending = VecDeque::new();
            let replayed = (|| -> Result<()> {
                for p in &self.replay {
                    d.send_packet(p)?;
                    drain_into(&mut *d, &mut pending)?;
                }
                if self.flushed {
                    d.flush()?;
                    drain_into(&mut *d, &mut pending)?;
                }
                Ok(())
            })();
            match replayed {
                Ok(()) => {
                    self.current = d;
                    self.pending = pending;
                    if !self.pending.is_empty() {
                        self.commit();
                    }
                    return Ok(());
                }
                Err(e) => err = e,
            }
        }
        Err(err)
    }

    fn commit(&mut self) {
        self.committed = true;
        self.replay = Vec::new();
        self.rest.clear();
    }
}

/// Move every frame `d` has ready into `out`.
fn drain_into(d: &mut dyn Decoder, out: &mut VecDeque<Frame>) -> Result<()> {
    loop {
        match d.receive_frame() {
            Ok(f) => out.push_back(f),
            Err(Error::NeedMore) | Err(Error::Eof) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}

impl Decoder for FallbackDecoder {
    fn codec_id(&self) -> &CodecId {
        &self.codec_id
    }

    fn send_packet(&mut self, packet: &Packet) -> Result<()> {
        if self.committed {
            return self.current.send_packet(packet);
        }
        self.replay.push(packet.clone());
        let res = self.current.send_packet(packet);
        if self.replay.len() >= FALLBACK_REPLAY_PACKETS {
            self.commit();
            return res;
        }
        match res {
            Ok(()) => Ok(()),
            Err(Error::NeedMore) => Err(Error::NeedMore),
            Err(e) => self.fall_back(e),
        }
    }

    fn receive_frame(&mut self) -> Result<Frame> {
        if let Some(f) = self.pending.pop_front() {
            return Ok(f);
        }
        match self.current.receive_frame() {
            Ok(f) => {
                self.commit();
                Ok(f)
            }
            Err(e @ (Error::NeedMore | Error::Eof)) => Err(e),
            Err(e) if self.committed => Err(e),
            Err(e) => {
                self.fall_back(e)?;
                self.receive_frame()
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.flushed = true;
        match self.current.flush() {
            Err(e) if !self.committed => self.fall_back(e),
            other => other,
        }
    }

    fn reset(&mut self) -> Result<()> {
        self.pending.clear();
        self.replay.clear();
        self.flushed = false;
        self.current.reset()
    }

    fn set_execution_context(&mut self, ctx: &ExecutionContext) {
        self.ctx = Some(ctx.clone());
        self.current.set_execution_context(ctx);
    }

    fn output_pixel_format(&self) -> Option<PixelFormat> {
        self.current.output_pixel_format()
    }
}

/// Free-function shorthand for encoder construction with default prefs.
pub fn make_encoder(reg: &CodecRegistry, params: &CodecParameters) -> Result<Box<dyn Encoder>> {
    make_encoder_with(reg, params, &CodecPreferences::default())
}

/// Build an encoder for `params` honouring `prefs`. Same shape as
/// [`make_decoder_with`].
pub fn make_encoder_with(
    reg: &CodecRegistry,
    params: &CodecParameters,
    prefs: &CodecPreferences,
) -> Result<Box<dyn Encoder>> {
    if reg.implementations(&params.codec_id).is_empty() {
        return Err(Error::CodecNotFound(params.codec_id.to_string()));
    }
    let ranked = ranked_implementations(reg, params, prefs, Side::Encode, true);
    let mut last_err: Option<Error> = None;
    for imp in ranked {
        match (imp.make_encoder.unwrap())(params) {
            Ok(e) => return Ok(e),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        Error::CodecNotFound(format!(
            "no encoder for {} accepts the requested parameters",
            params.codec_id
        ))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn caps(name: &str, hw: bool) -> CodecCapabilities {
        CodecCapabilities::audio(name).with_hardware(hw)
    }

    use oxideav_core::{AudioFrame, CodecInfo, TimeBase};

    /// A "hardware" decoder whose session cannot be created: every
    /// packet fails.
    struct Refusing(CodecId);
    impl Decoder for Refusing {
        fn codec_id(&self) -> &CodecId {
            &self.0
        }
        fn send_packet(&mut self, _: &Packet) -> Result<()> {
            Err(Error::other("session refused"))
        }
        fn receive_frame(&mut self) -> Result<Frame> {
            Err(Error::NeedMore)
        }
        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// A software decoder: one frame per packet, carrying the packet's
    /// first byte as its sample count.
    struct Echo(CodecId, VecDeque<u32>);
    impl Decoder for Echo {
        fn codec_id(&self) -> &CodecId {
            &self.0
        }
        fn send_packet(&mut self, p: &Packet) -> Result<()> {
            self.1.push_back(u32::from(p.data[0]));
            Ok(())
        }
        fn receive_frame(&mut self) -> Result<Frame> {
            match self.1.pop_front() {
                Some(n) => Ok(Frame::Audio(AudioFrame {
                    samples: n,
                    pts: None,
                    data: Vec::new(),
                })),
                None => Err(Error::NeedMore),
            }
        }
        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    fn registry() -> CodecRegistry {
        let id = CodecId::new("x");
        let mut reg = CodecRegistry::new();
        reg.register(
            CodecInfo::new(id.clone())
                .capabilities(
                    CodecCapabilities::audio("x_hw")
                        .with_hardware(true)
                        .with_priority(1),
                )
                .decoder(|p| Ok(Box::new(Refusing(p.codec_id.clone())))),
        );
        reg.register(
            CodecInfo::new(id)
                .capabilities(CodecCapabilities::audio("x_sw").with_priority(100))
                .decoder(|p| Ok(Box::new(Echo(p.codec_id.clone(), VecDeque::new())))),
        );
        reg
    }

    fn samples_of(d: &mut dyn Decoder) -> Vec<u32> {
        let mut out = Vec::new();
        while let Ok(Frame::Audio(a)) = d.receive_frame() {
            out.push(a.samples);
        }
        out
    }

    #[test]
    fn a_hardware_session_refused_at_decode_time_falls_back_to_software() {
        let reg = registry();
        let params = CodecParameters::audio(CodecId::new("x"));
        let mut d = make_decoder(&reg, &params).unwrap();
        let tb = TimeBase::new(1, 1000);
        d.send_packet(&Packet::new(0, tb, vec![7])).unwrap();
        d.send_packet(&Packet::new(0, tb, vec![9])).unwrap();
        assert_eq!(samples_of(&mut *d), vec![7, 9]);
        d.send_packet(&Packet::new(0, tb, vec![3])).unwrap();
        assert_eq!(samples_of(&mut *d), vec![3]);
    }

    #[test]
    fn without_a_software_candidate_the_error_surfaces() {
        let reg = registry();
        let params = CodecParameters::audio(CodecId::new("x"));
        let prefs = CodecPreferences {
            require_hardware: true,
            ..Default::default()
        };
        let mut d = make_decoder_with(&reg, &params, &prefs).unwrap();
        let tb = TimeBase::new(1, 1000);
        assert!(d.send_packet(&Packet::new(0, tb, vec![1])).is_err());
    }

    #[test]
    fn no_hardware_excludes_hw_only() {
        let prefs = CodecPreferences {
            no_hardware: true,
            ..Default::default()
        };
        assert!(prefs.excludes(&caps("aac_audiotoolbox", true)));
        assert!(!prefs.excludes(&caps("aac_sw", false)));
    }

    #[test]
    fn require_hardware_excludes_sw_only() {
        let prefs = CodecPreferences {
            require_hardware: true,
            ..Default::default()
        };
        assert!(!prefs.excludes(&caps("aac_audiotoolbox", true)));
        assert!(prefs.excludes(&caps("aac_sw", false)));
    }

    #[test]
    fn no_hardware_and_require_hardware_excludes_everything() {
        let prefs = CodecPreferences {
            no_hardware: true,
            require_hardware: true,
            ..Default::default()
        };
        assert!(prefs.excludes(&caps("aac_audiotoolbox", true)));
        assert!(prefs.excludes(&caps("aac_sw", false)));
    }

    /// An encoder that accepts nothing (never fed in these tests).
    struct Nop(CodecParameters);
    impl Encoder for Nop {
        fn codec_id(&self) -> &CodecId {
            &self.0.codec_id
        }
        fn output_params(&self) -> &CodecParameters {
            &self.0
        }
        fn send_frame(&mut self, _: &Frame) -> Result<()> {
            Err(Error::unsupported("nop"))
        }
        fn receive_packet(&mut self) -> Result<Packet> {
            Err(Error::NeedMore)
        }
        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
    }

    /// Two encoders for `x`, registered hardware-first the way a host
    /// with a system bridge registers them: the bridge declares no
    /// input restrictions, the software one takes 48 kHz only.
    fn encoder_registry() -> CodecRegistry {
        let id = CodecId::new("x");
        let mut reg = CodecRegistry::new();
        reg.register(
            CodecInfo::new(id.clone())
                .capabilities(
                    CodecCapabilities::audio("x_hw")
                        .with_hardware(true)
                        .with_priority(10),
                )
                .encoder(|p| Ok(Box::new(Nop(p.clone())))),
        );
        reg.register(
            CodecInfo::new(id)
                .capabilities(
                    CodecCapabilities::audio("x_sw")
                        .with_priority(100)
                        .with_sample_rates(vec![48_000]),
                )
                .encoder(|p| Ok(Box::new(Nop(p.clone())))),
        );
        reg
    }

    fn at_44100() -> CodecParameters {
        let mut p = CodecParameters::audio(CodecId::new("x"));
        p.sample_rate = Some(44_100);
        p.channels = Some(2);
        p
    }

    #[test]
    fn encoder_caps_follow_the_preference_order_not_registration_order() {
        let reg = encoder_registry();
        let params = at_44100();
        // Registered priorities: the bridge wins.
        let caps = preferred_encoder_caps(&reg, &params, &CodecPreferences::default()).unwrap();
        assert_eq!(caps.implementation, "x_hw");
        assert!(caps.accepted_sample_rates.is_empty());
        // Software-first (what a CLI's default preferences express):
        // the caps are the software encoder's, whose rate list is what
        // plans the resample stage in front of it.
        let software_first = CodecPreferences {
            prefer: vec!["x_sw".into()],
            boost: 1_000_000,
            ..Default::default()
        };
        let caps = preferred_encoder_caps(&reg, &params, &software_first).unwrap();
        assert_eq!(caps.implementation, "x_sw");
        assert_eq!(caps.accepted_sample_rates, vec![48_000]);
        // `no_hardware` excludes the bridge outright.
        let no_hw = CodecPreferences {
            no_hardware: true,
            ..Default::default()
        };
        assert_eq!(
            preferred_encoder_caps(&reg, &params, &no_hw)
                .unwrap()
                .implementation,
            "x_sw"
        );
        // `exclude` by name.
        let excluded = CodecPreferences {
            exclude: vec!["x_hw".into()],
            ..Default::default()
        };
        assert_eq!(
            preferred_encoder_caps(&reg, &params, &excluded)
                .unwrap()
                .implementation,
            "x_sw"
        );
        // Everything excluded: no answer, same as `make_encoder_with`.
        let nothing = CodecPreferences {
            no_hardware: true,
            require_hardware: true,
            ..Default::default()
        };
        assert!(preferred_encoder_caps(&reg, &params, &nothing).is_none());
        assert!(
            preferred_encoder_caps(&reg, &CodecParameters::audio(CodecId::new("y")), &nothing)
                .is_none()
        );
    }

    #[test]
    fn encoder_caps_and_the_built_encoder_name_the_same_implementation() {
        let reg = encoder_registry();
        let params = at_44100();
        for prefs in [
            CodecPreferences::default(),
            CodecPreferences {
                prefer: vec!["x_sw".into()],
                boost: 1_000_000,
                ..Default::default()
            },
            CodecPreferences {
                no_hardware: true,
                ..Default::default()
            },
        ] {
            let caps = preferred_encoder_caps(&reg, &params, &prefs).unwrap();
            let built = make_encoder_with(&reg, &params, &prefs).unwrap();
            // `Nop` publishes its input parameters; the implementation
            // that was built is the first one the ranking names.
            assert_eq!(built.codec_id(), &params.codec_id);
            let first = ranked_implementations(&reg, &params, &prefs, Side::Encode, true)[0];
            assert_eq!(first.caps.implementation, caps.implementation);
        }
    }

    #[test]
    fn unfitting_parameters_fall_back_to_the_best_ranked_candidate() {
        let id = CodecId::new("x");
        let mut reg = CodecRegistry::new();
        reg.register(
            CodecInfo::new(id)
                .capabilities(
                    CodecCapabilities::audio("x_narrow")
                        .with_priority(100)
                        .with_max_channels(2)
                        .with_sample_rates(vec![48_000]),
                )
                .encoder(|p| Ok(Box::new(Nop(p.clone())))),
        );
        let mut params = at_44100();
        params.channels = Some(6);
        // Nothing fits a 5.1 source, yet the caps name the encoder the
        // downmix is planned for.
        let caps = preferred_encoder_caps(&reg, &params, &CodecPreferences::default()).unwrap();
        assert_eq!(caps.implementation, "x_narrow");
        assert_eq!(caps.max_channels, Some(2));
    }

    #[test]
    fn decoder_caps_follow_the_preference_order() {
        let reg = registry();
        let params = CodecParameters::audio(CodecId::new("x"));
        assert_eq!(
            preferred_decoder_caps(&reg, &params, &CodecPreferences::default())
                .unwrap()
                .implementation,
            "x_hw"
        );
        let software_first = CodecPreferences {
            prefer: vec!["x_sw".into()],
            boost: 1_000_000,
            ..Default::default()
        };
        assert_eq!(
            preferred_decoder_caps(&reg, &params, &software_first)
                .unwrap()
                .implementation,
            "x_sw"
        );
    }
}
