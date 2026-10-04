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
    let candidates = reg.implementations(&params.codec_id);
    if candidates.is_empty() {
        return Err(Error::CodecNotFound(params.codec_id.to_string()));
    }
    let mut ranked: Vec<&CodecImplementation> = candidates
        .iter()
        .filter(|i| i.make_decoder.is_some() && !prefs.excludes(&i.caps))
        .filter(|i| i.caps.fits_params(params, false))
        .collect();
    ranked.sort_by_key(|i| prefs.effective_priority(&i.caps));
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
    let candidates = reg.implementations(&params.codec_id);
    if candidates.is_empty() {
        return Err(Error::CodecNotFound(params.codec_id.to_string()));
    }
    let mut ranked: Vec<&CodecImplementation> = candidates
        .iter()
        .filter(|i| i.make_encoder.is_some() && !prefs.excludes(&i.caps))
        .filter(|i| i.caps.fits_params(params, true))
        .collect();
    ranked.sort_by_key(|i| prefs.effective_priority(&i.caps));
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
}
