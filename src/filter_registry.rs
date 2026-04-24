//! Named-filter registry and legacy-trait adapters.
//!
//! This module is the single source of truth for "given a filter name +
//! JSON params + upstream [`PortSpec`]s, construct a [`StreamFilter`]
//! ready to wire into the pipeline". It replaces the hand-coded
//! `match`-on-filter-name dispatch that previously lived in
//! `executor.rs`.
//!
//! Two audiences:
//!
//! 1. **Native [`StreamFilter`] implementors** (e.g.
//!    [`oxideav_audio_filter::Spectrogram`]) — registered directly via
//!    their factory.
//! 2. **Legacy single-kind filters** that still implement the older
//!    [`AudioFilter`](oxideav_audio_filter::AudioFilter) or
//!    [`ImageFilter`](oxideav_image_filter::ImageFilter) traits —
//!    wrapped in [`AudioFilterAdapter`] / [`ImageFilterAdapter`] so the
//!    executor only ever sees `StreamFilter`.
//!
//! The legacy adapters are 1-in / 1-out with a single matching media
//! kind; they remain the majority of builtins until each filter crate
//! migrates to the native trait.

#[cfg(feature = "audio_filter")]
use oxideav_audio_filter::AudioFilter;
use oxideav_core::{
    filter::unknown_filter_error, Error, FilterContext, Frame, PixelFormat, PortParams, PortSpec,
    Result, SampleFormat, StreamFilter, TimeBase,
};
#[cfg(feature = "image_filter")]
use oxideav_image_filter::ImageFilter;
use serde_json::Value;
use std::collections::HashMap;

/// Factory for a named filter. The registry invokes this with the
/// caller-supplied JSON `params` and the input port specs resolved
/// from upstream. Factories are free to inspect the upstream port
/// params — e.g. `Resample` reads `inputs[0]` to learn the source
/// sample rate.
pub type FilterFactory =
    Box<dyn Fn(&Value, &[PortSpec]) -> Result<Box<dyn StreamFilter>> + Send + Sync>;

/// Named-filter registry. Construct with [`FilterRegistry::new`] (empty)
/// or [`FilterRegistry::default`] / [`FilterRegistry::with_builtins`]
/// for the shipped filters.
pub struct FilterRegistry {
    factories: HashMap<String, FilterFactory>,
}

impl FilterRegistry {
    /// Empty registry — no filters resolvable until [`register`] is
    /// called.
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Shorthand for `let mut r = FilterRegistry::new();
    /// r.register_builtins(); r`.
    pub fn with_builtins() -> Self {
        let mut r = Self::new();
        r.register_builtins();
        r
    }

    /// Register a factory under `name`. Overwrites any existing entry
    /// with the same name — last write wins.
    pub fn register(&mut self, name: &str, factory: FilterFactory) {
        self.factories.insert(name.to_string(), factory);
    }

    /// True when a filter is registered under `name`.
    pub fn contains(&self, name: &str) -> bool {
        self.factories.contains_key(name)
    }

    /// Instantiate the named filter. Returns [`Error::Unsupported`]
    /// for unregistered names.
    pub fn make(
        &self,
        name: &str,
        params: &Value,
        inputs: &[PortSpec],
    ) -> Result<Box<dyn StreamFilter>> {
        let bare = strip_filter_prefix(name);
        let factory = self
            .factories
            .get(bare)
            .or_else(|| self.factories.get(name))
            .ok_or_else(|| unknown_filter_error(name))?;
        factory(params, inputs)
    }

    /// Register every filter that ships with the workspace. Idempotent
    /// in practice (all factories are re-insertable).
    pub fn register_builtins(&mut self) {
        #[cfg(feature = "audio_filter")]
        {
            self.register("volume", Box::new(make_volume));
            self.register("noise_gate", Box::new(make_noise_gate));
            self.register("echo", Box::new(make_echo));
            self.register("resample", Box::new(make_resample));
            self.register("spectrogram", Box::new(make_spectrogram));
        }
        #[cfg(feature = "image_filter")]
        {
            self.register("blur", Box::new(make_blur));
            self.register("edge", Box::new(make_edge));
            self.register("resize", Box::new(make_resize));
        }
    }
}

impl Default for FilterRegistry {
    fn default() -> Self {
        Self::with_builtins()
    }
}

/// Strip `video.`, `v:`, `audio.`, `a:` prefixes — the schema allows
/// them for disambiguation; the registry doesn't care.
fn strip_filter_prefix(name: &str) -> &str {
    name.strip_prefix("video.")
        .or_else(|| name.strip_prefix("v:"))
        .or_else(|| name.strip_prefix("audio."))
        .or_else(|| name.strip_prefix("a:"))
        .unwrap_or(name)
}

// ───────────────────────── adapters ─────────────────────────

/// Wraps a legacy [`AudioFilter`] in the [`StreamFilter`] contract.
/// Single audio port in, single audio port out; both inherit params
/// from the upstream input port.
#[cfg(feature = "audio_filter")]
pub(crate) struct AudioFilterAdapter {
    inner: Box<dyn AudioFilter>,
    inp: [PortSpec; 1],
    outp: [PortSpec; 1],
}

#[cfg(feature = "audio_filter")]
impl AudioFilterAdapter {
    fn new(inner: Box<dyn AudioFilter>, in_port: PortSpec, out_port: PortSpec) -> Self {
        Self {
            inner,
            inp: [in_port],
            outp: [out_port],
        }
    }
}

#[cfg(feature = "audio_filter")]
impl StreamFilter for AudioFilterAdapter {
    fn input_ports(&self) -> &[PortSpec] {
        &self.inp
    }
    fn output_ports(&self) -> &[PortSpec] {
        &self.outp
    }
    fn push(&mut self, ctx: &mut dyn FilterContext, port: usize, frame: &Frame) -> Result<()> {
        if port != 0 {
            return Err(Error::invalid(format!(
                "audio-filter adapter: unknown input port {port}"
            )));
        }
        let Frame::Audio(a) = frame else {
            return Err(Error::invalid(
                "audio-filter adapter: input port 0 only accepts audio frames",
            ));
        };
        let outs = self.inner.process(a)?;
        for o in outs {
            ctx.emit(0, Frame::Audio(o))?;
        }
        Ok(())
    }
    fn flush(&mut self, ctx: &mut dyn FilterContext) -> Result<()> {
        let outs = self.inner.flush()?;
        for o in outs {
            ctx.emit(0, Frame::Audio(o))?;
        }
        Ok(())
    }
}

/// Wraps a legacy [`ImageFilter`] in the [`StreamFilter`] contract.
/// Single video port in, single video port out — the output port is
/// synthesised with the caller-supplied params (which may carry the
/// resized / format-changed output shape when the filter rewrites
/// dimensions).
#[cfg(feature = "image_filter")]
pub(crate) struct ImageFilterAdapter {
    inner: Box<dyn ImageFilter>,
    inp: [PortSpec; 1],
    outp: [PortSpec; 1],
}

#[cfg(feature = "image_filter")]
impl ImageFilterAdapter {
    fn new(inner: Box<dyn ImageFilter>, in_port: PortSpec, out_port: PortSpec) -> Self {
        Self {
            inner,
            inp: [in_port],
            outp: [out_port],
        }
    }
}

#[cfg(feature = "image_filter")]
impl StreamFilter for ImageFilterAdapter {
    fn input_ports(&self) -> &[PortSpec] {
        &self.inp
    }
    fn output_ports(&self) -> &[PortSpec] {
        &self.outp
    }
    fn push(&mut self, ctx: &mut dyn FilterContext, port: usize, frame: &Frame) -> Result<()> {
        if port != 0 {
            return Err(Error::invalid(format!(
                "image-filter adapter: unknown input port {port}"
            )));
        }
        let Frame::Video(v) = frame else {
            return Err(Error::invalid(
                "image-filter adapter: input port 0 only accepts video frames",
            ));
        };
        let out = self.inner.apply(v)?;
        ctx.emit(0, Frame::Video(out))?;
        Ok(())
    }
}

// ───────────────────────── factories ─────────────────────────

/// Pull the single audio port spec from `inputs`, or fall back to a
/// sane default if none is provided (allows registry unit tests that
/// don't have an upstream to skip port plumbing).
#[cfg(feature = "audio_filter")]
fn audio_in_port(inputs: &[PortSpec]) -> PortSpec {
    inputs
        .iter()
        .find(|p| matches!(p.params, PortParams::Audio { .. }))
        .cloned()
        .unwrap_or_else(|| PortSpec::audio("in", 48_000, 2, SampleFormat::F32))
}

#[cfg(feature = "image_filter")]
fn video_in_port(inputs: &[PortSpec]) -> PortSpec {
    inputs
        .iter()
        .find(|p| matches!(p.params, PortParams::Video { .. }))
        .cloned()
        .unwrap_or_else(|| PortSpec::video("in", 0, 0, PixelFormat::Yuv420P, TimeBase::new(1, 30)))
}

#[cfg(feature = "audio_filter")]
fn make_volume(params: &Value, inputs: &[PortSpec]) -> Result<Box<dyn StreamFilter>> {
    use oxideav_audio_filter::Volume;
    let p = params.as_object();
    let get_f64 = |k: &str| p.and_then(|m| m.get(k)).and_then(|v| v.as_f64());
    let volume = if let Some(db) = get_f64("gain_db") {
        let linear = 10f32.powf((db as f32) / 20.0);
        Volume::new(linear)
    } else if let Some(g) = get_f64("gain") {
        Volume::new(g as f32)
    } else {
        return Err(Error::invalid(
            "job: filter 'volume' needs `gain` or `gain_db`",
        ));
    };
    let in_port = audio_in_port(inputs);
    let out_port = PortSpec {
        name: "audio".to_string(),
        ..in_port.clone()
    };
    Ok(Box::new(AudioFilterAdapter::new(
        Box::new(volume),
        in_port,
        out_port,
    )))
}

#[cfg(feature = "audio_filter")]
fn make_noise_gate(params: &Value, inputs: &[PortSpec]) -> Result<Box<dyn StreamFilter>> {
    use oxideav_audio_filter::NoiseGate;
    let p = params.as_object();
    let get_f64 = |k: &str, dflt: f64| {
        p.and_then(|m| m.get(k))
            .and_then(|v| v.as_f64())
            .unwrap_or(dflt)
    };
    let gate = NoiseGate::new(
        get_f64("threshold_db", -40.0) as f32,
        get_f64("attack_ms", 10.0) as f32,
        get_f64("release_ms", 100.0) as f32,
        get_f64("hold_ms", 50.0) as f32,
    );
    let in_port = audio_in_port(inputs);
    let out_port = PortSpec {
        name: "audio".to_string(),
        ..in_port.clone()
    };
    Ok(Box::new(AudioFilterAdapter::new(
        Box::new(gate),
        in_port,
        out_port,
    )))
}

#[cfg(feature = "audio_filter")]
fn make_echo(params: &Value, inputs: &[PortSpec]) -> Result<Box<dyn StreamFilter>> {
    use oxideav_audio_filter::Echo;
    let p = params.as_object();
    let get_f64 = |k: &str, dflt: f64| {
        p.and_then(|m| m.get(k))
            .and_then(|v| v.as_f64())
            .unwrap_or(dflt)
    };
    let e = Echo::new(
        get_f64("delay_ms", 250.0) as f32,
        get_f64("feedback", 0.35) as f32,
        get_f64("mix", 0.5) as f32,
    );
    let in_port = audio_in_port(inputs);
    let out_port = PortSpec {
        name: "audio".to_string(),
        ..in_port.clone()
    };
    Ok(Box::new(AudioFilterAdapter::new(
        Box::new(e),
        in_port,
        out_port,
    )))
}

#[cfg(feature = "audio_filter")]
fn make_resample(params: &Value, inputs: &[PortSpec]) -> Result<Box<dyn StreamFilter>> {
    use oxideav_audio_filter::Resample;
    let p = params.as_object();
    let dst_rate = p
        .and_then(|m| m.get("rate"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| Error::invalid("job: filter 'resample' needs `rate` (output sample rate)"))?
        as u32;
    let in_port = audio_in_port(inputs);
    let (src_rate, channels, format) = match &in_port.params {
        PortParams::Audio {
            sample_rate,
            channels,
            format,
        } => (*sample_rate, *channels, *format),
        _ => (48_000, 2, SampleFormat::F32),
    };
    let filter = Resample::new(src_rate, dst_rate)?;
    let out_port = PortSpec::audio("audio", dst_rate, channels, format);
    Ok(Box::new(AudioFilterAdapter::new(
        Box::new(filter),
        in_port,
        out_port,
    )))
}

#[cfg(feature = "audio_filter")]
fn make_spectrogram(params: &Value, inputs: &[PortSpec]) -> Result<Box<dyn StreamFilter>> {
    use oxideav_audio_filter::spectrogram::{Colormap, Spectrogram, SpectrogramOptions, Window};
    let p = params.as_object();
    let get_u64 = |k: &str| p.and_then(|m| m.get(k)).and_then(|v| v.as_u64());
    let get_f64 = |k: &str| p.and_then(|m| m.get(k)).and_then(|v| v.as_f64());
    let get_str = |k: &str| p.and_then(|m| m.get(k)).and_then(|v| v.as_str());

    let mut opts = SpectrogramOptions::default();
    if let Some(v) = get_u64("fft_size") {
        opts.fft_size = v as usize;
    }
    if let Some(v) = get_u64("hop_size") {
        opts.hop_size = v as usize;
    }
    if let Some(v) = get_u64("width") {
        opts.width = v as u32;
    }
    if let Some(v) = get_u64("height") {
        opts.height = v as u32;
    }
    opts.window = match get_str("window") {
        Some("hamming") => Window::Hamming,
        Some("blackman") => Window::Blackman,
        _ => Window::Hann,
    };
    opts.colormap = match get_str("colormap") {
        Some("grayscale") | Some("gray") => Colormap::Grayscale,
        Some("magma") => Colormap::Magma,
        _ => Colormap::Viridis,
    };
    if let Some(lo) = get_f64("db_low") {
        opts.db_range.0 = lo as f32;
    }
    if let Some(hi) = get_f64("db_high") {
        opts.db_range.1 = hi as f32;
    }
    let fps = get_u64("fps").unwrap_or(30) as u32;
    let _ = inputs;
    let s = Spectrogram::new(opts)?.with_video_fps(fps);
    Ok(Box::new(s) as Box<dyn StreamFilter>)
}

#[cfg(feature = "image_filter")]
fn make_blur(params: &Value, inputs: &[PortSpec]) -> Result<Box<dyn StreamFilter>> {
    use oxideav_image_filter::{Blur, Planes};
    let p = params.as_object();
    let get_u64 = |k: &str| p.and_then(|m| m.get(k)).and_then(|v| v.as_u64());
    let get_f64 = |k: &str| p.and_then(|m| m.get(k)).and_then(|v| v.as_f64());
    let get_str = |k: &str| p.and_then(|m| m.get(k)).and_then(|v| v.as_str());

    let radius = get_u64("radius").unwrap_or(3) as u32;
    let planes = match get_str("planes") {
        Some("luma") => Planes::Luma,
        Some("chroma") => Planes::Chroma,
        Some(other) if other != "all" => {
            return Err(Error::invalid(format!(
                "job: filter 'blur': unknown planes '{other}' (expected 'all', 'luma', or 'chroma')"
            )));
        }
        _ => Planes::All,
    };
    let mut f = Blur::new(radius).with_planes(planes);
    if let Some(s) = get_f64("sigma") {
        f = f.with_sigma(s as f32);
    }
    let in_port = video_in_port(inputs);
    let out_port = PortSpec {
        name: "video".to_string(),
        ..in_port.clone()
    };
    Ok(Box::new(ImageFilterAdapter::new(
        Box::new(f),
        in_port,
        out_port,
    )))
}

#[cfg(feature = "image_filter")]
fn make_edge(_params: &Value, inputs: &[PortSpec]) -> Result<Box<dyn StreamFilter>> {
    use oxideav_image_filter::Edge;
    let f = Edge::new();
    let in_port = video_in_port(inputs);
    // Edge collapses any input to single-plane luma.
    let out_port = match &in_port.params {
        PortParams::Video {
            width,
            height,
            time_base,
            ..
        } => PortSpec::video("video", *width, *height, PixelFormat::Gray8, *time_base),
        _ => PortSpec::video("video", 0, 0, PixelFormat::Gray8, TimeBase::new(1, 30)),
    };
    Ok(Box::new(ImageFilterAdapter::new(
        Box::new(f),
        in_port,
        out_port,
    )))
}

#[cfg(feature = "image_filter")]
fn make_resize(params: &Value, inputs: &[PortSpec]) -> Result<Box<dyn StreamFilter>> {
    use oxideav_image_filter::{Interpolation, Resize};
    let p = params.as_object();
    let get_u64 = |k: &str| p.and_then(|m| m.get(k)).and_then(|v| v.as_u64());
    let get_str = |k: &str| p.and_then(|m| m.get(k)).and_then(|v| v.as_str());

    let w = get_u64("width")
        .ok_or_else(|| Error::invalid("job: filter 'resize' needs unsigned `width`"))?
        as u32;
    let h = get_u64("height")
        .ok_or_else(|| Error::invalid("job: filter 'resize' needs unsigned `height`"))?
        as u32;
    let interp = match get_str("interpolation") {
        Some("nearest") => Interpolation::Nearest,
        None | Some("bilinear") => Interpolation::Bilinear,
        Some(other) => {
            return Err(Error::invalid(format!(
                "job: filter 'resize': unknown interpolation '{other}' \
                 (expected 'nearest' or 'bilinear')"
            )));
        }
    };
    let f = Resize::new(w, h).with_interpolation(interp);
    let in_port = video_in_port(inputs);
    let out_port = match &in_port.params {
        PortParams::Video {
            format, time_base, ..
        } => PortSpec::video("video", w, h, *format, *time_base),
        _ => PortSpec::video("video", w, h, PixelFormat::Yuv420P, TimeBase::new(1, 30)),
    };
    Ok(Box::new(ImageFilterAdapter::new(
        Box::new(f),
        in_port,
        out_port,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxideav_core::{AudioFrame, SampleFormat};

    #[test]
    fn registry_dispatches_known_filter() {
        let r = FilterRegistry::with_builtins();
        let inputs = vec![PortSpec::audio("in", 48_000, 2, SampleFormat::F32)];
        let f = r
            .make("volume", &serde_json::json!({"gain": 0.5}), &inputs)
            .expect("registry should resolve 'volume'");
        assert_eq!(f.input_ports().len(), 1);
        assert_eq!(f.output_ports().len(), 1);
    }

    #[test]
    fn registry_rejects_unknown_filter() {
        let r = FilterRegistry::with_builtins();
        let inputs: Vec<PortSpec> = Vec::new();
        let err = match r.make(
            "definitely-not-a-real-filter",
            &serde_json::json!({}),
            &inputs,
        ) {
            Ok(_) => panic!("unknown filter must fail"),
            Err(e) => e,
        };
        let msg = format!("{err:?}");
        assert!(
            msg.contains("unknown filter") || msg.contains("Unsupported"),
            "unexpected: {msg}"
        );
    }

    #[test]
    fn registry_accepts_kind_prefix_in_filter_name() {
        let r = FilterRegistry::with_builtins();
        let inputs = vec![PortSpec::video(
            "in",
            16,
            16,
            PixelFormat::Yuv420P,
            TimeBase::new(1, 30),
        )];
        // `video.blur` / `v:blur` both route to the `blur` factory.
        r.make("video.blur", &serde_json::json!({}), &inputs)
            .unwrap();
        r.make("v:blur", &serde_json::json!({}), &inputs).unwrap();
    }

    #[cfg(feature = "audio_filter")]
    #[test]
    fn registry_accepts_spectrogram() {
        let r = FilterRegistry::with_builtins();
        let inputs = vec![PortSpec::audio("in", 44_100, 1, SampleFormat::F32)];
        let f = r
            .make(
                "spectrogram",
                &serde_json::json!({"width": 64, "height": 32, "fft_size": 256, "hop_size": 64}),
                &inputs,
            )
            .unwrap();
        assert_eq!(f.output_ports().len(), 2);
        assert!(matches!(
            &f.output_ports()[1].params,
            PortParams::Video { .. }
        ));
    }

    #[cfg(feature = "audio_filter")]
    #[test]
    fn audio_filter_adapter_preserves_process_semantics() {
        use oxideav_audio_filter::{AudioFilter, Volume};
        // Build a direct Volume instance.
        let mut direct = Volume::new(0.5);
        // Build the same filter through the adapter.
        let r = FilterRegistry::with_builtins();
        let inputs = vec![PortSpec::audio("in", 48_000, 1, SampleFormat::F32)];
        let mut wrapped = r
            .make("volume", &serde_json::json!({"gain": 0.5}), &inputs)
            .unwrap();

        // A short sine burst, mono f32.
        let n = 128usize;
        let mut bytes = Vec::with_capacity(n * 4);
        for i in 0..n {
            let s = (2.0 * std::f32::consts::PI * 440.0 * i as f32 / 48_000.0).sin() * 0.25;
            bytes.extend_from_slice(&s.to_le_bytes());
        }
        let frame = AudioFrame {
            format: SampleFormat::F32,
            sample_rate: 48_000,
            channels: 1,
            samples: n as u32,
            pts: None,
            time_base: TimeBase::new(1, 48_000),
            data: vec![bytes],
        };

        let direct_outs = direct.process(&frame).unwrap();
        assert_eq!(direct_outs.len(), 1);

        struct Collect {
            out: Vec<Frame>,
        }
        impl FilterContext for Collect {
            fn emit(&mut self, _port: usize, frame: Frame) -> Result<()> {
                self.out.push(frame);
                Ok(())
            }
        }
        let mut ctx = Collect { out: Vec::new() };
        wrapped.push(&mut ctx, 0, &Frame::Audio(frame)).unwrap();
        assert_eq!(ctx.out.len(), 1);
        let Frame::Audio(got) = &ctx.out[0] else {
            panic!("expected audio out");
        };
        assert_eq!(got.data, direct_outs[0].data);
    }
}
