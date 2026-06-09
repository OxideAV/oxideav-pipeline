//! Schema for the JSON job graph + serde (de)serialisation.
//!
//! The top-level document is a JSON object. Keys that start with `@` define
//! named aliases consumable by other entries; all other keys are treated as
//! output sinks (file paths or reserved sink names like `@null`).
//!
//! `TrackInput` is the recursive node type — each filter takes exactly one
//! upstream input today (multi-input fan-in is a future extension).

use indexmap::IndexMap;
use oxideav_core::{Error, MediaType, PixelFormat, Result};
use serde::{Deserialize, Serialize};

/// Top-level job: a set of named outputs + aliases.
#[derive(Clone, Debug, Default)]
pub struct Job {
    /// Output targets keyed by filename or reserved sink name (`@null`,
    /// `@display`, `@out`).
    pub outputs: IndexMap<String, OutputSpec>,
    /// Named intermediate aliases (keys starting with `@` that are not
    /// reserved sink names).
    pub aliases: IndexMap<String, OutputSpec>,
    /// Advisory thread budget for the executor. `None` = auto-detect
    /// (use the number of logical CPUs). `Some(1)` forces the serial
    /// executor; `Some(n)` with n ≥ 2 requests pipelined execution.
    /// Explicit CLI overrides (`Executor::with_threads`) take precedence.
    pub threads: Option<usize>,
}

/// Per-file/per-alias spec: track lists grouped by media type.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OutputSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub audio: Vec<TrackSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub video: Vec<TrackSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub subtitle: Vec<TrackSpec>,
    /// Tracks that should be pulled across media types. Resolved to
    /// kind-specific lists at DAG-build time.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub all: Vec<TrackSpec>,
}

impl OutputSpec {
    /// True when no tracks at all are declared — an error at validation time.
    pub fn is_empty(&self) -> bool {
        self.audio.is_empty()
            && self.video.is_empty()
            && self.subtitle.is_empty()
            && self.all.is_empty()
    }
}

/// A single track: an input chain plus optional encoder settings.
///
/// We do not use `deny_unknown_fields` here because `#[serde(flatten)]` on
/// `input` lifts either `SourceRef` or `FilterNode` fields up to the track
/// level — strict rejection wouldn't distinguish them from truly unknown
/// keys. The builder still catches empty / inconsistent specs in the DAG
/// resolve step.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TrackSpec {
    /// Recursive input (source or filter). We flatten so callers can write
    /// either `{"from": ...}` or `{"filter": ..., "input": ...}` directly
    /// on the track.
    #[serde(flatten)]
    pub input: TrackInput,
    /// Output codec id (e.g. `"h264"`, `"flac"`). If omitted the track is
    /// stream-copied — only valid when the upstream directly resolves to a
    /// demuxer packet of a codec the target muxer accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<String>,
    /// Codec-specific tuning (e.g. `{"crf": 23}`). Opaque to the schema —
    /// codec crates interpret their own keys. Named `codec_params` rather
    /// than `params` so it cannot collide with a flattened filter's
    /// `params` when the track itself is a filter node.
    #[serde(
        default,
        rename = "codec_params",
        skip_serializing_if = "is_null_or_empty"
    )]
    pub params: serde_json::Value,
    /// Optional stream filter applied after the upstream source/filter
    /// emits N streams.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_selector: Option<StreamSelector>,
}

/// Recursive input node.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TrackInput {
    /// `{"from": "path-or-@alias"}`.
    Source(SourceRef),
    /// `{"convert": "yuv420p", "input": <TrackInput>}`.
    ///
    /// Explicit pixel-format conversion node. Parsed before `Filter` in the
    /// untagged-enum dispatch so the `convert` key wins over a hypothetical
    /// `filter: "convert"` (not used today, but keeps the routing honest).
    Convert(ConvertNode),
    /// 3D-asset source: open `source` (URI / path) as a Scene3D via the
    /// caller-supplied Mesh3D registry, then rasterise via the renderer
    /// named by `backend` (e.g. `"scanline"`). The executor handles this
    /// by calling the user-installed
    /// [`crate::executor::RenderSourceFactory`] closure on the
    /// [`crate::executor::Executor`] — pipeline does not depend on any
    /// specific renderer crate; the consumer installs the factory at
    /// runtime.
    ///
    /// In JSON jobs this is `{ "render3d": "<uri>", "backend": "scanline",
    /// "opts": { ... } }`. `opts` is opaque to pipeline — it's
    /// round-tripped verbatim through the factory.
    ///
    /// Listed before `Filter` in the untagged dispatch so the unique
    /// `render3d` discriminator field is matched before serde considers
    /// the `filter` shape.
    Render3D(Render3DNode),
    /// `{"filter": "name", "params": {...}, "input": <TrackInput>}`.
    Filter(FilterNode),
}

/// Leaf input: either a file path or an alias reference.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SourceRef {
    /// Filename opened via the source registry, or `@alias` referencing
    /// another top-level entry.
    pub from: String,
}

/// Filter node — single-input for now.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FilterNode {
    /// Filter id. Unknown filters error at DAG-build, not parse — so the
    /// caller can still report a precise location.
    pub filter: String,
    /// Filter-specific parameters (opaque JSON).
    #[serde(default, skip_serializing_if = "is_null_or_empty")]
    pub params: serde_json::Value,
    /// Upstream node.
    pub input: Box<TrackInput>,
}

/// Explicit pixel-format conversion node.
///
/// The `convert` field carries an ffmpeg-style pixel format name
/// (`yuv420p`, `rgb24`, `rgba`, `pal8`, `gray8`, `nv12`, `rgb48le`, …).
/// Names are accepted case-insensitively and parsed into
/// [`oxideav_core::PixelFormat`] at DAG-build time — unknown names error
/// there, not at JSON parse time, so the error can point at the track
/// context.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ConvertNode {
    /// Target pixel format, as an ffmpeg-style string (`yuv420p`, etc.).
    pub convert: String,
    /// Upstream node.
    pub input: Box<TrackInput>,
}

/// [`TrackInput::Render3D`] payload.
///
/// JSON shape: `{ "render3d": "<uri>", "backend": "<name>", "opts": { ... } }`.
/// The `render3d` field serves as the discriminator for the untagged
/// [`TrackInput`] enum — see the variant docs for details.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Render3DNode {
    /// Source URI / path for the 3D scene file.
    #[serde(rename = "render3d")]
    pub source: String,
    /// Backend name, e.g. `"scanline"`. Looked up at runtime via the
    /// installed
    /// [`RenderSourceFactory`](crate::executor::RenderSourceFactory).
    pub backend: String,
    /// Opaque opts JSON; round-tripped verbatim through the factory.
    /// The consumer's factory deserialises this back into its own
    /// renderer options struct.
    #[serde(default, skip_serializing_if = "is_null_or_empty")]
    pub opts: serde_json::Value,
}

impl TrackInput {
    /// Discriminator tag for diagnostics. Stable string per variant —
    /// useful for log lines and error messages that need to name the
    /// node shape without printing the entire enum via `{:?}` (which
    /// recurses through the upstream tree).
    pub fn kind_str(&self) -> &'static str {
        match self {
            TrackInput::Source(_) => "source",
            TrackInput::Filter(_) => "filter",
            TrackInput::Convert(_) => "convert",
            TrackInput::Render3D(_) => "render3d",
        }
    }

    /// `true` when the node is a [`TrackInput::Source`] leaf.
    pub fn is_source(&self) -> bool {
        matches!(self, TrackInput::Source(_))
    }

    /// `true` when the node is a [`TrackInput::Filter`] wrapper.
    pub fn is_filter(&self) -> bool {
        matches!(self, TrackInput::Filter(_))
    }

    /// `true` when the node is a [`TrackInput::Convert`] wrapper.
    pub fn is_convert(&self) -> bool {
        matches!(self, TrackInput::Convert(_))
    }

    /// `true` when the node is a [`TrackInput::Render3D`] leaf.
    pub fn is_render3d(&self) -> bool {
        matches!(self, TrackInput::Render3D(_))
    }

    /// Borrow the [`SourceRef`] payload if this node is a `Source`,
    /// else `None`. Returns the inner reference so callers can read
    /// `from` without re-matching.
    pub fn as_source(&self) -> Option<&SourceRef> {
        match self {
            TrackInput::Source(s) => Some(s),
            _ => None,
        }
    }

    /// Borrow the [`FilterNode`] payload if this node is a `Filter`,
    /// else `None`.
    pub fn as_filter(&self) -> Option<&FilterNode> {
        match self {
            TrackInput::Filter(f) => Some(f),
            _ => None,
        }
    }

    /// Borrow the [`ConvertNode`] payload if this node is a `Convert`,
    /// else `None`.
    pub fn as_convert(&self) -> Option<&ConvertNode> {
        match self {
            TrackInput::Convert(c) => Some(c),
            _ => None,
        }
    }

    /// Borrow the [`Render3DNode`] payload if this node is a
    /// `Render3D`, else `None`.
    pub fn as_render3d(&self) -> Option<&Render3DNode> {
        match self {
            TrackInput::Render3D(n) => Some(n),
            _ => None,
        }
    }

    /// Borrow the direct upstream input of this node, if any.
    ///
    /// `Filter` and `Convert` are wrapper nodes that carry exactly one
    /// upstream `TrackInput` (today — multi-input fan-in is a future
    /// extension); both `Source` and `Render3D` are leaves and return
    /// `None`. Use [`Self::leaf`] to descend all the way to the
    /// terminal node in one call, or [`Self::walk`] to visit every
    /// node along the chain.
    pub fn upstream(&self) -> Option<&TrackInput> {
        match self {
            TrackInput::Filter(f) => Some(f.input.as_ref()),
            TrackInput::Convert(c) => Some(c.input.as_ref()),
            TrackInput::Source(_) | TrackInput::Render3D(_) => None,
        }
    }

    /// Walk wrapper nodes (`Filter` / `Convert`) until a leaf
    /// (`Source` / `Render3D`) is reached and return a borrow of the
    /// leaf. The wrapper-chain depth is bounded by the JSON-parsed
    /// schema, so the traversal is O(chain length) and never panics.
    pub fn leaf(&self) -> &TrackInput {
        let mut cur = self;
        while let Some(up) = cur.upstream() {
            cur = up;
        }
        cur
    }

    /// Visit every node in the wrapper chain in order from outermost
    /// (this node) to the terminal leaf. The visitor sees each node
    /// exactly once and the iteration order matches `self`,
    /// `self.upstream()`, `self.upstream().upstream()`, …, leaf.
    ///
    /// Convenient for collectors that need to inspect every filter +
    /// convert hop on a track without writing a recursive helper —
    /// see [`crate::validate`] for the analogous internal walker.
    pub fn walk<F: FnMut(&TrackInput)>(&self, mut f: F) {
        let mut cur = self;
        loop {
            f(cur);
            match cur.upstream() {
                Some(up) => cur = up,
                None => break,
            }
        }
    }
}

/// Parse an ffmpeg-style pixel format name (case-insensitive) into a
/// [`PixelFormat`]. Extend the match arms as new variants land in the
/// enum — unknown names return an [`Error::InvalidData`].
pub fn parse_pixel_format(s: &str) -> Result<PixelFormat> {
    let key = s.trim().to_ascii_lowercase();
    let fmt = match key.as_str() {
        "yuv420p" => PixelFormat::Yuv420P,
        "yuv422p" => PixelFormat::Yuv422P,
        "yuv444p" => PixelFormat::Yuv444P,
        "yuvj420p" => PixelFormat::YuvJ420P,
        "yuvj422p" => PixelFormat::YuvJ422P,
        "yuvj444p" => PixelFormat::YuvJ444P,
        "yuv420p10le" => PixelFormat::Yuv420P10Le,
        "yuv422p10le" => PixelFormat::Yuv422P10Le,
        "yuv444p10le" => PixelFormat::Yuv444P10Le,
        "yuv420p12le" => PixelFormat::Yuv420P12Le,
        "yuva420p" => PixelFormat::Yuva420P,
        "nv12" => PixelFormat::Nv12,
        "nv21" => PixelFormat::Nv21,
        "yuyv422" | "yuy2" => PixelFormat::Yuyv422,
        "uyvy422" | "uyvy" => PixelFormat::Uyvy422,
        "rgb24" => PixelFormat::Rgb24,
        "bgr24" => PixelFormat::Bgr24,
        "rgba" => PixelFormat::Rgba,
        "bgra" => PixelFormat::Bgra,
        "argb" => PixelFormat::Argb,
        "abgr" => PixelFormat::Abgr,
        "rgb48le" | "rgb48" => PixelFormat::Rgb48Le,
        "rgba64le" | "rgba64" => PixelFormat::Rgba64Le,
        "gray" | "gray8" | "y8" => PixelFormat::Gray8,
        "gray16le" | "gray16" | "y16le" => PixelFormat::Gray16Le,
        "gray10le" | "gray10" => PixelFormat::Gray10Le,
        "gray12le" | "gray12" => PixelFormat::Gray12Le,
        "ya8" | "gray8a" => PixelFormat::Ya8,
        "pal8" => PixelFormat::Pal8,
        "monob" | "monoblack" => PixelFormat::MonoBlack,
        "monow" | "monowhite" => PixelFormat::MonoWhite,
        other => {
            return Err(Error::invalid(format!(
                "pixfmt: unknown pixel format {other:?} \
                 (try yuv420p, rgb24, rgba, gray8, nv12, pal8, …)"
            )));
        }
    };
    Ok(fmt)
}

/// Selector for multi-stream inputs. When `kind` is omitted we default to
/// the context kind (e.g. a selector inside `"audio": [...]` only pulls
/// audio streams even if the upstream produces more).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StreamSelector {
    /// `"audio"` / `"video"` / `"subtitle"`. Case-insensitive on the wire.
    #[serde(
        default,
        rename = "type",
        alias = "kind",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "de_media_type_opt",
        serialize_with = "ser_media_type_opt"
    )]
    pub kind: Option<MediaType>,
    /// 0-based index within the filtered pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<u32>,
}

fn is_null_or_empty(v: &serde_json::Value) -> bool {
    v.is_null() || v.as_object().map(|m| m.is_empty()).unwrap_or(false)
}

fn de_media_type_opt<'de, D>(d: D) -> std::result::Result<Option<MediaType>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(d)?;
    Ok(match s.as_deref().map(|s| s.trim().to_ascii_lowercase()) {
        Some(ref s) if s == "audio" => Some(MediaType::Audio),
        Some(ref s) if s == "video" => Some(MediaType::Video),
        Some(ref s) if s == "subtitle" || s == "subtitles" => Some(MediaType::Subtitle),
        Some(ref s) if s == "data" => Some(MediaType::Data),
        None => None,
        Some(other) => {
            return Err(serde::de::Error::custom(format!(
                "unknown stream type {other:?} (expected audio|video|subtitle|data)"
            )));
        }
    })
}

fn ser_media_type_opt<S>(v: &Option<MediaType>, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match v {
        None => s.serialize_none(),
        Some(MediaType::Audio) => s.serialize_str("audio"),
        Some(MediaType::Video) => s.serialize_str("video"),
        Some(MediaType::Subtitle) => s.serialize_str("subtitle"),
        Some(MediaType::Data) => s.serialize_str("data"),
        Some(MediaType::Unknown) => s.serialize_str("unknown"),
    }
}

/// Reserved sink names (all start with `@`). These are **not** aliases —
/// they bind to built-in or caller-supplied sinks at execution time.
pub const RESERVED_SINKS: &[&str] = &["@null", "@display", "@out", "@stdout"];

impl Job {
    /// Parse a `Job` from a JSON string.
    pub fn from_json(s: &str) -> Result<Self> {
        let v: serde_json::Value = serde_json::from_str(s)
            .map_err(|e| Error::invalid(format!("job: JSON parse error: {e}")))?;
        Self::from_value(v)
    }

    /// Parse a `Job` from an already-decoded `serde_json::Value`.
    pub fn from_value(v: serde_json::Value) -> Result<Self> {
        let obj = v
            .as_object()
            .ok_or_else(|| Error::invalid("job: top level must be an object"))?;
        let mut job = Job::default();
        // Reserved meta keys — they describe the job itself rather than
        // a named alias or output. Pulled off before the walker runs so
        // parse errors on them give precise messages.
        if let Some(t) = obj.get("threads") {
            let n = t
                .as_u64()
                .ok_or_else(|| Error::invalid("job: `threads` must be a non-negative integer"))?;
            if n == 0 {
                return Err(Error::invalid(
                    "job: `threads` must be ≥ 1 (use CLI `--threads 0` for auto)",
                ));
            }
            job.threads = Some(n as usize);
        }
        for (key, val) in obj {
            if is_meta_key(key) {
                continue;
            }
            let spec: OutputSpec = serde_json::from_value(val.clone())
                .map_err(|e| Error::invalid(format!("job: {key}: {e}")))?;
            if key.is_empty() {
                return Err(Error::invalid("job: empty top-level key"));
            }
            if key.starts_with('@') && !RESERVED_SINKS.contains(&key.as_str()) {
                job.aliases.insert(key.clone(), spec);
            } else {
                job.outputs.insert(key.clone(), spec);
            }
        }
        Ok(job)
    }

    /// Serialise back to pretty-printed JSON (useful for `dry-run` dumps).
    pub fn to_json_pretty(&self) -> String {
        let mut merged: IndexMap<&String, &OutputSpec> = IndexMap::new();
        for (k, v) in &self.aliases {
            merged.insert(k, v);
        }
        for (k, v) in &self.outputs {
            merged.insert(k, v);
        }
        serde_json::to_string_pretty(&merged).unwrap_or_default()
    }
}

/// True when the given top-level key is a reserved sink name.
pub fn is_reserved_sink(name: &str) -> bool {
    RESERVED_SINKS.contains(&name)
}

/// True when the given sink consumes decoded frames (playback targets
/// that own a driver / window). The DAG inserts an auto-decode stage
/// for no-codec tracks pointed at these sinks so oxideplay doesn't
/// need to name `pcm_s16le` / `rawvideo` every time. `@null` and
/// `@stdout` stay in stream-copy mode.
pub fn is_playback_sink(name: &str) -> bool {
    matches!(name, "@display" | "@out")
}

/// Keys reserved for job metadata (not outputs or aliases). Pulled off
/// the top-level object before the output/alias walk.
const META_KEYS: &[&str] = &["threads"];

fn is_meta_key(name: &str) -> bool {
    META_KEYS.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_alias_and_output() {
        let job = Job::from_json(
            r#"{
                "@input": {"all": [{"from": "a.mp4"}]},
                "out.mkv": {
                    "audio": [{"from": "@input"}],
                    "video": [{"from": "@input"}]
                }
            }"#,
        )
        .unwrap();
        assert_eq!(job.aliases.len(), 1);
        assert_eq!(job.outputs.len(), 1);
        assert!(job.aliases.contains_key("@input"));
        assert!(job.outputs.contains_key("out.mkv"));
        let out = &job.outputs["out.mkv"];
        assert_eq!(out.audio.len(), 1);
        assert_eq!(out.video.len(), 1);
    }

    #[test]
    fn parses_filter_chain() {
        let job = Job::from_json(
            r#"{
                "out.flac": {
                    "audio": [{
                        "filter": "volume",
                        "params": {"gain_db": -3},
                        "input": {
                            "filter": "resample",
                            "params": {"rate": 48000},
                            "input": {"from": "in.wav"}
                        }
                    }]
                }
            }"#,
        )
        .unwrap();
        let track = &job.outputs["out.flac"].audio[0];
        match &track.input {
            TrackInput::Filter(f) => {
                assert_eq!(f.filter, "volume");
                match f.input.as_ref() {
                    TrackInput::Filter(inner) => assert_eq!(inner.filter, "resample"),
                    _ => panic!("expected inner filter"),
                }
            }
            _ => panic!("expected outer filter"),
        }
    }

    #[test]
    fn stream_selector_accepts_type_and_kind() {
        let j = Job::from_json(
            r#"{"o.wav": {"audio": [{"from": "x", "stream_selector": {"type": "audio", "index": 1}}]}}"#,
        ).unwrap();
        let sel = j.outputs["o.wav"].audio[0]
            .stream_selector
            .as_ref()
            .unwrap();
        assert_eq!(sel.kind, Some(MediaType::Audio));
        assert_eq!(sel.index, Some(1));

        let j = Job::from_json(
            r#"{"o.wav": {"audio": [{"from": "x", "stream_selector": {"kind": "subtitles"}}]}}"#,
        )
        .unwrap();
        let sel = j.outputs["o.wav"].audio[0]
            .stream_selector
            .as_ref()
            .unwrap();
        assert_eq!(sel.kind, Some(MediaType::Subtitle));
    }

    #[test]
    fn parses_threads_meta_key() {
        let j = Job::from_json(r#"{"threads": 4, "out.wav": {"audio": [{"from": "in.wav"}]}}"#)
            .unwrap();
        assert_eq!(j.threads, Some(4));
        assert_eq!(j.outputs.len(), 1);
        assert!(j.aliases.is_empty());
    }

    #[test]
    fn rejects_zero_threads() {
        let e = Job::from_json(r#"{"threads": 0, "out.wav": {"audio": []}}"#).unwrap_err();
        let msg = format!("{e}");
        assert!(msg.contains("≥ 1") || msg.contains(">= 1"), "got: {msg}");
    }

    #[test]
    fn reserved_sink_is_not_alias() {
        let j = Job::from_json(r#"{"@display": {"video": [{"from": "x"}]}}"#).unwrap();
        assert!(j.outputs.contains_key("@display"));
        assert!(j.aliases.is_empty());
    }

    #[test]
    fn rejects_non_object_top_level() {
        assert!(Job::from_json("42").is_err());
        assert!(Job::from_json("[]").is_err());
    }

    #[test]
    fn parses_render3d_variant_with_opts() {
        // Phase C-3f schema bridge: a track whose input is the new
        // Render3D variant must parse into `TrackInput::Render3D` and
        // carry the `source`, `backend`, and `opts` fields verbatim.
        let j = Job::from_json(
            r#"{
                "out.mp4": {
                    "video": [{
                        "render3d": "scene.gltf",
                        "backend": "scanline",
                        "opts": {"width": 64, "height": 64},
                        "codec": "h264"
                    }]
                }
            }"#,
        )
        .unwrap();
        let track = &j.outputs["out.mp4"].video[0];
        match &track.input {
            TrackInput::Render3D(node) => {
                assert_eq!(node.source, "scene.gltf");
                assert_eq!(node.backend, "scanline");
                assert_eq!(node.opts["width"], 64);
                assert_eq!(node.opts["height"], 64);
            }
            other => panic!("expected TrackInput::Render3D, got {other:?}"),
        }
        assert_eq!(track.codec.as_deref(), Some("h264"));
    }

    #[test]
    fn render3d_node_round_trips_through_json() {
        // Direct round-trip of the Render3DNode struct itself, so we can
        // assert the serde shape (discriminator key + sibling fields)
        // without relying on the surrounding TrackSpec flatten.
        let node = Render3DNode {
            source: "in.gltf".into(),
            backend: "scanline".into(),
            opts: serde_json::json!({"width": 64, "height": 64}),
        };
        let s = serde_json::to_string(&node).unwrap();
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        // The discriminator key on the wire is `render3d`, not `source`.
        assert_eq!(v["render3d"], "in.gltf");
        assert_eq!(v["backend"], "scanline");
        assert_eq!(v["opts"]["width"], 64);
        let back: Render3DNode = serde_json::from_value(v).unwrap();
        assert_eq!(back, node);
    }

    #[test]
    fn render3d_variant_dispatches_via_untagged_discriminator() {
        // Parsing the bare TrackInput must route the `render3d` key to
        // the Render3D variant ahead of Filter / Source / Convert.
        let v = serde_json::json!({
            "render3d": "x.gltf",
            "backend": "scanline"
        });
        let ti: TrackInput = serde_json::from_value(v).unwrap();
        match ti {
            TrackInput::Render3D(n) => {
                assert_eq!(n.source, "x.gltf");
                assert_eq!(n.backend, "scanline");
                assert!(n.opts.is_null() || n.opts.as_object().is_some_and(|m| m.is_empty()));
            }
            other => panic!("expected Render3D, got {other:?}"),
        }
    }

    #[test]
    fn pre_existing_track_inputs_still_parse_unchanged() {
        // Backward-compat guard: adding the Render3D variant must NOT
        // change how `from` / `filter` / `convert` shaped JSON dispatches.
        let src: TrackInput = serde_json::from_str(r#"{"from": "in.wav"}"#).unwrap();
        assert!(matches!(src, TrackInput::Source(_)));

        let flt: TrackInput = serde_json::from_str(
            r#"{"filter": "volume", "params": {}, "input": {"from": "in.wav"}}"#,
        )
        .unwrap();
        assert!(matches!(flt, TrackInput::Filter(_)));

        let cvt: TrackInput =
            serde_json::from_str(r#"{"convert": "yuv420p", "input": {"from": "in.mp4"}}"#).unwrap();
        assert!(matches!(cvt, TrackInput::Convert(_)));
    }

    #[test]
    fn parses_codec_params_field() {
        // Track-level encoder tuning lives under `codec_params` so it can't
        // collide with a flattened filter's own `params`.
        let j = Job::from_json(
            r#"{"o.mkv": {"video": [{"from": "x", "codec": "h264", "codec_params": {"crf": 23}}]}}"#,
        )
        .unwrap();
        let t = &j.outputs["o.mkv"].video[0];
        assert_eq!(t.codec.as_deref(), Some("h264"));
        assert_eq!(t.params, serde_json::json!({"crf": 23}));
    }

    // ─────── TrackInput typed-accessor primitives (r266) ───────

    fn ti_source(from: &str) -> TrackInput {
        TrackInput::Source(SourceRef { from: from.into() })
    }
    fn ti_filter(name: &str, upstream: TrackInput) -> TrackInput {
        TrackInput::Filter(FilterNode {
            filter: name.into(),
            params: serde_json::Value::Null,
            input: Box::new(upstream),
        })
    }
    fn ti_convert(target: &str, upstream: TrackInput) -> TrackInput {
        TrackInput::Convert(ConvertNode {
            convert: target.into(),
            input: Box::new(upstream),
        })
    }
    fn ti_render3d(source: &str, backend: &str) -> TrackInput {
        TrackInput::Render3D(Render3DNode {
            source: source.into(),
            backend: backend.into(),
            opts: serde_json::Value::Null,
        })
    }

    #[test]
    fn track_input_kind_str_covers_every_variant() {
        assert_eq!(ti_source("a").kind_str(), "source");
        assert_eq!(ti_filter("v", ti_source("a")).kind_str(), "filter");
        assert_eq!(ti_convert("yuv420p", ti_source("a")).kind_str(), "convert");
        assert_eq!(ti_render3d("s.gltf", "scanline").kind_str(), "render3d");
    }

    #[test]
    fn track_input_is_predicates_match_variant() {
        let s = ti_source("a");
        assert!(s.is_source());
        assert!(!s.is_filter());
        assert!(!s.is_convert());
        assert!(!s.is_render3d());

        let f = ti_filter("v", ti_source("a"));
        assert!(f.is_filter());
        assert!(!f.is_source());

        let c = ti_convert("yuv420p", ti_source("a"));
        assert!(c.is_convert());
        assert!(!c.is_filter());

        let r = ti_render3d("s.gltf", "scanline");
        assert!(r.is_render3d());
        assert!(!r.is_source());
    }

    #[test]
    fn track_input_as_returns_some_on_match_none_otherwise() {
        let s = ti_source("a.wav");
        assert_eq!(s.as_source().map(|x| x.from.as_str()), Some("a.wav"));
        assert!(s.as_filter().is_none());
        assert!(s.as_convert().is_none());
        assert!(s.as_render3d().is_none());

        let f = ti_filter("volume", ti_source("a"));
        assert_eq!(f.as_filter().map(|x| x.filter.as_str()), Some("volume"));
        assert!(f.as_source().is_none());

        let c = ti_convert("yuv420p", ti_source("a"));
        assert_eq!(c.as_convert().map(|x| x.convert.as_str()), Some("yuv420p"));
        assert!(c.as_render3d().is_none());

        let r = ti_render3d("s.gltf", "scanline");
        assert_eq!(r.as_render3d().map(|x| x.source.as_str()), Some("s.gltf"));
        assert!(r.as_filter().is_none());
    }

    #[test]
    fn track_input_upstream_descends_one_wrapper_at_a_time() {
        // source / render3d are leaves — upstream() is None.
        assert!(ti_source("a").upstream().is_none());
        assert!(ti_render3d("x.gltf", "scanline").upstream().is_none());

        // filter / convert each yield exactly their inner node, not the leaf.
        let inner_src = ti_source("in.wav");
        let f = ti_filter("volume", ti_filter("resample", inner_src));
        let mid = f.upstream().expect("filter has an upstream");
        assert_eq!(mid.kind_str(), "filter"); // not the leaf yet
        let leaf = mid.upstream().expect("inner filter has an upstream");
        assert!(leaf.is_source());
    }

    #[test]
    fn track_input_leaf_descends_to_terminal_node() {
        // Wrapper chains: convert(filter(filter(source))) — leaf is the source.
        let chain = ti_convert(
            "yuv420p",
            ti_filter("scale", ti_filter("denoise", ti_source("a.mp4"))),
        );
        let leaf = chain.leaf();
        assert!(leaf.is_source());
        assert_eq!(leaf.as_source().unwrap().from, "a.mp4");

        // A leaf node returns itself by value identity.
        let s = ti_source("only.wav");
        assert!(std::ptr::eq(s.leaf(), &s));
    }

    #[test]
    fn track_input_leaf_terminates_at_render3d() {
        // Render3D is also a leaf — leaf() walks the wrapper chain and stops
        // there, even though there's a wrapper between the call site and it.
        let chain = ti_filter("scale", ti_render3d("scene.gltf", "scanline"));
        let leaf = chain.leaf();
        assert!(leaf.is_render3d());
        assert_eq!(leaf.as_render3d().unwrap().backend, "scanline");
    }

    #[test]
    fn track_input_walk_visits_every_node_outer_to_leaf() {
        let chain = ti_convert(
            "yuv420p",
            ti_filter("scale", ti_filter("denoise", ti_source("a.mp4"))),
        );
        let mut seen: Vec<&'static str> = Vec::new();
        chain.walk(|node| seen.push(node.kind_str()));
        assert_eq!(seen, vec!["convert", "filter", "filter", "source"]);
    }

    #[test]
    fn track_input_walk_on_bare_leaf_visits_exactly_once() {
        // A leaf-only node fires the visitor exactly once with the leaf itself.
        let s = ti_source("only.wav");
        let mut count = 0usize;
        s.walk(|_| count += 1);
        assert_eq!(count, 1);

        let r = ti_render3d("only.gltf", "scanline");
        let mut kinds: Vec<&'static str> = Vec::new();
        r.walk(|n| kinds.push(n.kind_str()));
        assert_eq!(kinds, vec!["render3d"]);
    }

    #[test]
    fn track_input_walk_matches_manual_recursion() {
        // The visitor sequence must equal what the historical
        // hand-rolled match-then-recurse produced (see validate.rs
        // walk_input) — same chain, same order.
        let chain = ti_filter(
            "outer",
            ti_convert("rgb24", ti_filter("inner", ti_source("@in"))),
        );
        let mut via_walk: Vec<&'static str> = Vec::new();
        chain.walk(|n| via_walk.push(n.kind_str()));

        fn manual(node: &TrackInput, out: &mut Vec<&'static str>) {
            out.push(node.kind_str());
            if let Some(up) = node.upstream() {
                manual(up, out);
            }
        }
        let mut via_manual: Vec<&'static str> = Vec::new();
        manual(&chain, &mut via_manual);

        assert_eq!(via_walk, via_manual);
    }
}
