//! Validate a parsed `Job`: reference integrity + alias-cycle detection.

use oxideav_core::{Error, MediaType, Result};

use crate::schema::{is_reserved_sink, parse_pixel_format, Job, OutputSpec, TrackInput, TrackSpec};

impl Job {
    /// Walk every track input to confirm:
    ///
    /// 1. Every output/alias has at least one track (no empty specs).
    /// 2. Every `@alias` reference inside a `from` field resolves — either to
    ///    a defined alias in this document, or to a reserved sink name.
    /// 3. Alias references do not form a cycle.
    ///
    /// Returns `Err(InvalidData)` with a pointer at the offending key on
    /// first failure.
    pub fn validate(&self) -> Result<()> {
        for (name, spec) in self.outputs.iter().chain(self.aliases.iter()) {
            if spec.is_empty() {
                return Err(Error::invalid(format!(
                    "job: {name}: no tracks (need at least one of audio/video/subtitle/all)"
                )));
            }
            self.check_refs_in_spec(name, spec)?;
        }
        for alias in self.aliases.keys() {
            self.check_no_cycle(alias)?;
        }
        Ok(())
    }

    fn check_refs_in_spec(&self, ctx_name: &str, spec: &OutputSpec) -> Result<()> {
        let buckets: [(&[TrackSpec], Option<MediaType>); 4] = [
            (&spec.audio, Some(MediaType::Audio)),
            (&spec.video, Some(MediaType::Video)),
            (&spec.subtitle, Some(MediaType::Subtitle)),
            (&spec.all, None),
        ];
        for (tracks, bucket_kind) in buckets {
            for track in tracks {
                self.check_track(ctx_name, track, bucket_kind)?;
            }
        }
        Ok(())
    }

    fn check_track(
        &self,
        ctx_name: &str,
        track: &TrackSpec,
        bucket_kind: Option<MediaType>,
    ) -> Result<()> {
        // An explicitly-empty codec is always a mistake: `codec`
        // omitted means stream-copy, and a named codec must be a
        // non-blank id. Catch it here with the track context —
        // otherwise it survives to codec resolution and fails with
        // an opaque "no codec registered for \"\"".
        if let Some(c) = &track.codec {
            if c.trim().is_empty() {
                return Err(Error::invalid(format!(
                    "job: {ctx_name}: empty `codec` \
                     (omit the key for stream-copy, or name a codec id)"
                )));
            }
        }
        // A kind-specific bucket combined with a stream_selector naming
        // a DIFFERENT kind is contradictory: the selector kind wins at
        // DAG-build, so the selected stream's media type would not
        // match the track label the muxer is told about. Only the
        // `all:` bucket (bucket_kind == None) may carry an arbitrary
        // selector kind.
        if let (Some(bucket), Some(sel)) = (bucket_kind, &track.stream_selector) {
            if let Some(sel_kind) = sel.kind {
                if sel_kind != bucket {
                    return Err(Error::invalid(format!(
                        "job: {ctx_name}: stream_selector kind {sel_kind:?} \
                         contradicts the {bucket:?} track list it appears in \
                         (move the track to `all:` or drop the selector kind)"
                    )));
                }
            }
        }
        self.check_refs_in_input(ctx_name, &track.input)
    }

    fn check_refs_in_input(&self, ctx: &str, input: &TrackInput) -> Result<()> {
        match input {
            TrackInput::Source(src) => {
                if src.from.starts_with('@') {
                    if is_reserved_sink(&src.from) {
                        return Err(Error::invalid(format!(
                            "job: {ctx}: cannot use reserved sink {src} as a source",
                            src = src.from
                        )));
                    }
                    if !self.aliases.contains_key(&src.from) {
                        return Err(Error::invalid(format!(
                            "job: {ctx}: unresolved alias reference {src}",
                            src = src.from
                        )));
                    }
                } else if src.from.is_empty() {
                    return Err(Error::invalid(format!("job: {ctx}: empty `from`")));
                }
                Ok(())
            }
            TrackInput::Filter(f) => {
                if f.filter.trim().is_empty() {
                    return Err(Error::invalid(format!(
                        "job: {ctx}: filter node has empty `filter` name"
                    )));
                }
                self.check_refs_in_input(ctx, f.input.as_ref())
            }
            TrackInput::Convert(c) => {
                // Reject unknown pixel format names here so errors point at
                // the track context rather than the opaque DAG builder.
                parse_pixel_format(&c.convert)
                    .map_err(|e| Error::invalid(format!("job: {ctx}: convert: {e}")))?;
                self.check_refs_in_input(ctx, c.input.as_ref())
            }
            TrackInput::Render3D(node) => {
                // Phase C-3f: structural validation only. The backend
                // name + scene URI are opaque strings here; resolution
                // happens at executor run-time via the installed
                // `RenderSourceFactory`. We reject empty fields up
                // front so the failure points at the track context.
                if node.source.trim().is_empty() {
                    return Err(Error::invalid(format!(
                        "job: {ctx}: render3d node has empty `render3d` source"
                    )));
                }
                if node.backend.trim().is_empty() {
                    return Err(Error::invalid(format!(
                        "job: {ctx}: render3d node has empty `backend`"
                    )));
                }
                Ok(())
            }
        }
    }

    /// Depth-first search from `start` over the alias graph. Reports a cycle
    /// with the offending path if found.
    fn check_no_cycle(&self, start: &str) -> Result<()> {
        let mut stack: Vec<String> = vec![start.to_string()];
        let mut path: Vec<String> = vec![start.to_string()];
        self.visit_cycle(start, &mut stack, &mut path)
    }

    fn visit_cycle(
        &self,
        current: &str,
        stack: &mut Vec<String>,
        path: &mut Vec<String>,
    ) -> Result<()> {
        let spec = match self.aliases.get(current) {
            Some(s) => s,
            None => return Ok(()),
        };
        for refd in collect_alias_refs(spec) {
            if stack.iter().any(|s| s == &refd) {
                path.push(refd.clone());
                return Err(Error::invalid(format!(
                    "job: alias cycle detected: {}",
                    path.join(" -> ")
                )));
            }
            stack.push(refd.clone());
            path.push(refd.clone());
            self.visit_cycle(&refd, stack, path)?;
            stack.pop();
            path.pop();
        }
        Ok(())
    }
}

fn collect_alias_refs(spec: &OutputSpec) -> Vec<String> {
    let mut out = Vec::new();
    for t in spec
        .audio
        .iter()
        .chain(&spec.video)
        .chain(&spec.subtitle)
        .chain(&spec.all)
    {
        walk_input(&t.input, &mut out);
    }
    out
}

fn walk_input(input: &TrackInput, out: &mut Vec<String>) {
    match input {
        TrackInput::Source(s) => {
            if s.from.starts_with('@') {
                out.push(s.from.clone());
            }
        }
        TrackInput::Filter(f) => walk_input(f.input.as_ref(), out),
        TrackInput::Convert(c) => walk_input(c.input.as_ref(), out),
        TrackInput::Render3D(_) => {
            // Render3D is a leaf — it carries no upstream `TrackInput`
            // sub-tree, so there are no alias references to collect.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_output() {
        let j = Job::from_json(r#"{"out.mkv": {}}"#).unwrap();
        let e = j.validate().unwrap_err();
        assert!(matches!(e, Error::InvalidData(_)));
    }

    #[test]
    fn rejects_dangling_alias() {
        let j = Job::from_json(r#"{"out.mkv": {"audio": [{"from": "@missing"}]}}"#).unwrap();
        let e = j.validate().unwrap_err();
        let msg = format!("{e}");
        assert!(msg.contains("unresolved alias"), "got: {msg}");
    }

    #[test]
    fn detects_direct_cycle() {
        // @a references @b and @b references @a.
        let j = Job::from_json(
            r#"{
                "@a": {"all": [{"from": "@b"}]},
                "@b": {"all": [{"from": "@a"}]},
                "out.mkv": {"audio": [{"from": "@a"}]}
            }"#,
        )
        .unwrap();
        let e = j.validate().unwrap_err();
        let msg = format!("{e}");
        assert!(msg.contains("cycle"), "got: {msg}");
    }

    #[test]
    fn detects_self_cycle() {
        let j = Job::from_json(
            r#"{
                "@a": {"all": [{"from": "@a"}]},
                "out.mkv": {"audio": [{"from": "@a"}]}
            }"#,
        )
        .unwrap();
        assert!(j.validate().is_err());
    }

    #[test]
    fn accepts_legal_alias_chain() {
        let j = Job::from_json(
            r#"{
                "@in": {"all": [{"from": "a.mp4"}]},
                "@loud": {"audio": [{"filter": "volume", "params": {"gain_db": 3}, "input": {"from": "@in"}}]},
                "out.mkv": {"audio": [{"from": "@loud"}], "video": [{"from": "@in"}]}
            }"#,
        )
        .unwrap();
        j.validate().unwrap();
    }

    #[test]
    fn rejects_reserved_sink_as_source() {
        let j = Job::from_json(r#"{"out.mkv": {"audio": [{"from": "@display"}]}}"#).unwrap();
        assert!(j.validate().is_err());
    }

    #[test]
    fn rejects_empty_codec_string() {
        let j =
            Job::from_json(r#"{"out.mkv": {"audio": [{"from": "a.wav", "codec": ""}]}}"#).unwrap();
        let e = j.validate().unwrap_err();
        let msg = format!("{e}");
        assert!(msg.contains("empty `codec`"), "got: {msg}");
        assert!(msg.contains("out.mkv"), "error must carry ctx, got: {msg}");
    }

    #[test]
    fn rejects_whitespace_codec_string() {
        let j = Job::from_json(r#"{"out.mkv": {"audio": [{"from": "a.wav", "codec": "  "}]}}"#)
            .unwrap();
        assert!(j.validate().is_err());
    }

    #[test]
    fn rejects_empty_codec_inside_alias() {
        // The codec check must apply to alias bodies too, not just
        // outputs — the validate loop chains both maps.
        let j = Job::from_json(
            r#"{
                "@x": {"audio": [{"from": "a.wav", "codec": ""}]},
                "out.mkv": {"audio": [{"from": "@x"}]}
            }"#,
        )
        .unwrap();
        assert!(j.validate().is_err());
    }

    #[test]
    fn accepts_named_codec() {
        let j = Job::from_json(r#"{"out.flac": {"audio": [{"from": "a.wav", "codec": "flac"}]}}"#)
            .unwrap();
        j.validate().unwrap();
    }

    #[test]
    fn rejects_selector_kind_contradicting_bucket() {
        // A `video` selector inside the `audio:` list would win at
        // DAG-build and mux a video stream under an Audio track label.
        let j = Job::from_json(
            r#"{"out.mkv": {"audio": [{"from": "a.mkv", "stream_selector": {"type": "video"}}]}}"#,
        )
        .unwrap();
        let e = j.validate().unwrap_err();
        let msg = format!("{e}");
        assert!(msg.contains("contradicts"), "got: {msg}");
    }

    #[test]
    fn accepts_selector_kind_matching_bucket() {
        // Redundant but consistent — `audio` selector in the audio list
        // (useful for its `index` field) stays legal.
        let j = Job::from_json(
            r#"{"out.mkv": {"audio": [{"from": "a.mkv", "stream_selector": {"type": "audio", "index": 1}}]}}"#,
        )
        .unwrap();
        j.validate().unwrap();
    }

    #[test]
    fn accepts_any_selector_kind_in_all_bucket() {
        // `all:` has no bucket kind, so an explicit selector kind is the
        // only way to constrain it — must stay legal.
        let j = Job::from_json(
            r#"{"out.mkv": {"all": [{"from": "a.mkv", "stream_selector": {"type": "video"}}]}}"#,
        )
        .unwrap();
        j.validate().unwrap();
    }

    #[test]
    fn kind_free_selector_in_kind_bucket_stays_legal() {
        // index-only selector inside a typed bucket: no contradiction
        // (the bucket kind fills in), must pass.
        let j = Job::from_json(
            r#"{"out.mkv": {"video": [{"from": "a.mkv", "stream_selector": {"index": 2}}]}}"#,
        )
        .unwrap();
        j.validate().unwrap();
    }
}
