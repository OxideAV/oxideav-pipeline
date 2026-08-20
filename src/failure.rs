//! Typed failure attribution for executor runs.
//!
//! A pipeline run that fails surfaces ONE error — the first one
//! recorded, per the first-error-wins contract pinned by
//! `tests/error_propagation.rs`. Historically that error was a bare
//! [`oxideav_core::Error`]: the root cause survived, but *where* in the
//! graph it fired (which output, which track, which stage) was only
//! recoverable by parsing log lines. Engines that want to render
//! "encoder failed on the video track of out.mp4" — or decide
//! programmatically whether a failure is retryable per-output — need
//! the attribution as data.
//!
//! [`RunFailure`] carries exactly that: the ORIGINAL error (unwrapped,
//! never stringified into a new variant) plus the output key, the
//! [`FailureStage`], and the track index when the failing stage belongs
//! to one. [`Executor::run_reporting`](crate::Executor::run_reporting)
//! returns it directly;
//! [`Executor::run`](crate::Executor::run) keeps its historical
//! signature by discarding the attribution half
//! (`From<RunFailure> for Error` returns the stored original error, so
//! both surfaces observe the exact same root cause).

use std::fmt;

use oxideav_core::Error;

/// Where in a pipeline run the first error was recorded.
///
/// The variants follow the stage graph, not the thread layout: the
/// serial and pipelined executors attribute the same failure site to
/// the same variant, so an engine can branch on it without knowing
/// which executor path ran the job.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FailureStage {
    /// Preparation: validation, DAG build, source open, codec / filter
    /// instantiation, or sink resolution. No stream data flowed yet.
    Prepare,
    /// The source pump: a demuxer read, a packet-source read, or a
    /// frame-source read returned a non-EOF error mid-stream.
    Source,
    /// The packet copy (pass-through) stage of a copy-mode track.
    Copy,
    /// The decode stage. Per-packet decoder errors are tolerated
    /// (logged + counted in `packets_skipped`), so this only fires for
    /// failures the tolerance contract does not cover.
    Decode,
    /// A frame filter stage.
    Filter,
    /// A pixel-format conversion stage.
    Convert,
    /// The encode stage, including the end-of-stream encoder flush.
    Encode,
    /// The sink: `start`, `write_packet`, `write_frame`, or `barrier`.
    Sink,
    /// Sink finalisation (`finish` — e.g. the container trailer write).
    /// Split from [`FailureStage::Sink`] because every mid-stream byte
    /// already landed when this fires: the output is complete except
    /// for finalisation, which some engines treat as salvageable.
    SinkFinish,
}

impl FailureStage {
    /// Stable lowercase discriminator string for log lines and
    /// diagnostics.
    pub fn as_str(&self) -> &'static str {
        match self {
            FailureStage::Prepare => "prepare",
            FailureStage::Source => "source",
            FailureStage::Copy => "copy",
            FailureStage::Decode => "decode",
            FailureStage::Filter => "filter",
            FailureStage::Convert => "convert",
            FailureStage::Encode => "encode",
            FailureStage::Sink => "sink",
            FailureStage::SinkFinish => "sink-finish",
        }
    }
}

impl fmt::Display for FailureStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Typed report for a failed run: the original error plus where it
/// fired.
///
/// Returned by [`Executor::run_reporting`](crate::Executor::run_reporting)
/// and [`ExecutorHandle::stop_reporting`](crate::ExecutorHandle::stop_reporting).
/// The `error` field holds the ORIGINAL first error unchanged —
/// converting a `RunFailure` back into an [`Error`] (via `From`) yields
/// exactly the value the plain [`Executor::run`](crate::Executor::run)
/// surface reports, so the two surfaces never diverge on the root
/// cause.
#[derive(Debug)]
pub struct RunFailure {
    /// The output key the failure belongs to (`"out.mp4"`,
    /// `"@display"`, …). `None` for job-level failures that precede any
    /// per-output work (validation, DAG build).
    pub output: Option<String>,
    /// The stage that recorded the first error.
    pub stage: FailureStage,
    /// Track index within the output, when the failing stage belongs to
    /// one. Source pumps, sink resolution, and job-level failures carry
    /// `None`.
    pub track: Option<u32>,
    /// The original first error, unwrapped.
    pub error: Error,
}

impl RunFailure {
    /// Attribute a job-level failure (validation / DAG build) that has
    /// no owning output.
    pub(crate) fn job_level(error: Error) -> Self {
        Self {
            output: None,
            stage: FailureStage::Prepare,
            track: None,
            error,
        }
    }

    /// Attach the owning output key to an internal [`StageFailure`].
    pub(crate) fn for_output(output: &str, failure: StageFailure) -> Self {
        Self {
            output: Some(output.to_string()),
            stage: failure.stage,
            track: failure.track,
            error: failure.error,
        }
    }
}

impl fmt::Display for RunFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.output {
            Some(out) => write!(f, "output {out}: ")?,
            None => f.write_str("job: ")?,
        }
        write!(f, "{} stage", self.stage)?;
        if let Some(t) = self.track {
            write!(f, " (track {t})")?;
        }
        write!(f, ": {}", self.error)
    }
}

impl std::error::Error for RunFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl From<RunFailure> for Error {
    /// Recover the plain-error surface: the ORIGINAL first error,
    /// unwrapped — not a re-stringified copy — so error-kind matching
    /// (`Error::ResourceExhausted`, `Error::Unsupported`, …) behaves
    /// identically on both reporting surfaces.
    fn from(f: RunFailure) -> Self {
        f.error
    }
}

/// Internal attribution carrier: a [`FailureStage`] + optional track
/// index + the original error, WITHOUT the output key (the executor
/// attaches that at the per-output call sites, where the key is known).
#[derive(Debug)]
pub(crate) struct StageFailure {
    pub(crate) stage: FailureStage,
    pub(crate) track: Option<u32>,
    pub(crate) error: Error,
}

impl StageFailure {
    pub(crate) fn new(stage: FailureStage, track: Option<u32>, error: Error) -> Self {
        Self {
            stage,
            track,
            error,
        }
    }
}

impl From<StageFailure> for Error {
    fn from(f: StageFailure) -> Self {
        f.error
    }
}

/// Alias for internal executor plumbing that carries attribution.
pub(crate) type StageResult<T> = std::result::Result<T, StageFailure>;

/// `map_err` helper: attribute an [`Error`] to `(stage, track)`.
pub(crate) fn attribute(stage: FailureStage, track: Option<u32>) -> impl Fn(Error) -> StageFailure {
    move |error| StageFailure::new(stage, track, error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_strings_are_stable() {
        for (stage, s) in [
            (FailureStage::Prepare, "prepare"),
            (FailureStage::Source, "source"),
            (FailureStage::Copy, "copy"),
            (FailureStage::Decode, "decode"),
            (FailureStage::Filter, "filter"),
            (FailureStage::Convert, "convert"),
            (FailureStage::Encode, "encode"),
            (FailureStage::Sink, "sink"),
            (FailureStage::SinkFinish, "sink-finish"),
        ] {
            assert_eq!(stage.as_str(), s);
            assert_eq!(stage.to_string(), s);
        }
    }

    #[test]
    fn display_carries_output_stage_track_and_cause() {
        let f = RunFailure {
            output: Some("out.mp4".into()),
            stage: FailureStage::Encode,
            track: Some(1),
            error: Error::other("boom"),
        };
        assert_eq!(
            f.to_string(),
            "output out.mp4: encode stage (track 1): boom"
        );
        let f = RunFailure::job_level(Error::invalid("cyclic"));
        assert_eq!(f.to_string(), "job: prepare stage: invalid data: cyclic");
    }

    #[test]
    fn conversion_returns_the_original_error_unwrapped() {
        let f = RunFailure {
            output: Some("x".into()),
            stage: FailureStage::Sink,
            track: None,
            error: Error::resource_exhausted("cap"),
        };
        let e: Error = f.into();
        assert!(e.is_resource_exhausted());
        assert_eq!(e.to_string(), "resource exhausted: cap");
    }

    #[test]
    fn source_chain_exposes_the_original_error() {
        let f = RunFailure {
            output: None,
            stage: FailureStage::Prepare,
            track: None,
            error: Error::unsupported("nope"),
        };
        let src = std::error::Error::source(&f).expect("source");
        assert_eq!(src.to_string(), "unsupported: nope");
    }
}
