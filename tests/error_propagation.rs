//! Error-propagation coherence tests.
//!
//! Contract: a failing node anywhere in the graph surfaces the
//! ORIGINAL typed error from `Executor::run` — promptly, on both the
//! serial and pipelined paths. No hang (the abort cascade must wake
//! every blocked worker), no silent truncation (the run must not
//! report `Ok` after dropping data on the floor), and no cascading
//! symptom masking the root cause (`AbortState` keeps the FIRST
//! error).
//!
//! Two failure sites are pinned, each on both executor paths:
//!
//! * **Sink failure** — `write_frame` starts erroring mid-stream
//!   (models a full disk / dead audio device).
//! * **Source failure** — a frame source returns a non-EOF error
//!   mid-stream (models a dropped capture device / network feed).

use std::sync::{Arc, Mutex};

use oxideav_core::{
    AudioFrame, CodecId, CodecParameters, Error, Frame, FrameSource, MediaType, Packet, Result,
    RuntimeContext, SampleFormat, StreamInfo,
};
use oxideav_pipeline::{Executor, Job, JobSink};

const SINK_ERR: &str = "observer sink deliberately failed";
const SOURCE_ERR: &str = "frame source deliberately failed";

fn audio_params() -> CodecParameters {
    let mut p = CodecParameters::audio(CodecId::new("errprop_pcm"));
    p.sample_rate = Some(8_000);
    p.channels = Some(1);
    p.sample_format = Some(SampleFormat::S16);
    p
}

/// Endless frame source; `fail_after == Some(n)` makes `next_frame`
/// return `Error::other(SOURCE_ERR)` once `n` frames have been
/// emitted.
struct FlakyFrameSource {
    params: CodecParameters,
    emitted: u64,
    fail_after: Option<u64>,
}

impl FrameSource for FlakyFrameSource {
    fn params(&self) -> &CodecParameters {
        &self.params
    }
    fn next_frame(&mut self) -> Result<Frame> {
        if let Some(n) = self.fail_after {
            if self.emitted >= n {
                return Err(Error::other(SOURCE_ERR));
            }
        }
        let pts = (self.emitted * 16) as i64;
        self.emitted += 1;
        Ok(Frame::Audio(AudioFrame {
            samples: 16,
            pts: Some(pts),
            data: vec![vec![0u8; 32]],
        }))
    }
}

/// Sink that errors on every `write_frame` after the first `ok_frames`.
struct FailingSink {
    ok_frames: u64,
    seen: Arc<Mutex<u64>>,
}

impl JobSink for FailingSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        let mut seen = self.seen.lock().unwrap();
        *seen += 1;
        if *seen > self.ok_frames {
            return Err(Error::other(SINK_ERR));
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

fn make_ctx() -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    // Endless healthy source (sink-failure tests need the stream to
    // outlive the failure point).
    ctx.sources.register_frames("epok", |_uri| {
        Ok(Box::new(FlakyFrameSource {
            params: audio_params(),
            emitted: 0,
            fail_after: None,
        }))
    });
    // Source that dies after 3 frames.
    ctx.sources.register_frames("epdie", |_uri| {
        Ok(Box::new(FlakyFrameSource {
            params: audio_params(),
            emitted: 0,
            fail_after: Some(3),
        }))
    });
    ctx
}

fn display_job(uri: &str) -> Job {
    Job::from_json(&format!(
        r#"{{"@display": {{"audio": [{{"from": "{uri}"}}]}}}}"#
    ))
    .expect("parse job")
}

fn run_expect_err(uri: &str, threads: usize, ok_frames: u64) -> (Error, u64) {
    let ctx = make_ctx();
    let job = display_job(uri);
    let seen = Arc::new(Mutex::new(0u64));
    let sink = Box::new(FailingSink {
        ok_frames,
        seen: seen.clone(),
    });
    let err = Executor::new(&job, &ctx)
        .with_threads(threads)
        .with_sink_override("@display", sink)
        .run()
        .expect_err("run must surface the failure as Err");
    let n = *seen.lock().unwrap();
    (err, n)
}

#[test]
fn sink_failure_surfaces_original_error_serial() {
    let (err, seen) = run_expect_err("epok://endless", 1, 3);
    let msg = format!("{err}");
    assert!(msg.contains(SINK_ERR), "root cause lost, got: {msg}");
    // The failing write is the 4th; the serial path stops there
    // instead of continuing to pump an endless source.
    assert_eq!(seen, 4);
}

#[test]
fn sink_failure_surfaces_original_error_pipelined() {
    let (err, seen) = run_expect_err("epok://endless", 2, 3);
    let msg = format!("{err}");
    assert!(msg.contains(SINK_ERR), "root cause lost, got: {msg}");
    // Pipelined: the mux loop records the error and the abort cascade
    // tears the workers down. In-flight frames bounded by the channel
    // depths never reach the sink after the error — `write_frame` is
    // called exactly once past the healthy budget.
    assert_eq!(seen, 4);
}

#[test]
fn source_failure_surfaces_original_error_serial() {
    let (err, seen) = run_expect_err("epdie://gen", 1, u64::MAX);
    let msg = format!("{err}");
    assert!(msg.contains(SOURCE_ERR), "root cause lost, got: {msg}");
    // All frames produced before the failure were delivered — the
    // failure must not retroactively drop them.
    assert_eq!(seen, 3);
}

#[test]
fn source_failure_surfaces_original_error_pipelined() {
    let (err, seen) = run_expect_err("epdie://gen", 2, u64::MAX);
    let msg = format!("{err}");
    assert!(msg.contains(SOURCE_ERR), "root cause lost, got: {msg}");
    // Pipelined semantics differ from serial here, deliberately: the
    // abort cascade tears the worker graph down PROMPTLY when any
    // stage errors, so frames still in flight in the bounded channels
    // (at most channel-depth per stage) may be dropped rather than
    // drained. Promptness wins over completeness on the pipelined
    // path — the same tradeoff `ExecutorHandle::stop` relies on. What
    // must hold: never MORE than the pre-failure frames, and never a
    // hang.
    assert!(
        seen <= 3,
        "frames delivered after the source failure point: {seen}"
    );
}
