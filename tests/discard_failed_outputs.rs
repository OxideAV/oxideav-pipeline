//! Failed-output disposal contract tests.
//!
//! `Executor::with_discard_failed_outputs(true)` opts a job into
//! partial-output cleanup: a failing output's sink receives
//! `JobSink::abandon` instead of being dropped mid-write, and the
//! built-in `FileSink` implements `abandon` by deleting the
//! partially-written file. Pinned here:
//!
//! * a failed output's file is REMOVED when opted in — serial and
//!   pipelined;
//! * the default (knob off) keeps the partial file — historical
//!   behaviour, the file starts with the container header but was
//!   never finalised;
//! * a clean run is unaffected by the knob (file present + finalised);
//! * multi-output: only the failing output's file is disposed —
//!   healthy wave-mates finalise normally;
//! * a wave PREP failure disposes wave-mates that were prepared but
//!   never ran (their zero-byte files would otherwise linger);
//! * custom sinks: the `abandon` hook fires exactly once on failure
//!   when opted in, never by default, and never on success;
//! * `ExecutorHandle::stop` without a recorded error still finalises
//!   (stop is not a failure);
//! * `FileSink::abandon` is idempotent and later writes report the
//!   abandonment instead of resurrecting the file.

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::stub::{DIE_ERR, EXT_OUT, MUX_HEADER, MUX_TRAILER};
use oxideav_core::{Frame, MediaType, Packet, Result, RuntimeContext, StreamInfo};
use oxideav_pipeline::{Executor, FailureStage, FileSink, Job, JobSink};

fn stub_ctx() -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);
    ctx
}

fn json_path(p: &std::path::Path) -> String {
    p.display().to_string().replace('\\', "\\\\")
}

/// Fresh output path (`.stubout`) with any leftover from a previous
/// test run removed.
fn out_path(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("oxideav_pipeline_out_{name}.{EXT_OUT}"));
    let _ = std::fs::remove_file(&p);
    p
}

fn assert_finalised(path: &std::path::Path) {
    let bytes = std::fs::read(path).expect("finalised output must exist");
    assert!(
        bytes.starts_with(MUX_HEADER),
        "output missing container header"
    );
    assert!(
        bytes.ends_with(MUX_TRAILER),
        "output missing container trailer (never finalised)"
    );
}

// ───────────────────── single output, both paths ─────────────────────

#[test]
fn failed_output_file_removed_when_opted_in() {
    for threads in [1usize, 2] {
        let src = common::stub::touch_die(&format!("disposal_on_{threads}"));
        let out = out_path(&format!("disposal_on_{threads}"));
        let job_json = format!(
            r#"{{"{}": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&out),
            json_path(&src)
        );
        let ctx = stub_ctx();
        let job = Job::from_json(&job_json).expect("parse job");
        let f = Executor::new(&job, &ctx)
            .with_threads(threads)
            .with_discard_failed_outputs(true)
            .run_reporting()
            .expect_err("dying source must fail the run");
        assert_eq!(f.stage, FailureStage::Source, "threads={threads}: {f}");
        assert!(
            f.error.to_string().contains(DIE_ERR),
            "root cause lost: {f}"
        );
        assert!(
            !out.exists(),
            "threads={threads}: partial output file must be removed, but {} exists",
            out.display()
        );
    }
}

#[test]
fn failed_output_file_kept_by_default() {
    for threads in [1usize, 2] {
        let src = common::stub::touch_die(&format!("disposal_off_{threads}"));
        let out = out_path(&format!("disposal_off_{threads}"));
        let job_json = format!(
            r#"{{"{}": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&out),
            json_path(&src)
        );
        let ctx = stub_ctx();
        let job = Job::from_json(&job_json).expect("parse job");
        Executor::new(&job, &ctx)
            .with_threads(threads)
            .run()
            .expect_err("dying source must fail the run");
        // Historical behaviour: the half-written file stays. It has the
        // header (packets flowed before the failure) but no trailer.
        let bytes = std::fs::read(&out).expect("partial output must be kept by default");
        assert!(bytes.starts_with(MUX_HEADER));
        assert!(
            !bytes.ends_with(MUX_TRAILER),
            "a failed run must not have finalised the output"
        );
    }
}

#[test]
fn clean_run_is_unaffected_by_the_knob() {
    for threads in [1usize, 2] {
        let src = common::stub::touch(&format!("disposal_clean_{threads}"));
        let out = out_path(&format!("disposal_clean_{threads}"));
        let job_json = format!(
            r#"{{"{}": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&out),
            json_path(&src)
        );
        let ctx = stub_ctx();
        let job = Job::from_json(&job_json).expect("parse job");
        let stats = Executor::new(&job, &ctx)
            .with_threads(threads)
            .with_discard_failed_outputs(true)
            .run()
            .expect("clean run");
        assert!(stats.packets_copied > 0);
        assert_finalised(&out);
    }
}

// ───────────────────────── multi-output ─────────────────────────

#[test]
fn multi_output_disposes_only_the_failing_output() {
    let src_ok = common::stub::touch("disposal_multi_ok");
    let src_die = common::stub::touch_die("disposal_multi_die");
    let out_ok = out_path("disposal_multi_ok");
    let out_die = out_path("disposal_multi_die");
    let job_json = format!(
        r#"{{
            "{}": {{"audio": [{{"from": "{}"}}]}},
            "{}": {{"audio": [{{"from": "{}"}}]}}
        }}"#,
        json_path(&out_ok),
        json_path(&src_ok),
        json_path(&out_die),
        json_path(&src_die),
    );
    let ctx = stub_ctx();
    let job = Job::from_json(&job_json).expect("parse job");
    let f = Executor::new(&job, &ctx)
        .with_threads(4)
        .with_discard_failed_outputs(true)
        .run_reporting()
        .expect_err("the dying output must fail the run");
    assert_eq!(f.output.as_deref(), Some(json_path(&out_die).as_str()));
    assert_eq!(f.stage, FailureStage::Source);
    // The healthy wave-mate ran to completion and finalised normally.
    assert_finalised(&out_ok);
    // The failing output's partial file is gone.
    assert!(
        !out_die.exists(),
        "failing output's partial file must be removed"
    );
}

#[test]
fn wave_prep_failure_disposes_prepared_wave_mates() {
    // Output A prepares fine (its file is created at sink resolution);
    // output B fails PREPARATION (unknown codec), so the wave never
    // starts and A never runs. With disposal on, A's just-created file
    // must not linger.
    let src = common::stub::touch("disposal_prep_src");
    let out_a = out_path("disposal_prep_a");
    let out_b = out_path("disposal_prep_b");
    let job_json = format!(
        r#"{{
            "{}": {{"audio": [{{"from": "{}"}}]}},
            "{}": {{"audio": [{{"from": "{}", "codec": "disposal_no_such_codec"}}]}}
        }}"#,
        json_path(&out_a),
        json_path(&src),
        json_path(&out_b),
        json_path(&src),
    );
    let ctx = stub_ctx();
    let job = Job::from_json(&job_json).expect("parse job");
    let f = Executor::new(&job, &ctx)
        .with_threads(4)
        .with_discard_failed_outputs(true)
        .run_reporting()
        .expect_err("unknown codec must fail preparation");
    assert_eq!(f.output.as_deref(), Some(json_path(&out_b).as_str()));
    assert_eq!(f.stage, FailureStage::Prepare);
    assert!(
        !out_a.exists(),
        "prepared-but-never-run wave-mate's file must be disposed"
    );
    assert!(!out_b.exists(), "failing output never got past open");

    // Default (knob off): the historical behaviour keeps A's empty
    // artifact around.
    let out_a2 = out_path("disposal_prep_a2");
    let out_b2 = out_path("disposal_prep_b2");
    let job_json = format!(
        r#"{{
            "{}": {{"audio": [{{"from": "{}"}}]}},
            "{}": {{"audio": [{{"from": "{}", "codec": "disposal_no_such_codec"}}]}}
        }}"#,
        json_path(&out_a2),
        json_path(&src),
        json_path(&out_b2),
        json_path(&src),
    );
    let job = Job::from_json(&job_json).expect("parse job");
    Executor::new(&job, &ctx)
        .with_threads(4)
        .run()
        .expect_err("unknown codec must fail preparation");
    assert!(
        out_a2.exists(),
        "default behaviour keeps the prepared wave-mate's artifact"
    );
}

// ───────────────────────── custom sinks ─────────────────────────

/// Observer sink counting `abandon` / `finish` invocations.
struct HookSink {
    abandons: Arc<AtomicUsize>,
    finishes: Arc<AtomicUsize>,
}

impl HookSink {
    fn boxed() -> (Box<dyn JobSink + Send>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let abandons = Arc::new(AtomicUsize::new(0));
        let finishes = Arc::new(AtomicUsize::new(0));
        (
            Box::new(Self {
                abandons: abandons.clone(),
                finishes: finishes.clone(),
            }),
            abandons,
            finishes,
        )
    }
}

impl JobSink for HookSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        self.finishes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn abandon(&mut self) -> Result<()> {
        self.abandons.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn custom_sink_abandon_hook_fires_once_on_failure_when_opted_in() {
    for threads in [1usize, 2] {
        let src = common::stub::touch_die(&format!("disposal_hook_{threads}"));
        let job_json = format!(
            r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
            json_path(&src)
        );
        let ctx = stub_ctx();
        let job = Job::from_json(&job_json).expect("parse job");
        let (sink, abandons, finishes) = HookSink::boxed();
        Executor::new(&job, &ctx)
            .with_threads(threads)
            .with_discard_failed_outputs(true)
            .with_sink_override("@display", sink)
            .run()
            .expect_err("dying source must fail");
        assert_eq!(abandons.load(Ordering::SeqCst), 1, "threads={threads}");
        assert_eq!(finishes.load(Ordering::SeqCst), 0, "threads={threads}");
    }
}

#[test]
fn custom_sink_abandon_hook_silent_by_default_and_on_success() {
    // Failure without the knob: no abandon, no finish.
    let src = common::stub::touch_die("disposal_hook_default");
    let job_json = format!(
        r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
        json_path(&src)
    );
    let ctx = stub_ctx();
    let job = Job::from_json(&job_json).expect("parse job");
    let (sink, abandons, finishes) = HookSink::boxed();
    Executor::new(&job, &ctx)
        .with_threads(2)
        .with_sink_override("@display", sink)
        .run()
        .expect_err("dying source must fail");
    assert_eq!(abandons.load(Ordering::SeqCst), 0);
    assert_eq!(finishes.load(Ordering::SeqCst), 0);

    // Success with the knob: finish, never abandon.
    let src = common::stub::touch("disposal_hook_success");
    let job_json = format!(
        r#"{{"@display": {{"audio": [{{"from": "{}"}}]}}}}"#,
        json_path(&src)
    );
    let job = Job::from_json(&job_json).expect("parse job");
    let (sink, abandons, finishes) = HookSink::boxed();
    Executor::new(&job, &ctx)
        .with_threads(2)
        .with_discard_failed_outputs(true)
        .with_sink_override("@display", sink)
        .run()
        .expect("clean run");
    assert_eq!(abandons.load(Ordering::SeqCst), 0);
    assert_eq!(finishes.load(Ordering::SeqCst), 1);
}

// ───────────────────────── stop() semantics ─────────────────────────

#[test]
fn clean_stop_finalises_instead_of_abandoning() {
    // `stop()` on a healthy stream is a deliberate cancel, not a
    // failure: no error is recorded, so the sink finalises (trailer
    // written) even with disposal opted in.
    let src = common::stub::touch("disposal_stop");
    let out = out_path("disposal_stop");
    let job_json = format!(
        r#"{{"{}": {{"audio": [{{"from": "{}"}}]}}}}"#,
        json_path(&out),
        json_path(&src)
    );
    let ctx = stub_ctx();
    let job = Job::from_json(&job_json).expect("parse job");
    let handle = Executor::new(&job, &ctx)
        .with_threads(2)
        .with_discard_failed_outputs(true)
        .spawn()
        .expect("spawn");
    // Let some packets flow, then cancel mid-stream (the stub source
    // carries 60 s of audio, far more than this window).
    std::thread::sleep(std::time::Duration::from_millis(150));
    handle.stop().expect("clean stop");
    assert_finalised(&out);
}

// ───────────────────────── FileSink::abandon ─────────────────────────

#[test]
fn file_sink_abandon_is_idempotent_and_blocks_later_writes() {
    let ctx = stub_ctx();
    let path = out_path("disposal_filesink_unit");
    let fout = oxideav_pipeline::sinks::open_file_write(&path).expect("create file");
    let muxer = ctx
        .containers
        .open_muxer(common::stub::CONTAINER_OUT, fout, &[])
        .expect("open muxer");
    let mut sink = FileSink::new(path.clone(), muxer);
    sink.start(&[]).expect("start");
    assert!(path.exists());
    sink.abandon().expect("abandon");
    assert!(!path.exists(), "abandon must remove the file");
    // Idempotent: second call (file already gone) still reports Ok.
    sink.abandon().expect("abandon twice");
    // Later hooks report the abandonment instead of resurrecting the
    // file.
    let err = sink.finish().expect_err("finish after abandon must fail");
    assert!(
        err.to_string().contains("abandoned"),
        "unexpected error: {err}"
    );
    assert!(!path.exists());
}
