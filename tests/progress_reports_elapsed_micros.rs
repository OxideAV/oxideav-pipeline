//! Contract test for [`Progress::elapsed_micros`].
//!
//! The pipelined runner stamps each `Progress` it emits with the wall-
//! clock microseconds since the runner's baseline `Instant` — captured
//! just before the first worker thread spawns and therefore just before
//! any packet starts flowing. Engines use this in three concrete ways:
//!
//! * Realtime ratio (`pts_micros / elapsed_micros`) for a transcode
//!   speed indicator (`> 1.0` faster-than-realtime).
//! * Realtime drift detection on live sources — the engine compares
//!   `pts` to `elapsed_micros` to see whether the pipeline is keeping
//!   up with the source clock.
//! * EOF wall-clock total: the `eof: true` progress event carries the
//!   total runtime so CLI tools don't need to bracket
//!   `executor.spawn()/.stop()` with their own `Instant::now()`.
//!
//! Asserts:
//!
//! 1. **`elapsed_micros` is non-decreasing across consecutive
//!    emissions.** `Instant::elapsed()` is monotonic so the mux loop's
//!    successive readings must form a non-decreasing sequence. A
//!    decreasing value would indicate a stale clock or a recreated
//!    baseline mid-run, both of which would break the realtime-ratio
//!    derivation.
//!
//! 2. **`elapsed_micros` is non-zero at EOF for any run that does
//!    actual work.** Even the fastest pipelined run with the in-tree
//!    stub takes hundreds of microseconds (channel sync + thread join);
//!    a literal zero on the EOF event would mean the field is never
//!    being populated (e.g. a future refactor that dropped the
//!    `started_at` thread-through silently).
//!
//! 3. **`elapsed_micros` mid-run is bounded above by the EOF value.**
//!    Wall-clock can only grow, so any pre-EOF event's value must be
//!    `<=` the final EOF event's value. This is a sanity check that
//!    the mid-run and EOF code paths share the same `started_at`
//!    baseline rather than reading two independent clocks.

mod common;

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{Executor, Job, JobSink};

/// Trivial sink that drains writes onto a shared `SyncSender<()>` so the
/// test harness can drain it in the background and keep the executor's
/// output channel from filling up. Identical shape to the sink used by
/// `progress_reports_queue_bytes.rs` minus the throttling — we want the
/// run to complete promptly here.
struct DrainSink {
    tx: SyncSender<()>,
}

impl JobSink for DrainSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        let _ = self.tx.try_send(());
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        let _ = self.tx.try_send(());
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Spawn a pipelined copy-only job against the in-tree stub source.
/// Returns the handle + a channel the harness drains so the executor
/// doesn't park on a full output channel.
fn spawn_simple(name: &str) -> (oxideav_pipeline::ExecutorHandle, Receiver<()>) {
    let src = common::stub::touch(name);
    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);
    let job_json = format!(
        r#"{{
            "@in":      {{"all": [{{"from": "{}"}}]}},
            "@display": {{"audio": [{{"from": "@in", "copy": true}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Box::leak(Box::new(Job::from_json(&job_json).expect("parse job")));
    let ctx = Box::leak(Box::new(ctx));

    let (write_tx, write_rx) = mpsc::sync_channel::<()>(1024);
    let sink = Box::new(DrainSink { tx: write_tx });

    let handle = Executor::new(job, ctx)
        .with_sink_override("@display", sink)
        .with_threads(2)
        .spawn()
        .expect("spawn executor");
    (handle, write_rx)
}

/// Collect every Progress emission the handle ever produces. Spawns a
/// background drain on the sink-write channel so the mux loop doesn't
/// park on a full output buffer, polls progress in a tight loop until
/// `has_finished` is observed, then drains every trailing emission off
/// the receiver after the worker thread has joined.
///
/// Using a single helper keeps the three tests' polling shape identical
/// — earlier versions diverged subtly and produced timing-flaky failures
/// when one variant missed the EOF event because the worker finished
/// between two polls. `try_progress` is `try_recv` based: any progress
/// the mux loop pushed before the worker exited is still queued on the
/// receiver after `stop()` returns, so the drain-after-stop loop here
/// reliably recovers every event the mux loop emitted.
fn collect_all_progress(name: &str) -> Vec<oxideav_pipeline::staged::Progress> {
    let (handle, write_rx) = spawn_simple(name);
    let _drain = std::thread::spawn(
        move || {
            while write_rx.recv_timeout(Duration::from_secs(2)).is_ok() {}
        },
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut events = Vec::new();
    while Instant::now() < deadline {
        while let Some(p) = handle.try_progress() {
            events.push(p);
        }
        if handle.has_finished() {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    // Final drain BEFORE `stop()` (which consumes the handle): the mux
    // loop pushes the EOF progress event right before it returns, and a
    // tight poll above can finish before that event reaches the
    // sync_channel(64). Without this trailing drain, the EOF event would
    // be racily missed when the worker happens to finish between two
    // `try_progress` polls. A short sleep gives the mux loop time to
    // surface its final emission.
    std::thread::sleep(Duration::from_millis(10));
    while let Some(p) = handle.try_progress() {
        events.push(p);
    }
    let _ = handle.stop();
    events
}

#[test]
fn elapsed_micros_is_non_decreasing() {
    // Walk through every Progress emission, asserting each one's
    // `elapsed_micros` is `>=` the previous one. `Instant::elapsed` is
    // monotonic so consecutive samples must form a non-decreasing
    // sequence — a regression here would indicate the mux loop is
    // reading some recreated baseline instead of the persistent
    // `started_at` taken before workers spawn.
    let events = collect_all_progress("elapsed_micros_monotone");
    assert!(
        events.iter().any(|p| p.eof),
        "never observed an eof:true progress event within 5s — pipeline stalled"
    );
    assert!(
        events.len() >= 2,
        "expected at least two progress samples (mid-run + EOF); got {}",
        events.len()
    );
    for w in events.windows(2) {
        assert!(
            w[1].elapsed_micros >= w[0].elapsed_micros,
            "elapsed_micros must be non-decreasing across emissions; \
             observed {} then {} ({} us regression) — suggests the mux loop \
             is reading a different baseline than the one taken before \
             workers spawned",
            w[0].elapsed_micros,
            w[1].elapsed_micros,
            w[0].elapsed_micros - w[1].elapsed_micros
        );
    }
}

#[test]
fn elapsed_micros_nonzero_at_eof() {
    // The EOF progress event is emitted after `sink.finish()` returns,
    // which is unconditionally after the workers join — even the
    // fastest pipelined run takes hundreds of microseconds of channel
    // sync + thread join. A literal `0` here would mean
    // `Progress::elapsed_micros` is never being populated, e.g. a
    // future refactor that dropped the `started_at` thread-through.
    let events = collect_all_progress("elapsed_micros_nonzero_at_eof");
    let p = events
        .into_iter()
        .find(|p| p.eof)
        .expect("never observed an eof:true progress event");
    assert!(
        p.elapsed_micros > 0,
        "elapsed_micros at EOF must be > 0 — even the fastest pipelined run \
         spends hundreds of microseconds in channel sync + thread join; \
         observed `0` indicates the field is not being populated"
    );
}

#[test]
fn elapsed_micros_bounded_by_eof_value() {
    // Wall-clock can only grow forward, so any mid-run sample must be
    // `<=` the final EOF sample. The interesting case this catches is
    // a refactor that reads two independent baselines on the mid-run
    // and EOF code paths — they'd look fine in isolation but the EOF
    // sample could be visibly smaller than a mid-run sample taken from
    // a different baseline.
    let events = collect_all_progress("elapsed_micros_bounded_by_eof");
    let eof = events
        .iter()
        .find(|p| p.eof)
        .map(|p| p.elapsed_micros)
        .expect("never observed an eof:true progress event");
    let mid_run_peak = events
        .iter()
        .filter(|p| !p.eof)
        .map(|p| p.elapsed_micros)
        .max()
        .unwrap_or(0);
    assert!(
        mid_run_peak <= eof,
        "mid-run peak elapsed_micros ({mid_run_peak}) must be <= EOF value \
         ({eof}); a mid-run sample exceeding the EOF sample suggests the two \
         code paths are reading different `started_at` baselines"
    );
}
