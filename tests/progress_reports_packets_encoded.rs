//! Contract test for [`Progress::packets_encoded`].
//!
//! The pipelined runner already tracks the encoder's cumulative
//! `packets_encoded` count on `PipelineCounters` and surfaces it to the
//! caller via the final `ExecutorStats::packets_encoded` returned by
//! `Executor::stop()`. This test pins the new live-sampled surface: the
//! mux loop reads the same atomic on every `Progress` emission so an
//! engine can detect a stalled encoder without waiting for EOF — the
//! decoder is making progress (`frames` and the demuxer's
//! `packets_read` both climb) but the encoder isn't emitting packets
//! (`packets_encoded` stays flat), indicating the encode stage is
//! wedged on a pathological frame rather than the source being slow.
//!
//! Pre-r209 `packets_encoded` was only readable on the final
//! `ExecutorStats` snapshot, so a stress harness had to wait for the
//! run to finish before it could even tell whether the encode stage
//! had been running at all — and a CLI status bar couldn't surface
//! "encoded N packets / decoded M frames" without instrumenting the
//! encoder externally.
//!
//! Asserts:
//!
//! 1. **`packets_encoded` is non-zero at EOF on a transcode run.** The
//!    in-tree stub `StubEncoder` emits one packet per `send_frame`
//!    invocation, and the stub demuxer + decoder produce dozens of
//!    frames, so any run that reaches `eof: true` must have a non-zero
//!    `packets_encoded`. A literal `0` here would mean the field is
//!    not being populated on the EOF emission.
//!
//! 2. **`packets_encoded` is monotonically non-decreasing across
//!    consecutive emissions.** Mirrors the `frames` / `packets_read` /
//!    `packets_skipped` contracts. The shared atomic only ever grows,
//!    so consecutive snapshots must form a non-decreasing sequence.
//!
//! 3. **`packets_encoded` on the EOF event matches the final
//!    `ExecutorStats::packets_encoded`.** The mux loop reads from the
//!    shared atomic on each emission; the EOF emission reads it
//!    *after* the workers join, so it must equal the snapshot value
//!    returned by `Executor::stop()`. Any drift would mean the two
//!    paths are wired to different counters.
//!
//! 4. **`packets_encoded` is `0` on a copy-only output.** The staged
//!    runner only wires `run_encode_stage` when the schema names an
//!    output codec — a `"copy": true` track has no encoder, so the
//!    `PipelineCounters::packets_encoded` atomic never moves off
//!    zero. Both mid-run and EOF emissions must reflect that.

mod common;

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{Executor, Job, JobSink};

/// Trivial sink that drains writes onto a shared `SyncSender<()>` so the
/// test harness can drain it in the background and keep the executor's
/// output channel from filling up.
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

/// Spawn a pipelined transcode job (decode → encode) against the
/// in-tree stub source. The output codec id (`stub_pcm_out`) is
/// registered on the test stub so the staged executor builds a
/// `run_encode_stage`, which is where `packets_encoded` ticks.
fn spawn_transcode(name: &str) -> (oxideav_pipeline::ExecutorHandle, Receiver<()>) {
    let src = common::stub::touch(name);
    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);
    let job_json = format!(
        r#"{{
            "@in":  {{"all": [{{"from": "{}"}}]}},
            "@out": {{"audio": [{{"from": "@in", "codec": "stub_pcm_out"}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Box::leak(Box::new(Job::from_json(&job_json).expect("parse job")));
    let ctx = Box::leak(Box::new(ctx));

    let (write_tx, write_rx) = mpsc::sync_channel::<()>(1024);
    let sink = Box::new(DrainSink { tx: write_tx });

    let handle = Executor::new(job, ctx)
        .with_sink_override("@out", sink)
        .with_threads(2)
        .spawn()
        .expect("spawn executor");
    (handle, write_rx)
}

/// Spawn a pipelined copy-only job — no encoder is instantiated, so
/// `packets_encoded` must stay at `0` throughout. Symmetric helper to
/// `spawn_transcode` for the copy-route invariant assertion.
fn spawn_copy(name: &str) -> (oxideav_pipeline::ExecutorHandle, Receiver<()>) {
    let src = common::stub::touch(name);
    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);
    let job_json = format!(
        r#"{{
            "@in":  {{"all": [{{"from": "{}"}}]}},
            "@out": {{"audio": [{{"from": "@in", "copy": true}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Box::leak(Box::new(Job::from_json(&job_json).expect("parse job")));
    let ctx = Box::leak(Box::new(ctx));

    let (write_tx, write_rx) = mpsc::sync_channel::<()>(1024);
    let sink = Box::new(DrainSink { tx: write_tx });

    let handle = Executor::new(job, ctx)
        .with_sink_override("@out", sink)
        .with_threads(2)
        .spawn()
        .expect("spawn executor");
    (handle, write_rx)
}

/// Collect every Progress emission the handle ever produces plus the
/// final stats. Same shape as the sibling `progress_reports_*` tests'
/// helpers — drain the sink-write channel in a background thread, poll
/// `try_progress` until `has_finished`, then drain any trailing
/// emissions before `stop()` consumes the handle.
fn collect_all_progress(
    handle: oxideav_pipeline::ExecutorHandle,
    write_rx: Receiver<()>,
) -> (
    Vec<oxideav_pipeline::staged::Progress>,
    oxideav_pipeline::executor::ExecutorStats,
) {
    let _drain =
        std::thread::spawn(
            move || {
                while write_rx.recv_timeout(Duration::from_secs(30)).is_ok() {}
            },
        );
    let deadline = Instant::now() + Duration::from_secs(30);
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
    std::thread::sleep(Duration::from_millis(10));
    while let Some(p) = handle.try_progress() {
        events.push(p);
    }
    let stats = handle.stop().expect("executor must finish");
    (events, stats)
}

#[test]
fn packets_encoded_nonzero_at_eof_on_transcode() {
    let (handle, write_rx) = spawn_transcode("packets_encoded_nonzero_at_eof");
    let (events, _stats) = collect_all_progress(handle, write_rx);
    let p = events
        .into_iter()
        .find(|p| p.eof)
        .expect("never observed an eof:true progress event");
    assert!(
        p.packets_encoded > 0,
        "packets_encoded at EOF must be > 0 on a transcode run — the \
         in-tree stub encoder emits one packet per send_frame and the \
         stub source produces dozens of frames; observed `0` indicates \
         the field is not being populated on the EOF emission (or the \
         encode stage never ran)"
    );
}

#[test]
fn packets_encoded_is_non_decreasing() {
    let (handle, write_rx) = spawn_transcode("packets_encoded_monotone");
    let (events, _stats) = collect_all_progress(handle, write_rx);
    assert!(
        events.iter().any(|p| p.eof),
        "never observed an eof:true progress event within 30s — pipeline stalled"
    );
    assert!(
        events.len() >= 2,
        "expected at least two progress samples (mid-run + EOF); got {}",
        events.len()
    );
    for w in events.windows(2) {
        assert!(
            w[1].packets_encoded >= w[0].packets_encoded,
            "packets_encoded must be non-decreasing across emissions; \
             observed {} then {} ({} regression) — suggests the mux \
             loop is reading something other than the shared \
             `PipelineCounters` atomic",
            w[0].packets_encoded,
            w[1].packets_encoded,
            w[0].packets_encoded - w[1].packets_encoded
        );
    }
}

#[test]
fn packets_encoded_eof_matches_final_stats() {
    let (handle, write_rx) = spawn_transcode("packets_encoded_eof_matches_stats");
    let (events, stats) = collect_all_progress(handle, write_rx);
    let eof = events
        .iter()
        .find(|p| p.eof)
        .expect("never observed an eof:true progress event");
    assert_eq!(
        eof.packets_encoded, stats.packets_encoded,
        "the EOF progress emission's packets_encoded ({}) must match \
         the final ExecutorStats::packets_encoded ({}) — both read \
         from the same atomic and the EOF emission is sent after \
         workers join, so any divergence would mean the two paths are \
         wired to different counters",
        eof.packets_encoded, stats.packets_encoded
    );
}

#[test]
fn packets_encoded_is_zero_on_copy_only() {
    let (handle, write_rx) = spawn_copy("packets_encoded_zero_on_copy");
    let (events, stats) = collect_all_progress(handle, write_rx);
    assert!(
        events.iter().any(|p| p.eof),
        "never observed an eof:true progress event within 30s — copy pipeline stalled"
    );
    for (i, p) in events.iter().enumerate() {
        assert_eq!(
            p.packets_encoded, 0,
            "packets_encoded must be 0 throughout a copy-only run \
             (no encoder instantiated, no encode-stage worker spawned); \
             observed {} on emission #{} (eof={})",
            p.packets_encoded, i, p.eof
        );
    }
    assert_eq!(
        stats.packets_encoded, 0,
        "ExecutorStats::packets_encoded must be 0 after a copy-only \
         run — the encoder factory was never invoked"
    );
}
