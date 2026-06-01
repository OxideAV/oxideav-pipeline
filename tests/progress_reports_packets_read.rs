//! Contract test for [`Progress::packets_read`].
//!
//! The pipelined runner already tracks the demuxer's cumulative
//! `packets_read` count on `PipelineCounters` and surfaces it to the
//! caller via the final `ExecutorStats::packets_read` returned by
//! `Executor::stop()`. This test pins the new live-sampled surface: the
//! mux loop reads the same atomic on every `Progress` emission so an
//! engine can detect a stalled decoder before EOF (the demuxer keeps
//! reading — `packets_read` climbs — but `frames` and `packets_skipped`
//! stay flat, indicating the decode stage is wedged).
//!
//! Headroom = `packets_read - frames - packets_skipped` is the count of
//! demuxed packets the decode stage hasn't yet resolved. A value pinned
//! at the channel-depth budget with a flat `frames` field is the
//! diagnostic signature of a wedged decoder; pre-r205 the only way to
//! compute that was to maintain a parallel counter outside the
//! executor.
//!
//! Asserts:
//!
//! 1. **`packets_read` is monotonically non-decreasing across
//!    consecutive emissions.** Mirrors the `frames` and
//!    `packets_skipped` contracts. The shared atomic only ever grows,
//!    so consecutive snapshots must form a non-decreasing sequence.
//!
//! 2. **`packets_read` is non-zero at EOF on a run that does work.**
//!    The in-tree stub demuxer emits dozens of packets so any run that
//!    reaches `eof: true` must have a non-zero `packets_read`. A
//!    literal `0` here would mean the field isn't being populated.
//!
//! 3. **`packets_read` on the EOF event matches the final
//!    `ExecutorStats::packets_read`.** The mux loop reads from the
//!    shared atomic on each emission; the EOF emission reads it
//!    *after* the workers join, so it must equal the snapshot value
//!    returned by `Executor::stop()`. Any drift would mean the two
//!    paths are wired to different counters.

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

/// Collect every Progress emission the handle ever produces plus the
/// final stats. Same shape as the sibling `progress_reports_*` tests'
/// helpers — drain the sink-write channel in a background thread, poll
/// `try_progress` until `has_finished`, then drain any trailing
/// emissions before `stop()` consumes the handle.
fn collect_all_progress(
    name: &str,
) -> (
    Vec<oxideav_pipeline::staged::Progress>,
    oxideav_pipeline::executor::ExecutorStats,
) {
    let (handle, write_rx) = spawn_simple(name);
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
fn packets_read_is_non_decreasing() {
    let (events, _stats) = collect_all_progress("packets_read_monotone");
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
            w[1].packets_read >= w[0].packets_read,
            "packets_read must be non-decreasing across emissions; \
             observed {} then {} ({} regression) — suggests the mux loop \
             is reading something other than the shared `PipelineCounters` \
             atomic",
            w[0].packets_read,
            w[1].packets_read,
            w[0].packets_read - w[1].packets_read
        );
    }
}

#[test]
fn packets_read_nonzero_at_eof() {
    let (events, _stats) = collect_all_progress("packets_read_nonzero_at_eof");
    let p = events
        .into_iter()
        .find(|p| p.eof)
        .expect("never observed an eof:true progress event");
    assert!(
        p.packets_read > 0,
        "packets_read at EOF must be > 0 — the in-tree stub source emits \
         dozens of packets per run; observed `0` indicates the field is \
         not being populated on the EOF emission"
    );
}

#[test]
fn packets_read_eof_matches_final_stats() {
    let (events, stats) = collect_all_progress("packets_read_eof_matches_stats");
    let eof = events
        .iter()
        .find(|p| p.eof)
        .expect("never observed an eof:true progress event");
    assert_eq!(
        eof.packets_read, stats.packets_read,
        "the EOF progress emission's packets_read ({}) must match the final \
         ExecutorStats::packets_read ({}) — both read from the same atomic \
         and the EOF emission is sent after workers join, so any divergence \
         would mean the two paths are wired to different counters",
        eof.packets_read, stats.packets_read
    );
}
