//! Contract test for [`Progress::queue_bytes`].
//!
//! The pipelined runner's [`crate::staged::QueueBudget`] tracks the
//! aggregate bytes of every packet that has left the demuxer but not yet
//! been consumed by the next stage. Round-126 wired the byte ceiling
//! through to back-pressure the demuxer; round-127 surfaces the running
//! total to engines via [`Progress::queue_bytes`] so a player UI can
//! visualise back-pressure (or a stress harness can verify it is
//! actually happening) without poking at private state.
//!
//! Asserts:
//!
//! 1. **`queue_bytes` is `0` when no ceiling is configured.** The
//!    [`QueueBudget`] short-circuits its admit/release when `max == 0`,
//!    so the running total never moves off zero. A run that never calls
//!    `with_max_queue_bytes` must report `0` end-to-end — even mid-run
//!    while packets are flowing — confirming the disabled path is a
//!    true no-op (no accounting overhead).
//!
//! 2. **`queue_bytes` returns to `0` at EOF.** Whether or not a ceiling
//!    is in force, every admitted packet must have a matching release
//!    by the time the demuxer drains. The `eof: true` `Progress` is
//!    sent after all worker threads have joined, so the in-flight count
//!    *must* be zero — anything else indicates a leak in the accountant.
//!
//! 3. **`queue_bytes` reports non-zero while back-pressure is active.**
//!    With a tight ceiling + a sink that artificially delays writes, the
//!    demuxer parks while the consumer drains; mid-run progress events
//!    must surface a non-zero `queue_bytes` so the engine can observe
//!    that the pipeline is in fact buffering, not stalled for some
//!    other reason. This is the diagnostic value: an operator who sees
//!    `queue_bytes == max_queue_bytes` knows the byte budget is the
//!    binding constraint, not the count caps or a slow encoder.

mod common;

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{Executor, Job, JobSink};

/// A test sink whose `write_packet` / `write_frame` can be artificially
/// throttled via a shared `Duration`. Used to force the demuxer→worker
/// queues to fill up so the byte budget actually binds — without the
/// delay, the sink drains as fast as the demuxer produces and the
/// in-flight count never grows.
struct ThrottledSink {
    tx: SyncSender<()>,
    delay: Arc<Mutex<Duration>>,
}

impl JobSink for ThrottledSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        let d = *self.delay.lock().unwrap();
        if d > Duration::ZERO {
            std::thread::sleep(d);
        }
        let _ = self.tx.try_send(());
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        let d = *self.delay.lock().unwrap();
        if d > Duration::ZERO {
            std::thread::sleep(d);
        }
        let _ = self.tx.try_send(());
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Spawn an executor with the given byte budget + sink throttle. Returns
/// the handle so the test can poll `try_progress`. The job is a stream-
/// copy of the stub audio source — packets pass straight through the
/// demuxer→copy-stage→mux channels, which is exactly the queue the
/// byte ceiling guards.
fn spawn_throttled(
    max_queue_bytes: u64,
    sink_delay: Duration,
    name: &str,
) -> (oxideav_pipeline::ExecutorHandle, Receiver<()>) {
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
    let sink = Box::new(ThrottledSink {
        tx: write_tx,
        delay: Arc::new(Mutex::new(sink_delay)),
    });

    let handle = Executor::new(job, ctx)
        .with_sink_override("@display", sink)
        .with_threads(2)
        .with_max_queue_bytes(max_queue_bytes)
        .spawn()
        .expect("spawn executor");
    (handle, write_rx)
}

#[test]
fn queue_bytes_zero_when_ceiling_disabled() {
    // A run that never calls `with_max_queue_bytes` (or passes `0`) gets
    // the short-circuit path: the QueueBudget never accounts anything.
    // Every Progress emission must surface `queue_bytes == 0` end-to-end
    // — pre-run, mid-run, post-EOF.
    let (handle, write_rx) = spawn_throttled(0, Duration::ZERO, "queue_bytes_disabled");

    // Drain sink writes in the background so the executor doesn't park
    // on a full bounded channel.
    let _drain = std::thread::spawn(
        move || {
            while write_rx.recv_timeout(Duration::from_secs(2)).is_ok() {}
        },
    );

    // Poll progress for a while, capturing every snapshot. Even under
    // sustained back-pressure-shaped load the value must stay at zero
    // because the accountant is disabled.
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut saw_any = false;
    let mut max_observed = 0u64;
    while Instant::now() < deadline {
        if let Some(p) = handle.try_progress() {
            saw_any = true;
            if p.queue_bytes > max_observed {
                max_observed = p.queue_bytes;
            }
            if p.eof {
                break;
            }
        }
        if handle.has_finished() {
            // Drain trailing progress emissions.
            while let Some(p) = handle.try_progress() {
                if p.queue_bytes > max_observed {
                    max_observed = p.queue_bytes;
                }
            }
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        saw_any,
        "no progress events arrived within 3s — pipeline stalled"
    );
    assert_eq!(
        max_observed, 0,
        "queue_bytes must stay at 0 with the ceiling disabled \
         (observed max = {max_observed}) — the QueueBudget should short-circuit \
         its accounting when max == 0"
    );
    let _ = handle.stop();
}

#[test]
fn queue_bytes_returns_to_zero_at_eof() {
    // Whether or not a ceiling is in force, every admit must have a
    // matching release. The EOF progress event is emitted after all
    // worker threads have joined, so the running total *must* be zero
    // by then — anything else is an accounting leak.
    //
    // Use a moderate ceiling (4096 B) — large enough to never bind on
    // the stub's small packets, small enough that the accountant is
    // enabled and observed-zero is a meaningful assertion.
    let (handle, write_rx) = spawn_throttled(4096, Duration::ZERO, "queue_bytes_zero_at_eof");
    let _drain = std::thread::spawn(
        move || {
            while write_rx.recv_timeout(Duration::from_secs(2)).is_ok() {}
        },
    );

    // Wait until we see an `eof: true` progress event.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut eof_progress: Option<oxideav_pipeline::staged::Progress> = None;
    while Instant::now() < deadline {
        if let Some(p) = handle.try_progress() {
            if p.eof {
                eof_progress = Some(p);
                break;
            }
        }
        if handle.has_finished() {
            // Drain any trailing emissions and stop polling.
            while let Some(p) = handle.try_progress() {
                if p.eof {
                    eof_progress = Some(p);
                }
            }
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let _ = handle.stop();

    let p = eof_progress.expect("never observed an eof:true progress event");
    assert_eq!(
        p.queue_bytes, 0,
        "queue_bytes at EOF must be 0 — every admitted packet should \
         have a matching release by the time workers join; observed = {} \
         indicates an accounting leak somewhere in the demuxer/copy path",
        p.queue_bytes
    );
}

#[test]
fn queue_bytes_non_zero_under_active_backpressure() {
    // With a tight ceiling and a sink that takes ~5 ms per write, the
    // demuxer parks on the byte budget while the copy stage waits for
    // the bounded output channel to drain. At least one mid-run
    // Progress emission must surface a non-zero queue_bytes — that's
    // the whole diagnostic point of the field.
    //
    // The stub emits 100 ms-of-audio packets (~1600 B each). A 4096 B
    // ceiling lets ~2-3 packets in flight before parking. With a 5 ms
    // sink delay, the queue stays populated for the bulk of the run.
    let (handle, write_rx) =
        spawn_throttled(4096, Duration::from_millis(5), "queue_bytes_backpressure");
    let _drain = std::thread::spawn(move || {
        // Slow drain — let the sink delay actually bite. We drop writes
        // we don't pick up; the channel is sync_channel(1024) so it
        // won't block early.
        while write_rx.recv_timeout(Duration::from_secs(2)).is_ok() {}
    });

    // Poll progress for a bounded window, tracking the peak queue_bytes
    // observed. With back-pressure active we expect to see at least one
    // event where `queue_bytes > 0`.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut peak = 0u64;
    let mut saw_any = false;
    while Instant::now() < deadline {
        if let Some(p) = handle.try_progress() {
            saw_any = true;
            if p.queue_bytes > peak {
                peak = p.queue_bytes;
            }
            if peak > 0 && !p.eof {
                // We've seen the back-pressure signal we needed — done
                // early so the test finishes promptly. The throttled
                // sink would otherwise hold the stream open for tens of
                // seconds.
                break;
            }
            if p.eof {
                break;
            }
        }
        if handle.has_finished() {
            while let Some(p) = handle.try_progress() {
                if p.queue_bytes > peak {
                    peak = p.queue_bytes;
                }
            }
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    handle.request_abort();
    let _ = handle.stop();
    assert!(
        saw_any,
        "no progress events arrived within 5s — pipeline stalled"
    );
    assert!(
        peak > 0,
        "queue_bytes never went above 0 despite a tight 4096-byte ceiling \
         and a 5ms-per-write sink — back-pressure either didn't engage \
         (suggests the count caps drained first) or the accountant isn't \
         surfacing in-flight bytes through Progress"
    );
}
