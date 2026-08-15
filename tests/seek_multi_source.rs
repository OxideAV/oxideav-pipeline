//! Multi-source seek fan-out contract tests.
//!
//! A job whose tracks come from SEVERAL source URIs historically
//! delivered every `ExecutorHandle::seek` to only the first routed
//! source — the other sources kept playing from their old position
//! and silently desynced. The staged runner now fans every `SeekCmd`
//! out to every routed source; each answers with its own barrier
//! (`SeekFlush` with its landed pts, or `SeekRejected` when it has no
//! seek surface), all stamped with the dispatch generation. Pinned
//! here:
//!
//! * **Every source re-anchors** — a two-URI job produces one
//!   `SeekFlush` per track, same generation, both landed at the
//!   target.
//! * **Mixed seekability** — seekable + unseekable URIs in one job
//!   yield one `SeekFlush` + one `SeekRejected` for the SAME
//!   generation, and the stream keeps flowing.

mod common;

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::time::{Duration, Instant};

use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo, TimeBase};
use oxideav_pipeline::{BarrierKind, Executor, Job, JobSink};

enum SinkEvent {
    Payload,
    Barrier(BarrierKind),
    Finished,
}

struct ChannelSink {
    tx: SyncSender<SinkEvent>,
}

impl JobSink for ChannelSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Payload);
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Payload);
        Ok(())
    }
    fn barrier(&mut self, kind: BarrierKind) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Barrier(kind));
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Finished);
        Ok(())
    }
}

/// Collect barrier events until `n` arrive or the deadline passes.
fn collect_barriers(rx: &Receiver<SinkEvent>, n: usize, deadline: Instant) -> Vec<BarrierKind> {
    let mut got = Vec::new();
    while got.len() < n && Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(SinkEvent::Barrier(kind)) => got.push(kind),
            Ok(SinkEvent::Finished) => break,
            Ok(SinkEvent::Payload) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    got
}

/// Wait for the first payload so the pipeline is demonstrably rolling
/// before the seek is dispatched.
fn wait_first_payload(rx: &Receiver<SinkEvent>, deadline: Instant) {
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(SinkEvent::Payload) => return,
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    panic!("pipeline produced no payload before the seek");
}

fn spawn_two_source_job(
    uri_a: &std::path::Path,
    uri_b: &std::path::Path,
) -> (oxideav_pipeline::ExecutorHandle, Receiver<SinkEvent>) {
    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);
    let job = Job::from_json(&format!(
        r#"{{
            "@display": {{"audio": [
                {{"from": "{}"}},
                {{"from": "{}"}}
            ]}}
        }}"#,
        uri_a.display().to_string().replace('\\', "\\\\"),
        uri_b.display().to_string().replace('\\', "\\\\"),
    ))
    .expect("parse job");
    let (tx, rx) = mpsc::sync_channel::<SinkEvent>(4096);
    let handle = Executor::new(&job, &ctx)
        .with_sink_override("@display", Box::new(ChannelSink { tx }))
        .spawn()
        .expect("spawn");
    (handle, rx)
}

#[test]
fn seek_reaches_every_routed_source() {
    // Two independent seekable stub URIs feeding one output. A single
    // seek must re-anchor BOTH: two SeekFlush barriers (one per
    // track), same generation, both landed exactly on the target (the
    // stub demuxer's seek_to snaps exactly).
    let src_a = common::stub::touch("seek_multi_a");
    let src_b = common::stub::touch("seek_multi_b");
    let (handle, rx) = spawn_two_source_job(&src_a, &src_b);
    let deadline = Instant::now() + Duration::from_secs(20);
    wait_first_payload(&rx, deadline);

    // 30 s into the 60 s stream, in 1/8000 ticks.
    let target = 240_000i64;
    let generation = handle
        .seek_with_generation(
            0,
            target,
            TimeBase::new(1, common::stub::SAMPLE_RATE as i64),
        )
        .expect("seek dispatch");

    let barriers = collect_barriers(&rx, 2, deadline);
    assert_eq!(
        barriers.len(),
        2,
        "each of the two sources must emit a barrier, got {barriers:?}"
    );
    for b in &barriers {
        match b {
            BarrierKind::SeekFlush {
                generation: g,
                landed_pts,
                time_base,
            } => {
                assert_eq!(*g, generation, "generation mismatch");
                assert_eq!(*landed_pts, target, "stub seek_to snaps exactly");
                assert_eq!(
                    *time_base,
                    TimeBase::new(1, common::stub::SAMPLE_RATE as i64)
                );
            }
            other => panic!("expected SeekFlush on both tracks, got {other:?}"),
        }
    }
    handle.stop().expect("stop");
}

#[test]
fn mixed_seekability_answers_flush_and_rejected_same_generation() {
    // One seekable + one unseekable URI. The dispatch must reach both:
    // the seekable source flushes, the unseekable one rejects — same
    // generation on both barriers — and packets keep flowing after.
    let src_a = common::stub::touch("seek_multi_mixed_a");
    let src_b = common::stub::touch_noseek("seek_multi_mixed_b");
    let (handle, rx) = spawn_two_source_job(&src_a, &src_b);
    let deadline = Instant::now() + Duration::from_secs(20);
    wait_first_payload(&rx, deadline);

    let target = 240_000i64;
    let generation = handle
        .seek_with_generation(
            0,
            target,
            TimeBase::new(1, common::stub::SAMPLE_RATE as i64),
        )
        .expect("seek dispatch");

    let barriers = collect_barriers(&rx, 2, deadline);
    assert_eq!(
        barriers.len(),
        2,
        "both sources must answer, got {barriers:?}"
    );
    let mut flushes = 0;
    let mut rejects = 0;
    for b in &barriers {
        match b {
            BarrierKind::SeekFlush {
                generation: g,
                landed_pts,
                ..
            } => {
                assert_eq!(*g, generation);
                assert_eq!(*landed_pts, target);
                flushes += 1;
            }
            BarrierKind::SeekRejected { generation: g } => {
                assert_eq!(*g, generation);
                rejects += 1;
            }
        }
    }
    assert_eq!((flushes, rejects), (1, 1), "got {barriers:?}");

    // Stream stays alive after the mixed outcome.
    let mut post_payloads = 0;
    let post_deadline = Instant::now() + Duration::from_secs(10);
    while post_payloads < 5 && Instant::now() < post_deadline {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(SinkEvent::Payload) => post_payloads += 1,
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    assert!(
        post_payloads >= 5,
        "pipeline wedged after mixed seek outcome ({post_payloads} payloads)"
    );
    handle.stop().expect("stop");
}

#[test]
fn seek_burst_delivers_one_barrier_per_source_per_generation() {
    // Rapid-fire seeks (scrubbing): every dispatched generation must
    // produce exactly ONE barrier per routed source — no coalescing
    // that drops a generation on one source but not the other, no
    // duplicates.
    let src_a = common::stub::touch("seek_multi_burst_a");
    let src_b = common::stub::touch("seek_multi_burst_b");
    let (handle, rx) = spawn_two_source_job(&src_a, &src_b);
    let deadline = Instant::now() + Duration::from_secs(20);
    wait_first_payload(&rx, deadline);

    let tb = TimeBase::new(1, common::stub::SAMPLE_RATE as i64);
    let targets = [80_000i64, 160_000, 240_000];
    let mut generations = Vec::new();
    for t in targets {
        generations.push(handle.seek_with_generation(0, t, tb).expect("seek"));
    }

    let barriers = collect_barriers(&rx, 6, deadline);
    assert_eq!(
        barriers.len(),
        6,
        "3 generations x 2 sources, got {barriers:?}"
    );
    for (gen, target) in generations.iter().zip(targets) {
        let matching: Vec<_> = barriers
            .iter()
            .filter(|b| match b {
                BarrierKind::SeekFlush {
                    generation,
                    landed_pts,
                    ..
                } => {
                    if generation == gen {
                        assert_eq!(*landed_pts, target, "generation {gen} landed off-target");
                        true
                    } else {
                        false
                    }
                }
                BarrierKind::SeekRejected { generation } => generation == gen,
            })
            .collect();
        assert_eq!(
            matching.len(),
            2,
            "generation {gen} must appear once per source, got {barriers:?}"
        );
    }
    handle.stop().expect("stop");
}
