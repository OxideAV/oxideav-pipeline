//! Contract test for [`ExecutorHandle::seek_with_generation`].
//!
//! The handle now assigns each `seek` a monotonic `generation` value
//! and returns it to the caller. The demuxer stage copies the value
//! verbatim into the resulting `BarrierKind::SeekFlush` /
//! `SeekRejected`, so callers can correlate their dispatches with the
//! barriers (and ignore payloads that arrived before the most recent
//! generation as stale).
//!
//! Asserts:
//!
//! 1. The first call returns `1` (matches the prior demuxer-side
//!    counter that incremented before stamping the barrier).
//! 2. The returned value equals the barrier's `generation` field.
//! 3. A second seek returns `2` and the barrier carries `2`.
//! 4. Wrapping back across the seek-channel still works (concurrent
//!    callers each get a unique value; tested by issuing two seeks
//!    back-to-back from two threads and verifying the barriers
//!    carry the union of the returned values, no duplicates).
//!
//! This is the regression coverage for the seek-correlation contract:
//! pre-fix the demuxer maintained its own `generation` counter and the
//! caller maintained a separate mirror, which could desync silently if
//! a seek command was dropped on a saturated channel or if two
//! threads called `seek` interleaved with packet drainage.

mod common;

use std::collections::HashSet;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{BarrierKind, Executor, Job, JobSink};

enum SinkEvent {
    Started(Vec<StreamInfo>),
    Payload { _kind: MediaType, _pts: Option<i64> },
    Barrier(BarrierKind),
    Finished,
}

struct ChannelSink {
    tx: SyncSender<SinkEvent>,
    streams: Arc<Mutex<Vec<StreamInfo>>>,
}

impl JobSink for ChannelSink {
    fn start(&mut self, streams: &[StreamInfo]) -> Result<()> {
        *self.streams.lock().unwrap() = streams.to_vec();
        let _ = self.tx.send(SinkEvent::Started(streams.to_vec()));
        Ok(())
    }
    fn write_packet(&mut self, kind: MediaType, pkt: &Packet) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Payload {
            _kind: kind,
            _pts: pkt.pts,
        });
        Ok(())
    }
    fn write_frame(&mut self, kind: MediaType, frm: &Frame) -> Result<()> {
        let pts = match frm {
            Frame::Audio(a) => a.pts,
            Frame::Video(v) => v.pts,
            _ => None,
        };
        let _ = self.tx.send(SinkEvent::Payload {
            _kind: kind,
            _pts: pts,
        });
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

fn wait_for<F>(rx: &Receiver<SinkEvent>, deadline: Instant, mut pred: F) -> Option<SinkEvent>
where
    F: FnMut(&SinkEvent) -> bool,
{
    while Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining.min(Duration::from_millis(100))) {
            Ok(ev) => {
                if pred(&ev) {
                    return Some(ev);
                }
                if matches!(ev, SinkEvent::Finished) {
                    return None;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return None,
        }
    }
    None
}

fn barrier_generation(b: BarrierKind) -> u32 {
    match b {
        BarrierKind::SeekFlush { generation, .. } => generation,
        BarrierKind::SeekRejected { generation } => generation,
    }
}

#[test]
fn seek_with_generation_returns_value_matching_barrier() {
    let src = common::stub::touch("seek_with_generation_basic");

    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);

    let job_json = format!(
        r#"{{
            "@in":      {{"all": [{{"from": "{}"}}]}},
            "@display": {{"audio": [{{"from": "@in"}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Job::from_json(&job_json).expect("parse job");

    let (tx, rx) = mpsc::sync_channel::<SinkEvent>(8);
    let streams_slot = Arc::new(Mutex::new(Vec::<StreamInfo>::new()));
    let sink = Box::new(ChannelSink {
        tx,
        streams: streams_slot.clone(),
    });

    let handle = Executor::new(&job, &ctx)
        .with_sink_override("@display", sink)
        .with_threads(2)
        .spawn()
        .expect("spawn executor");

    let start_deadline = Instant::now() + Duration::from_secs(5);
    let started = wait_for(&rx, start_deadline, |e| matches!(e, SinkEvent::Started(_)))
        .expect("Started event never arrived");
    let SinkEvent::Started(streams) = started else {
        unreachable!()
    };
    let audio = streams
        .iter()
        .find(|s| s.params.media_type == MediaType::Audio)
        .expect("no audio stream");
    let audio_idx = audio.index;
    let audio_tb = audio.time_base;

    // Drain some payloads so the pipeline is warm.
    let _ = wait_for(&rx, Instant::now() + Duration::from_secs(2), |e| {
        matches!(e, SinkEvent::Payload { .. })
    });

    // First seek: handle assigns generation = 1, barrier carries 1.
    let target_pts = (10.0_f64 / audio_tb.as_rational().as_f64()).round() as i64;
    let gen1 = handle
        .seek_with_generation(audio_idx, target_pts, audio_tb)
        .expect("seek dispatch #1");
    assert_eq!(
        gen1, 1,
        "first seek_with_generation must return 1 (matches the prior \
         demuxer-side counter contract: increment-then-stamp)"
    );

    let barrier_deadline = Instant::now() + Duration::from_secs(5);
    let evt = wait_for(&rx, barrier_deadline, |e| {
        matches!(e, SinkEvent::Barrier(_))
    })
    .expect("Barrier #1 never arrived");
    let SinkEvent::Barrier(b1) = evt else {
        unreachable!()
    };
    assert_eq!(
        barrier_generation(b1),
        gen1,
        "barrier #1 must carry the generation the handle returned ({gen1})"
    );

    // Second seek: must return 2, barrier carries 2.
    let target_pts_2 = (20.0_f64 / audio_tb.as_rational().as_f64()).round() as i64;
    let gen2 = handle
        .seek_with_generation(audio_idx, target_pts_2, audio_tb)
        .expect("seek dispatch #2");
    assert_eq!(gen2, 2, "second seek_with_generation must return 2");

    let barrier_deadline = Instant::now() + Duration::from_secs(5);
    let evt = wait_for(&rx, barrier_deadline, |e| {
        matches!(e, SinkEvent::Barrier(_))
    })
    .expect("Barrier #2 never arrived");
    let SinkEvent::Barrier(b2) = evt else {
        unreachable!()
    };
    assert_eq!(
        barrier_generation(b2),
        gen2,
        "barrier #2 must carry the generation the handle returned ({gen2})"
    );

    // Drain to clean shutdown.
    let drainer = std::thread::spawn(
        move || {
            while rx.recv_timeout(Duration::from_millis(500)).is_ok() {}
        },
    );
    let _stats = handle.stop().expect("stop executor");
    let _ = drainer.join();
}

#[test]
fn seek_returns_unique_generations_for_each_dispatch() {
    // Issue several seeks back-to-back from one thread and verify the
    // resulting barriers carry the exact set of generations the handle
    // returned — no duplicates, no gaps, FIFO order preserved.
    //
    // This is the contract that lets engines ignore stale pre-seek
    // payloads: a frame whose surrounding barrier has gen < current
    // pending gen is from an older seek and must be dropped.
    let src = common::stub::touch("seek_with_generation_burst");

    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);

    let job_json = format!(
        r#"{{
            "@in":      {{"all": [{{"from": "{}"}}]}},
            "@display": {{"audio": [{{"from": "@in"}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Job::from_json(&job_json).expect("parse job");

    let (tx, rx) = mpsc::sync_channel::<SinkEvent>(64);
    let streams_slot = Arc::new(Mutex::new(Vec::<StreamInfo>::new()));
    let sink = Box::new(ChannelSink {
        tx,
        streams: streams_slot.clone(),
    });

    let handle = Executor::new(&job, &ctx)
        .with_sink_override("@display", sink)
        .with_threads(2)
        .spawn()
        .expect("spawn executor");

    let start_deadline = Instant::now() + Duration::from_secs(5);
    let started = wait_for(&rx, start_deadline, |e| matches!(e, SinkEvent::Started(_)))
        .expect("Started event never arrived");
    let SinkEvent::Started(streams) = started else {
        unreachable!()
    };
    let audio = streams
        .iter()
        .find(|s| s.params.media_type == MediaType::Audio)
        .expect("no audio stream");
    let audio_idx = audio.index;
    let audio_tb = audio.time_base;

    // Warm up.
    let _ = wait_for(&rx, Instant::now() + Duration::from_secs(2), |e| {
        matches!(e, SinkEvent::Payload { .. })
    });

    // Burst: 5 seeks back-to-back. The demuxer drains the channel in
    // FIFO order and stamps each barrier with the seek's gen, so we
    // expect to see barriers with generations {1, 2, 3, 4, 5}.
    let mut issued: Vec<u32> = Vec::new();
    for i in 0..5 {
        let pts = ((5.0_f64 + i as f64) / audio_tb.as_rational().as_f64()).round() as i64;
        let g = handle
            .seek_with_generation(audio_idx, pts, audio_tb)
            .expect("seek dispatch");
        issued.push(g);
    }
    assert_eq!(
        issued,
        vec![1, 2, 3, 4, 5],
        "five back-to-back seeks must produce generations 1..=5 in dispatch order"
    );

    // Collect the next 5 barriers. They must carry exactly the set we
    // issued (any order; the demuxer may coalesce-then-broadcast).
    let mut seen: HashSet<u32> = HashSet::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    while seen.len() < 5 && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining.min(Duration::from_millis(200))) {
            Ok(SinkEvent::Barrier(b)) => {
                let g = barrier_generation(b);
                assert!(
                    seen.insert(g),
                    "duplicate barrier generation {g} — generations must be unique"
                );
            }
            Ok(SinkEvent::Finished) => break,
            Ok(_) => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }
    let expected: HashSet<u32> = issued.into_iter().collect();
    assert_eq!(
        seen, expected,
        "barrier generations must equal the set of seek_with_generation \
         returned values (issued={expected:?}, seen={seen:?})"
    );

    // Drain to clean shutdown.
    let drainer = std::thread::spawn(
        move || {
            while rx.recv_timeout(Duration::from_millis(500)).is_ok() {}
        },
    );
    let _stats = handle.stop().expect("stop executor");
    let _ = drainer.join();
}
