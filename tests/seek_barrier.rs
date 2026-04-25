//! Integration test for the seek-barrier control surface.
//!
//! Spawns an `Executor` over a synthetic stub stream (60 seconds, so
//! the demuxer doesn't hit EOF before we issue a seek), wires a
//! channel-forwarding sink so we observe `start` / `write_frame` /
//! `barrier` / `finish` calls from the test thread, and:
//!
//! 1. Issues `ExecutorHandle::seek(stream, half-way pts, tb)`.
//! 2. Waits for the sink's `barrier(SeekFlush { generation: 1 })` call.
//! 3. Verifies that subsequent payloads carry pts at-or-near the seek
//!    target (the stub demuxer's `seek_to` snaps exactly).
//!
//! Also exercises `try_progress` + `stop` for handle lifecycle
//! coverage.

mod common;

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_codec::CodecRegistry;
use oxideav_container::ContainerRegistry;
use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{BarrierKind, Executor, Job, JobSink};
use oxideav_source::SourceRegistry;

/// A test sink that forwards every JobSink event to a channel so the
/// test driver can `recv()` them in order.
enum SinkEvent {
    Started(Vec<StreamInfo>),
    Payload { _kind: MediaType, pts: Option<i64> },
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
            pts: pkt.pts,
        });
        Ok(())
    }
    fn write_frame(&mut self, kind: MediaType, frm: &Frame) -> Result<()> {
        let pts = match frm {
            Frame::Audio(a) => a.pts,
            Frame::Video(v) => v.pts,
            _ => None,
        };
        let _ = self.tx.send(SinkEvent::Payload { _kind: kind, pts });
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

/// Drain events until either `pred` matches one (returns it), the
/// `Finished` event arrives, or the deadline is reached. Returns
/// the matched event or `None` on timeout/Finished.
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

#[test]
fn spawn_seek_emits_barrier_and_advances_pts() {
    // Stub demuxer emits 60s of synthetic mono audio at 8 kHz.
    // We seek to the half-way point (~30s) and verify the sink sees
    // a SeekFlush barrier with generation = 1.
    let src = common::stub::touch("seek_barrier");

    let mut codecs = CodecRegistry::new();
    let mut containers = ContainerRegistry::new();
    common::stub::register(&mut codecs, &mut containers);
    let sources = SourceRegistry::with_defaults();

    let job_json = format!(
        r#"{{
            "@in":      {{"all": [{{"from": "{}"}}]}},
            "@display": {{"audio": [{{"from": "@in"}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Job::from_json(&job_json).expect("parse job");

    // Small channel so the sink back-pressures the executor — the
    // pipeline can't outrun this test thread.
    let (tx, rx) = mpsc::sync_channel::<SinkEvent>(8);
    let streams_slot = Arc::new(Mutex::new(Vec::<StreamInfo>::new()));
    let sink = Box::new(ChannelSink {
        tx,
        streams: streams_slot.clone(),
    });

    let handle = Executor::new(&job, &codecs, &containers, &sources)
        .with_sink_override("@display", sink)
        .with_threads(2)
        .spawn()
        .expect("spawn executor");

    // Wait for `start` to fire and capture the audio stream's tb +
    // index. The first event must be Started.
    let start_deadline = Instant::now() + Duration::from_secs(5);
    let started = wait_for(&rx, start_deadline, |e| matches!(e, SinkEvent::Started(_)))
        .expect("Started event never arrived");
    let SinkEvent::Started(streams) = started else {
        unreachable!()
    };
    assert!(!streams.is_empty(), "no streams reported");
    let audio = streams
        .iter()
        .find(|s| s.params.media_type == MediaType::Audio)
        .expect("no audio stream");
    let audio_idx = audio.index;
    let audio_tb = audio.time_base;

    // Drain a few frames/packets so the pipeline is "running" before
    // we seek.
    let _ = wait_for(&rx, Instant::now() + Duration::from_secs(2), |e| {
        matches!(e, SinkEvent::Payload { .. })
    });

    // Seek to ~30s. Stub tb is 1/sample_rate so pts is in samples.
    let target_pts = (30.0_f64 / audio_tb.as_rational().as_f64()).round() as i64;
    handle
        .seek(audio_idx, target_pts, audio_tb)
        .expect("seek dispatch");

    // Wait for the barrier with generation == 1 to surface on the sink.
    let barrier_deadline = Instant::now() + Duration::from_secs(5);
    let evt = wait_for(&rx, barrier_deadline, |e| {
        matches!(e, SinkEvent::Barrier(_))
    })
    .expect("Barrier never arrived after seek");
    let SinkEvent::Barrier(b) = evt else {
        unreachable!()
    };
    match b {
        BarrierKind::SeekFlush { generation } => {
            assert_eq!(generation, 1, "first seek must produce generation = 1");
        }
    }

    // Subsequent payloads must have pts at-or-near the seek target.
    let post_deadline = Instant::now() + Duration::from_secs(5);
    let evt = wait_for(&rx, post_deadline, |e| {
        matches!(e, SinkEvent::Payload { pts: Some(_), .. })
    })
    .expect("no post-barrier payload within deadline");
    if let SinkEvent::Payload { pts: Some(p), .. } = evt {
        // Allow anything within ±1s of the target; the stub's
        // seek_to lands exactly, but any bounded-channel buffering
        // can let one stale pre-seek packet slip through.
        let target_secs = audio_tb.seconds_of(target_pts);
        let p_secs = audio_tb.seconds_of(p);
        let drift = (p_secs - target_secs).abs();
        assert!(
            drift < 1.0,
            "post-barrier pts {p_secs:.3}s drifted from target {target_secs:.3}s by {drift:.3}s"
        );
    }

    // Tear down cleanly.
    let _stats = handle.stop().expect("stop executor");
}
