//! Integration test for the seek-rejected control surface.
//!
//! Mirrors `seek_barrier.rs` but routes through the
//! [`common::stub::NoSeekStubDemuxer`] whose `seek_to` always returns
//! `Error::unsupported`. Asserts:
//!
//! 1. The pipeline does NOT die when a seek is rejected (pre-fix, the
//!    demuxer thread propagated the error and the executor exited).
//! 2. A [`BarrierKind::SeekRejected`] barrier with the matching
//!    generation surfaces on the sink.
//! 3. Audio payloads keep flowing after the rejected seek.
//!
//! This is the regression that the user reported as "if I run oxideplay
//! on any file (mp3, video, etc) I can't seek" — every demuxer that
//! still uses the default `seek_to` (mp3, mov, aac, ac3) hit the same
//! bug, manifesting as the player stalling after the first seek key.

mod common;

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{BarrierKind, Executor, Job, JobSink};

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
fn rejected_seek_emits_seek_rejected_barrier_and_keeps_running() {
    // Stub demuxer (noseek variant): emits 60s of synthetic mono audio
    // and rejects every `seek_to`.
    let src = common::stub::touch_noseek("seek_rejected");

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

    // Get audio stream metadata via the first Started event.
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

    // Drain a few pre-seek payloads so the pipeline is steady-state.
    let _ = wait_for(&rx, Instant::now() + Duration::from_secs(2), |e| {
        matches!(e, SinkEvent::Payload { .. })
    });

    // Issue a seek — the noseek demuxer will reject it.
    let target_pts = (30.0_f64 / audio_tb.as_rational().as_f64()).round() as i64;
    handle
        .seek(audio_idx, target_pts, audio_tb)
        .expect("seek dispatch");

    // Wait for a barrier. It must be SeekRejected with generation == 1.
    let barrier_deadline = Instant::now() + Duration::from_secs(5);
    let evt = wait_for(&rx, barrier_deadline, |e| {
        matches!(e, SinkEvent::Barrier(_))
    })
    .expect("Barrier never arrived after rejected seek (pipeline likely died)");
    let SinkEvent::Barrier(b) = evt else {
        unreachable!()
    };
    match b {
        BarrierKind::SeekRejected { generation } => {
            assert_eq!(
                generation, 1,
                "first rejected seek must report generation = 1"
            );
        }
        other => panic!("expected SeekRejected, got {other:?}"),
    }

    // After the rejected seek, the pipeline must KEEP producing audio
    // payloads from where it was. This is the bug that the user
    // reported: pre-fix the demuxer thread propagated the error and
    // the executor exited, so no further payloads arrived.
    let post_deadline = Instant::now() + Duration::from_secs(5);
    let evt = wait_for(&rx, post_deadline, |e| {
        matches!(e, SinkEvent::Payload { pts: Some(_), .. })
    })
    .expect("no post-rejection payload — pipeline died on rejected seek");
    if let SinkEvent::Payload { pts: Some(p), .. } = evt {
        // pts should be in the pre-seek timeline (i.e. < 30s of audio),
        // not anywhere near the rejected target. The stub emits 100ms
        // packets so by the time the seek + barrier round-trip
        // completes we're at most a few seconds in.
        let p_secs = audio_tb.seconds_of(p);
        assert!(
            p_secs < 25.0,
            "post-rejection pts {p_secs:.3}s is suspiciously close to the rejected target (30s) — demuxer should NOT have moved"
        );
    }

    // Spawn a draining thread BEFORE stop so the executor's final
    // `sink.finish()` send (Finished event) doesn't deadlock the
    // bounded sink channel. Mirrors the fix in `seek_barrier.rs`.
    let drainer = std::thread::spawn(
        move || {
            while rx.recv_timeout(Duration::from_millis(500)).is_ok() {}
        },
    );
    let _stats = handle.stop().expect("stop executor");
    let _ = drainer.join();
}
