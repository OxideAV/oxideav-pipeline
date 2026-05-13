//! Contract test: the [`BarrierKind::SeekFlush`] barrier must carry
//! the demuxer's actual landed pts (and the matching `time_base`)
//! end-to-end, not the caller's requested pts.
//!
//! Routes through [`common::stub::FixedLandingStubDemuxer`], whose
//! `seek_to` always returns `Ok(FIXED_LANDED_PTS = 42)` regardless of
//! the requested target. After issuing a seek to a wildly different
//! pts (e.g. 30 s ≈ 240 000 samples at 8 kHz), the sink must observe
//! `SeekFlush { landed_pts: 42, .. }` — proving the demuxer's return
//! value reaches downstream consumers without being rewritten on the
//! way through the pipeline.
//!
//! Why this matters: pre-fix the engine in oxideplay re-anchored its
//! master clock at "next audio packet's pts", which is typically
//! 50-200 ms after the actual landing (video lands on a keyframe
//! ≤ target, audio lands on the next packet ≥ target). Atomic
//! anchoring at `landed_pts` eliminates that drift entirely.

mod common;

use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{BarrierKind, Executor, Job, JobSink};

use common::stub::FIXED_LANDED_PTS;

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

#[test]
fn seek_flush_carries_landed_pts() {
    let src = common::stub::touch_fixed("seek_flush_landed_pts");

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

    // Drain a few pre-seek payloads so the pipeline is warm.
    let _ = wait_for(&rx, Instant::now() + Duration::from_secs(2), |e| {
        matches!(e, SinkEvent::Payload { .. })
    });

    // Ask for ~30 s — totally different from FIXED_LANDED_PTS (42
    // ticks ≈ 5.25 ms at 8 kHz). The fixed-landing demuxer ignores
    // the request and reports 42 anyway.
    let target_pts = (30.0_f64 / audio_tb.as_rational().as_f64()).round() as i64;
    handle
        .seek(audio_idx, target_pts, audio_tb)
        .expect("seek dispatch");

    // The barrier must carry the demuxer's *actual* landed pts (42),
    // NOT the requested target (~240 000).
    let barrier_deadline = Instant::now() + Duration::from_secs(5);
    let evt = wait_for(&rx, barrier_deadline, |e| {
        matches!(e, SinkEvent::Barrier(_))
    })
    .expect("Barrier never arrived after seek");
    let SinkEvent::Barrier(b) = evt else {
        unreachable!()
    };
    match b {
        BarrierKind::SeekFlush {
            generation,
            landed_pts,
            time_base,
        } => {
            assert_eq!(generation, 1, "first seek must produce generation = 1");
            assert_eq!(
                landed_pts, FIXED_LANDED_PTS,
                "barrier must surface the demuxer's actual landed pts ({}), \
                 not the caller's requested target ({})",
                FIXED_LANDED_PTS, target_pts
            );
            assert_eq!(
                time_base, audio_tb,
                "barrier must carry the SeekCmd's time_base verbatim so \
                 consumers can convert landed_pts to wall-clock without \
                 re-resolving the stream's tb"
            );
        }
        BarrierKind::SeekRejected { .. } => {
            panic!("fixed-landing stub returns Ok(42); SeekFlush expected")
        }
    }

    // Drain to clean shutdown.
    let drainer = std::thread::spawn(
        move || {
            while rx.recv_timeout(Duration::from_millis(500)).is_ok() {}
        },
    );
    let _stats = handle.stop().expect("stop executor");
    let _ = drainer.join();
}
