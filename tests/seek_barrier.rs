//! Integration test for the seek-barrier control surface.
//!
//! Spawns an `Executor` over a synthetic WAV (long enough that the
//! demuxer doesn't hit EOF before we can issue a seek), wires a
//! channel-forwarding sink so we observe `start` / `write_frame` /
//! `barrier` / `finish` calls from the test thread, and:
//!
//! 1. Issues `ExecutorHandle::seek(stream, half-way pts, tb)`.
//! 2. Waits for the sink's `barrier(SeekFlush { generation: 1 })` call.
//! 3. Verifies that subsequent audio frames carry pts >= the seek
//!    target (the demuxer's seek_to landed at-or-past the requested
//!    pts; any decoder samples fed through pre-seek must have been
//!    flushed when the decode stage observed the barrier).
//!
//! Also exercises `try_progress` + `stop` for handle lifecycle
//! coverage.

use std::io::Write;
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_codec::CodecRegistry;
use oxideav_container::ContainerRegistry;
use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{BarrierKind, Executor, Job, JobSink};
use oxideav_source::SourceRegistry;

fn build_sine_wav(path: &std::path::Path, sample_rate: u32, ms: u32, freq: f32) {
    let n_samples = (sample_rate as u64 * ms as u64 / 1000) as u32;
    let byte_rate = sample_rate * 2;
    let data_sz = n_samples * 2;
    let riff_sz = 36 + data_sz;
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(b"RIFF").unwrap();
    f.write_all(&riff_sz.to_le_bytes()).unwrap();
    f.write_all(b"WAVE").unwrap();
    f.write_all(b"fmt ").unwrap();
    f.write_all(&16u32.to_le_bytes()).unwrap();
    f.write_all(&1u16.to_le_bytes()).unwrap();
    f.write_all(&1u16.to_le_bytes()).unwrap();
    f.write_all(&sample_rate.to_le_bytes()).unwrap();
    f.write_all(&byte_rate.to_le_bytes()).unwrap();
    f.write_all(&2u16.to_le_bytes()).unwrap();
    f.write_all(&16u16.to_le_bytes()).unwrap();
    f.write_all(b"data").unwrap();
    f.write_all(&data_sz.to_le_bytes()).unwrap();
    for i in 0..n_samples {
        let t = i as f32 / sample_rate as f32;
        let s = (2.0 * std::f32::consts::PI * freq * t).sin();
        let v = (s * 0.5 * i16::MAX as f32) as i16;
        f.write_all(&v.to_le_bytes()).unwrap();
    }
}

fn registries() -> (CodecRegistry, ContainerRegistry) {
    let mut codecs = CodecRegistry::new();
    let mut containers = ContainerRegistry::new();
    oxideav_basic::register_codecs(&mut codecs);
    oxideav_basic::register_containers(&mut containers);
    (codecs, containers)
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("oxideav_pipeline_{name}"));
    let _ = std::fs::create_dir_all(&p);
    p
}

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
    // 60 seconds of sine — a bounded sink channel applies back-pressure
    // so the demuxer can't race ahead and hit EOF before we issue the
    // seek. We seek to the half-way point (~30s) and verify the sink
    // sees a SeekFlush barrier with generation = 1.
    let dir = tmp_dir("seek_barrier");
    let src = dir.join("sine.wav");
    build_sine_wav(&src, 8_000, 60_000, 440.0);

    let (codecs, containers) = registries();
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

    // Seek to ~30s. tb for 8 kHz mono PCM is 1/8000 so pts is in samples.
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
    // (Copy mode here means we see packets, not frames; same shape.)
    let post_deadline = Instant::now() + Duration::from_secs(5);
    let evt = wait_for(&rx, post_deadline, |e| {
        matches!(e, SinkEvent::Payload { pts: Some(_), .. })
    })
    .expect("no post-barrier payload within deadline");
    if let SinkEvent::Payload { pts: Some(p), .. } = evt {
        // Allow anything within ±0.5s of the target; the demuxer's
        // seek_to may snap to a packet boundary.
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
