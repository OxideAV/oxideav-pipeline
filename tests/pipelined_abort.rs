//! Regression test: when the sink returns `Err` on the pipelined
//! executor, the run must unwind cleanly instead of deadlocking.
//!
//! The bug: the mux loop would break on the sink error and set the
//! abort flag, but upstream worker threads blocked inside
//! `SyncSender::send()` never observed it (setting an `AtomicBool`
//! doesn't unblock a bounded-channel send, and the mux-end receivers
//! were still alive, so senders never got a disconnection error
//! either). `h.join()` on those workers hung forever, and the caller
//! saw the whole executor freeze until the process was killed.
//!
//! Fix: drop `track_output_rx` before joining workers so every
//! pending send becomes `Err(SendError)` and the cascade propagates
//! up to the demuxer via each worker's existing
//! `tx.send(...).is_err()` branch.

use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_codec::CodecRegistry;
use oxideav_container::ContainerRegistry;
use oxideav_core::{Error, Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{Executor, Job, JobSink};
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

/// Sink that errors on the very first `write_*`. Exists to trigger
/// the abort path in the pipelined runtime on turn 1, when upstream
/// workers are almost certainly mid-`send()` into bounded channels.
struct AbortingSink {
    started: Arc<AtomicBool>,
    write_count: Arc<Mutex<usize>>,
}

impl JobSink for AbortingSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        *self.write_count.lock().unwrap() += 1;
        Err(Error::other("test: abort on first packet"))
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        *self.write_count.lock().unwrap() += 1;
        Err(Error::other("test: abort on first frame"))
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn pipelined_sink_error_unwinds_cleanly() {
    // A few seconds of audio — enough samples that the bounded
    // channel between decoder and mux is realistically full by the
    // time the sink errors out.
    let dir = tmp_dir("pipelined_abort");
    let src = dir.join("sine.wav");
    build_sine_wav(&src, 44_100, 3_000, 440.0);

    let (codecs, containers) = registries();
    let sources = SourceRegistry::with_defaults();

    let job_json = format!(
        r#"{{
            "@in":       {{"all": [{{"from": "{}"}}]}},
            "@display":  {{"audio": [{{"from": "@in"}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Job::from_json(&job_json).expect("parse job");

    let started = Arc::new(AtomicBool::new(false));
    let write_count = Arc::new(Mutex::new(0usize));

    // Run the executor on a worker thread with a short timeout — if
    // we regress the abort path, this will hang and the timeout
    // branch will fail the test rather than the whole suite.
    let t_started = started.clone();
    let t_count = write_count.clone();

    let deadline = Instant::now() + Duration::from_secs(5);
    let handle = std::thread::spawn(move || {
        let sink = Box::new(AbortingSink {
            started: t_started,
            write_count: t_count,
        });
        Executor::new(&job, &codecs, &containers, &sources)
            .with_threads(4) // force the pipelined path
            .with_sink_override("@display", sink)
            .run()
    });

    // Poll the join up to the deadline.
    while Instant::now() < deadline {
        if handle.is_finished() {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    assert!(
        handle.is_finished(),
        "pipelined executor deadlocked on sink error — abort-path receiver drop regression"
    );

    let result = handle.join().expect("executor panicked");
    assert!(
        result.is_err(),
        "executor must propagate the sink error, got Ok({:?})",
        result.as_ref().ok()
    );
    assert!(
        started.load(Ordering::SeqCst),
        "sink.start was never called"
    );
    assert!(
        *write_count.lock().unwrap() >= 1,
        "sink.write_* was never called — didn't reach the abort path"
    );
}
