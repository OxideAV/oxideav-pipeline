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

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_core::{Error, Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{Executor, Job, JobSink};

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
    // Synthetic 60-second mono PCM stream. The stub demuxer ignores
    // the file's contents — touching a `.stub` file just satisfies
    // the source registry's `file://` open. Long enough that the
    // bounded channels between demuxer/decoder/mux are realistically
    // full by the time the sink errors out.
    let src = common::stub::touch("pipelined_abort");

    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);

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
        Executor::new(&job, &ctx)
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
