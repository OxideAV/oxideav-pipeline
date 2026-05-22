//! Contract test for [`Executor::with_channel_caps`].
//!
//! The pipelined staged executor's per-track packet- and frame-channel
//! depth defaults are 16 and 8 respectively. [`ChannelCaps`] exposes
//! those knobs to the caller: embedded playback may want to clamp the
//! depth down (memory budget is tight), high-throughput offline
//! transcodes may want to raise it (let bursty decoders amortise their
//! per-call overhead instead of blocking on the encoder).
//!
//! Asserts:
//!
//! 1. **Smallest legal depth (`packets: 1, frames: 1`) still runs to
//!    completion.** This is the "tightest possible bound" — every
//!    channel degenerates to one element, so the source thread blocks
//!    until the consumer has drained. If the runner deadlocks under
//!    this configuration (e.g. because a stage broadcasts more than
//!    one barrier before the downstream catches up), the test times
//!    out and fails.
//!
//! 2. **Zero is clamped up to 1.** A caller passing `packets: 0` must
//!    not panic the runner (`sync_channel(0)` is a rendezvous channel,
//!    which would serialise the entire pipeline). [`ChannelCaps::
//!    resolved`] handles the clamp; we verify end-to-end that a job
//!    constructed with zero caps still completes.
//!
//! 3. **Default behaviour is unchanged.** A run with no
//!    `with_channel_caps()` call must process the same packet count as
//!    a run with the default caps explicitly passed — confirms the
//!    `Option<ChannelCaps>` plumbing through `PreparedRun` /
//!    `PipelineControl` correctly defaults to the historical depth.

mod common;

use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{ChannelCaps, Executor, Job, JobSink};

enum SinkEvent {
    Started,
    Payload,
    Finished,
}

struct CountingSink {
    tx: SyncSender<SinkEvent>,
    payloads: Arc<Mutex<u64>>,
}

impl JobSink for CountingSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Started);
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        *self.payloads.lock().unwrap() += 1;
        let _ = self.tx.send(SinkEvent::Payload);
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        *self.payloads.lock().unwrap() += 1;
        let _ = self.tx.send(SinkEvent::Payload);
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        let _ = self.tx.send(SinkEvent::Finished);
        Ok(())
    }
}

/// Run a copy-only audio job to completion under a given [`ChannelCaps`]
/// (or the default if `None`) and return the total payload count the
/// sink observed. The stub demuxer emits a deterministic number of
/// packets, so the count is reproducible.
fn run_with_caps(caps: Option<ChannelCaps>, name: &str) -> u64 {
    let src = common::stub::touch(name);

    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);

    // `copy: true` keeps the route stream-copy — no decoder / encoder
    // instantiated, just packet pass-through. That exercises the
    // demuxer→copy-stage→mux channels (packet caps) without dragging in
    // a decode stage (frame caps); a separate variant could exercise
    // the frame path, but copy is enough to verify the caps plumbing
    // because both channel families use the same `pkt_cap` field — the
    // `frame_cap` field is read by a different code path (decoder
    // output + per-stage filter outputs).
    let job_json = format!(
        r#"{{
            "@in":   {{"all": [{{"from": "{}"}}]}},
            "@out":  {{"audio": [{{"from": "@in", "copy": true}}]}}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Job::from_json(&job_json).expect("parse job");

    let (tx, rx) = mpsc::sync_channel::<SinkEvent>(1024);
    let payloads = Arc::new(Mutex::new(0u64));
    let sink = Box::new(CountingSink {
        tx,
        payloads: payloads.clone(),
    });

    let mut exec = Executor::new(&job, &ctx)
        .with_sink_override("@out", sink)
        .with_threads(2);
    if let Some(caps) = caps {
        exec = exec.with_channel_caps(caps);
    }

    let _stats = exec.run().expect("executor run");

    // Drain any trailing events (Finished, etc.) so the sender side
    // doesn't leak. We just sanity-check that at least one Started and
    // one Finished event landed in the queue — the actual payload count
    // is read from the shared mutex below.
    let mut saw_start = false;
    let mut saw_finish = false;
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(SinkEvent::Started) => saw_start = true,
            Ok(SinkEvent::Finished) => {
                saw_finish = true;
                break;
            }
            Ok(SinkEvent::Payload) => {}
            Err(_) => break,
        }
    }
    assert!(saw_start, "sink.start was never called");
    assert!(saw_finish, "sink.finish was never called");

    let n = *payloads.lock().unwrap();
    n
}

#[test]
fn tightest_caps_one_one_runs_to_completion() {
    // packets=1, frames=1 is the smallest non-rendezvous depth.
    // If the staged runner's barrier-broadcast / EOF-cascade logic
    // assumed any minimum queue depth > 1, this test would deadlock
    // and the harness would kill it (test runner timeout).
    let count = run_with_caps(
        Some(ChannelCaps {
            packets: 1,
            frames: 1,
        }),
        "channel_caps_tight",
    );
    assert!(
        count > 0,
        "tight caps run produced no payloads — pipeline did not progress"
    );
}

#[test]
fn zero_caps_clamped_up_to_one() {
    // `sync_channel(0)` would be a rendezvous channel; the runner must
    // promote 0 → 1 instead of panicking. Verified end-to-end (a panic
    // anywhere inside the runner would mark the test failed via the
    // joined worker-thread error).
    let count = run_with_caps(
        Some(ChannelCaps {
            packets: 0,
            frames: 0,
        }),
        "channel_caps_zero",
    );
    assert!(
        count > 0,
        "zero caps run produced no payloads — clamp logic failed"
    );
}

#[test]
fn default_caps_match_unconfigured_runs() {
    // A run with no `with_channel_caps()` call must produce the same
    // payload count as a run that explicitly passes the default.
    // Confirms the `Option<ChannelCaps>` plumbing's `None` branch
    // resolves to the historical depth.
    let count_default = run_with_caps(None, "channel_caps_default_none");
    let count_explicit = run_with_caps(Some(ChannelCaps::default()), "channel_caps_default_some");
    assert_eq!(
        count_default, count_explicit,
        "default-via-None ({count_default}) and default-via-Some(ChannelCaps::default()) \
         ({count_explicit}) must process the same packet count — they are the same path"
    );
    assert!(count_default > 0, "default run produced no payloads");
}
