//! Contract test for [`Executor::with_max_queue_bytes`].
//!
//! [`Executor::with_channel_caps`] bounds the demuxer→worker packet
//! queues by *element count*. `with_max_queue_bytes` adds an orthogonal
//! *byte* ceiling: the demuxer parks before reading the next packet
//! while the aggregate in-flight packet bytes are at or above the
//! ceiling, and the consuming stage (copy/decode) frees the bytes the
//! instant it pulls the packet off the channel.
//!
//! Asserts:
//!
//! 1. **A byte ceiling smaller than a single packet still runs to
//!    completion.** This is the deadlock guard: a lone packet larger
//!    than the whole budget must be *admitted* (we cross the line by one
//!    packet) rather than parking the demuxer forever waiting for an
//!    in-flight total that can never drop low enough. If the runner
//!    deadlocked under a sub-packet ceiling the test would time out.
//!
//! 2. **`0` (the default / disabled) matches an unconfigured run.** A
//!    run that never calls `with_max_queue_bytes` must produce the same
//!    payload count as one passing `0` explicitly — confirms the
//!    disabled path is a true no-op (no admit/release, no parking).
//!
//! 3. **A byte ceiling does not drop or duplicate packets.** Back-
//!    pressure only changes *when* the demuxer advances, never *what*
//!    flows through. A tight ceiling must yield the exact same payload
//!    count as the unbounded run.
//!
//! 4. **The byte ceiling composes with the count caps.** Both knobs set
//!    together still runs to completion and preserves the payload count
//!    — whichever binds first applies, neither corrupts the stream.

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

/// Run a copy-only audio job to completion under a given byte ceiling
/// (`max_queue_bytes`) and optional [`ChannelCaps`], returning the total
/// payload count the sink observed. The stub demuxer emits a
/// deterministic number of packets, so the count is reproducible across
/// configurations — the whole point of the byte-ceiling contract is that
/// it must not change.
fn run_with_budget(max_queue_bytes: u64, caps: Option<ChannelCaps>, name: &str) -> u64 {
    let src = common::stub::touch(name);

    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);

    // `copy: true` keeps the route stream-copy — packets pass straight
    // through the demuxer→copy-stage→mux channels, which is exactly the
    // queue the byte ceiling guards. No decode stage is dragged in.
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
        .with_threads(2)
        .with_max_queue_bytes(max_queue_bytes);
    if let Some(caps) = caps {
        exec = exec.with_channel_caps(caps);
    }

    let _stats = exec.run().expect("executor run");

    // Drain trailing events; sanity-check the lifecycle bookended the run.
    let mut saw_start = false;
    let mut saw_finish = false;
    let deadline = Instant::now() + Duration::from_secs(5);
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
fn sub_packet_ceiling_runs_to_completion() {
    // 1 byte is smaller than any packet the stub emits. The demuxer must
    // admit the packet that crosses the ceiling (rather than park forever
    // on a total that can never fall below 1) so the run completes. A
    // deadlock here would trip the test runner's timeout.
    let count = run_with_budget(1, None, "max_queue_bytes_sub_packet");
    assert!(
        count > 0,
        "sub-packet byte ceiling produced no payloads — demuxer likely deadlocked \
         on a packet larger than the whole budget"
    );
}

#[test]
fn zero_budget_matches_unconfigured() {
    // `0` disables the byte ceiling — admit/release short-circuit and the
    // demuxer never parks. A run that never sets a budget hits the same
    // default field (0), so the two must agree exactly.
    let count_zero = run_with_budget(0, None, "max_queue_bytes_zero");
    let count_default = {
        let src = common::stub::touch("max_queue_bytes_unconfigured");
        let mut ctx = oxideav_core::RuntimeContext::new();
        common::stub::register(&mut ctx.codecs, &mut ctx.containers);
        oxideav_source::register(&mut ctx);
        let job_json = format!(
            r#"{{
                "@in":   {{"all": [{{"from": "{}"}}]}},
                "@out":  {{"audio": [{{"from": "@in", "copy": true}}]}}
            }}"#,
            src.display().to_string().replace('\\', "\\\\"),
        );
        let job = Job::from_json(&job_json).expect("parse job");
        let (tx, _rx) = mpsc::sync_channel::<SinkEvent>(1024);
        let payloads = Arc::new(Mutex::new(0u64));
        let sink = Box::new(CountingSink {
            tx,
            payloads: payloads.clone(),
        });
        // Note: no `.with_max_queue_bytes(..)` call here at all.
        let exec = Executor::new(&job, &ctx)
            .with_sink_override("@out", sink)
            .with_threads(2);
        let _ = exec.run().expect("executor run");
        let n = *payloads.lock().unwrap();
        n
    };
    assert_eq!(
        count_zero, count_default,
        "with_max_queue_bytes(0) ({count_zero}) and an unconfigured run ({count_default}) \
         must process the same packet count — both leave the ceiling disabled"
    );
    assert!(count_zero > 0, "zero/unconfigured run produced no payloads");
}

#[test]
fn tight_ceiling_preserves_payload_count() {
    // Back-pressure changes *when* the demuxer advances, never *what*
    // flows. A tight (but multi-packet) ceiling must produce exactly the
    // same payload count as the unbounded run — no drops, no dupes.
    let unbounded = run_with_budget(0, None, "max_queue_bytes_unbounded_ref");
    let bounded = run_with_budget(64, None, "max_queue_bytes_tight");
    assert_eq!(
        unbounded, bounded,
        "a 64-byte ceiling ({bounded}) must yield the same payload count as the \
         unbounded run ({unbounded}) — back-pressure must not drop or duplicate packets"
    );
    assert!(unbounded > 0, "reference run produced no payloads");
}

#[test]
fn byte_ceiling_composes_with_channel_caps() {
    // Both knobs set together: the count caps clamp to 1 element, the
    // byte ceiling clamps to a sub-packet 1 byte. Whichever binds first
    // applies; the run must still complete and preserve the count.
    let reference = run_with_budget(0, None, "max_queue_bytes_compose_ref");
    let combined = run_with_budget(
        1,
        Some(ChannelCaps {
            packets: 1,
            frames: 1,
        }),
        "max_queue_bytes_compose",
    );
    assert_eq!(
        reference, combined,
        "byte ceiling + tight count caps ({combined}) must process the same packet count \
         as the unbounded run ({reference})"
    );
    assert!(combined > 0, "combined-knobs run produced no payloads");
}
