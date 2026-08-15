//! Multi-output parallelism contract tests.
//!
//! `Executor::run` on a job with several outputs and `threads ≥ 2`
//! runs the outputs concurrently in document-order waves (width
//! clamped through `ExecutionContext::effective_workers`). Pinned
//! here:
//!
//! * **Completeness + parity** — every output receives its full
//!   stream; the merged `ExecutorStats` match the strictly-sequential
//!   `threads == 1` run of the same job.
//! * **Actual overlap** — two outputs of one wave demonstrably run at
//!   the same time (a rendezvous between their sources succeeds; a
//!   sequential runner would time out).
//! * **Wave width respects the budget** — a 4-output job on a
//!   2-thread budget never has more than 2 outputs pumping at once.
//! * **Error precedence** — the earliest failing output in DOCUMENT
//!   order wins, deterministically, regardless of thread timing;
//!   healthy wave-mates still deliver their full stream; outputs in
//!   waves after the failure never start.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxideav_core::{
    AudioFrame, CodecId, CodecParameters, Error, Frame, FrameSource, MediaType, Packet, Result,
    RuntimeContext, SampleFormat, StreamInfo,
};
use oxideav_pipeline::executor::ExecutorStats;
use oxideav_pipeline::{Executor, Job, JobSink};

// ───────────────────────── shared sink stubs ─────────────────────────

/// Sink that counts every payload (packet or frame) it receives.
struct CountingSink {
    started: Arc<AtomicBool>,
    payloads: Arc<AtomicUsize>,
}

impl CountingSink {
    fn boxed() -> (Box<dyn JobSink + Send>, Arc<AtomicBool>, Arc<AtomicUsize>) {
        let started = Arc::new(AtomicBool::new(false));
        let payloads = Arc::new(AtomicUsize::new(0));
        (
            Box::new(Self {
                started: started.clone(),
                payloads: payloads.clone(),
            }),
            started,
            payloads,
        )
    }
}

impl JobSink for CountingSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        self.payloads.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        self.payloads.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Sink that errors on every payload past `ok_payloads`.
struct FailingSink {
    ok_payloads: usize,
    seen: Arc<AtomicUsize>,
    message: &'static str,
}

impl FailingSink {
    fn poke(&mut self) -> Result<()> {
        let n = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        if n > self.ok_payloads {
            return Err(Error::other(self.message));
        }
        Ok(())
    }
}

impl JobSink for FailingSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        self.poke()
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        self.poke()
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

fn stub_ctx() -> RuntimeContext {
    let mut ctx = RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);
    ctx
}

fn audio_params(codec: &str) -> CodecParameters {
    let mut p = CodecParameters::audio(CodecId::new(codec));
    p.sample_rate = Some(8_000);
    p.channels = Some(1);
    p.sample_format = Some(SampleFormat::S16);
    p
}

/// Packets the default 60 s stub demuxer emits (100 ms per packet).
const STUB_PACKETS: usize = 600;

// ─────────────────── completeness + serial parity ────────────────────

fn run_two_output_copy_job(threads: usize, tag: &str) -> (ExecutorStats, usize, usize) {
    let src_a = common::stub::touch(&format!("mop_parity_a_{tag}"));
    let src_b = common::stub::touch(&format!("mop_parity_b_{tag}"));
    let job = Job::from_json(&format!(
        r#"{{
            "outa": {{"audio": [{{"from": "{}"}}]}},
            "outb": {{"audio": [{{"from": "{}"}}]}}
        }}"#,
        src_a.display(),
        src_b.display(),
    ))
    .expect("parse job");
    let ctx = stub_ctx();
    let (sink_a, _, payloads_a) = CountingSink::boxed();
    let (sink_b, _, payloads_b) = CountingSink::boxed();
    let stats = Executor::new(&job, &ctx)
        .with_threads(threads)
        .with_sink_override("outa", sink_a)
        .with_sink_override("outb", sink_b)
        .run()
        .expect("run");
    (
        stats,
        payloads_a.load(Ordering::SeqCst),
        payloads_b.load(Ordering::SeqCst),
    )
}

#[test]
fn parallel_outputs_deliver_every_stream_and_match_serial_stats() {
    let (par_stats, par_a, par_b) = run_two_output_copy_job(4, "par");
    assert_eq!(par_a, STUB_PACKETS, "output A short-changed");
    assert_eq!(par_b, STUB_PACKETS, "output B short-changed");

    let (ser_stats, ser_a, ser_b) = run_two_output_copy_job(1, "ser");
    assert_eq!(ser_a, STUB_PACKETS);
    assert_eq!(ser_b, STUB_PACKETS);

    assert_eq!(par_stats.packets_read, ser_stats.packets_read);
    assert_eq!(par_stats.packets_copied, ser_stats.packets_copied);
    assert_eq!(par_stats.packets_encoded, ser_stats.packets_encoded);
    assert_eq!(par_stats.frames_decoded, ser_stats.frames_decoded);
    assert_eq!(par_stats.frames_written, ser_stats.frames_written);
    assert_eq!(par_stats.packets_skipped, ser_stats.packets_skipped);
    assert_eq!(par_stats.packets_read as usize, 2 * STUB_PACKETS);
}

// ───────────────────────── actual overlap ────────────────────────────

/// How many rendezvous sources have produced their first frame.
static OVL_STARTED: AtomicUsize = AtomicUsize::new(0);

/// Frame source whose FIRST `next_frame` blocks until a second
/// rendezvous source has also reached its first `next_frame`. Under a
/// sequential runner the first output would exhaust the 10 s deadline
/// and fail; under the parallel runner both sources start within the
/// same wave and release each other immediately.
struct RendezvousSource {
    params: CodecParameters,
    emitted: u64,
    rendezvoused: bool,
}

impl FrameSource for RendezvousSource {
    fn params(&self) -> &CodecParameters {
        &self.params
    }
    fn next_frame(&mut self) -> Result<Frame> {
        if !self.rendezvoused {
            self.rendezvoused = true;
            OVL_STARTED.fetch_add(1, Ordering::SeqCst);
            let deadline = Instant::now() + Duration::from_secs(10);
            while OVL_STARTED.load(Ordering::SeqCst) < 2 {
                if Instant::now() >= deadline {
                    return Err(Error::other(
                        "rendezvous timed out: outputs did not run concurrently",
                    ));
                }
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        if self.emitted >= 10 {
            return Err(Error::Eof);
        }
        let pts = (self.emitted * 16) as i64;
        self.emitted += 1;
        Ok(Frame::Audio(AudioFrame {
            samples: 16,
            pts: Some(pts),
            data: vec![vec![0u8; 32]],
        }))
    }
}

#[test]
fn outputs_of_one_wave_run_concurrently() {
    // Frame-shape sources carry no packets, so a plain output needs an
    // encoder to be representable — route through the stub encoder.
    let mut ctx = stub_ctx();
    ctx.sources.register_frames("mopovl", |_uri| {
        Ok(Box::new(RendezvousSource {
            params: audio_params("mop_pcm"),
            emitted: 0,
            rendezvoused: false,
        }))
    });
    let job = Job::from_json(&format!(
        r#"{{
            "outa": {{"audio": [{{"from": "mopovl://a", "codec": "{enc}"}}]}},
            "outb": {{"audio": [{{"from": "mopovl://b", "codec": "{enc}"}}]}}
        }}"#,
        enc = common::stub::ENC_CODEC_ID,
    ))
    .expect("parse job");
    let (sink_a, _, payloads_a) = CountingSink::boxed();
    let (sink_b, _, payloads_b) = CountingSink::boxed();
    Executor::new(&job, &ctx)
        .with_threads(4)
        .with_sink_override("outa", sink_a)
        .with_sink_override("outb", sink_b)
        .run()
        .expect("parallel run must let both sources rendezvous");
    assert_eq!(payloads_a.load(Ordering::SeqCst), 10);
    assert_eq!(payloads_b.load(Ordering::SeqCst), 10);
}

// ───────────────── wave width respects the budget ────────────────────

static GAUGE_ACTIVE: AtomicUsize = AtomicUsize::new(0);
static GAUGE_PEAK: AtomicUsize = AtomicUsize::new(0);
static GAUGE_STARTS: AtomicUsize = AtomicUsize::new(0);

/// Frame source that tracks how many of its kind are pumping
/// simultaneously (first `next_frame` → EOF).
struct GaugeSource {
    params: CodecParameters,
    emitted: u64,
    entered: bool,
    finished: bool,
}

impl FrameSource for GaugeSource {
    fn params(&self) -> &CodecParameters {
        &self.params
    }
    fn next_frame(&mut self) -> Result<Frame> {
        if !self.entered {
            self.entered = true;
            GAUGE_STARTS.fetch_add(1, Ordering::SeqCst);
            let now = GAUGE_ACTIVE.fetch_add(1, Ordering::SeqCst) + 1;
            GAUGE_PEAK.fetch_max(now, Ordering::SeqCst);
        }
        if self.emitted >= 20 {
            if !self.finished {
                self.finished = true;
                GAUGE_ACTIVE.fetch_sub(1, Ordering::SeqCst);
            }
            return Err(Error::Eof);
        }
        // Slow the pump slightly so wave-mates overlap in practice.
        std::thread::sleep(Duration::from_millis(1));
        let pts = (self.emitted * 16) as i64;
        self.emitted += 1;
        Ok(Frame::Audio(AudioFrame {
            samples: 16,
            pts: Some(pts),
            data: vec![vec![0u8; 32]],
        }))
    }
}

#[test]
fn wave_width_never_exceeds_thread_budget() {
    let mut ctx = stub_ctx();
    ctx.sources.register_frames("mopgauge", |_uri| {
        Ok(Box::new(GaugeSource {
            params: audio_params("mop_pcm"),
            emitted: 0,
            entered: false,
            finished: false,
        }))
    });
    let job = Job::from_json(&format!(
        r#"{{
            "o0": {{"audio": [{{"from": "mopgauge://0", "codec": "{enc}"}}]}},
            "o1": {{"audio": [{{"from": "mopgauge://1", "codec": "{enc}"}}]}},
            "o2": {{"audio": [{{"from": "mopgauge://2", "codec": "{enc}"}}]}},
            "o3": {{"audio": [{{"from": "mopgauge://3", "codec": "{enc}"}}]}}
        }}"#,
        enc = common::stub::ENC_CODEC_ID,
    ))
    .expect("parse job");
    let mut exec = Executor::new(&job, &ctx).with_threads(2);
    let mut counters = Vec::new();
    for name in ["o0", "o1", "o2", "o3"] {
        let (sink, _, payloads) = CountingSink::boxed();
        exec = exec.with_sink_override(name, sink);
        counters.push(payloads);
    }
    exec.run().expect("run");
    assert_eq!(
        GAUGE_STARTS.load(Ordering::SeqCst),
        4,
        "an output never ran"
    );
    let peak = GAUGE_PEAK.load(Ordering::SeqCst);
    assert!(
        (1..=2).contains(&peak),
        "4 outputs on a 2-thread budget must pump at most 2 at a time, saw {peak}"
    );
    for (i, payloads) in counters.iter().enumerate() {
        assert_eq!(
            payloads.load(Ordering::SeqCst),
            20,
            "output o{i} incomplete"
        );
    }
}

// ───────────────────────── error precedence ──────────────────────────

const ERR_FIRST: &str = "first-document-order output deliberately failed";
const ERR_SECOND: &str = "second-document-order output deliberately failed";

/// Two-output job where `fail` names the outputs (in document order)
/// that get a failing sink; the others count. Returns the run error
/// plus each healthy output's payload count.
fn run_precedence_job(
    order: [&'static str; 2],
    fail: &[&'static str],
    tag: &str,
) -> (Error, Vec<(String, usize)>) {
    let ctx = stub_ctx();
    let mut json = String::from("{");
    for (i, name) in order.iter().enumerate() {
        let src = common::stub::touch(&format!("mop_prec_{tag}_{name}"));
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            r#""{name}": {{"audio": [{{"from": "{}"}}]}}"#,
            src.display()
        ));
    }
    json.push('}');
    let job = Job::from_json(&json).expect("parse job");
    let mut exec = Executor::new(&job, &ctx).with_threads(4);
    let mut healthy = Vec::new();
    for (i, name) in order.iter().enumerate() {
        if fail.contains(name) {
            let message = if i == 0 { ERR_FIRST } else { ERR_SECOND };
            exec = exec.with_sink_override(
                name,
                Box::new(FailingSink {
                    ok_payloads: 3,
                    seen: Arc::new(AtomicUsize::new(0)),
                    message,
                }),
            );
        } else {
            let (sink, _, payloads) = CountingSink::boxed();
            exec = exec.with_sink_override(name, sink);
            healthy.push((name.to_string(), payloads));
        }
    }
    let err = exec.run().expect_err("a failing sink must surface as Err");
    let counts = healthy
        .into_iter()
        .map(|(n, p)| (n, p.load(Ordering::SeqCst)))
        .collect();
    (err, counts)
}

#[test]
fn failing_output_errors_while_wave_mate_delivers_fully() {
    // Failure in the FIRST output: its error surfaces, and the healthy
    // second output still delivers its complete stream (wave-mates run
    // to completion; nothing is retroactively discarded).
    let (err, counts) = run_precedence_job(["bad", "good"], &["bad"], "firstfails");
    let msg = format!("{err}");
    assert!(msg.contains(ERR_FIRST), "root cause lost, got: {msg}");
    assert_eq!(counts, vec![("good".to_string(), STUB_PACKETS)]);

    // Failure in the SECOND output: same contract, mirrored.
    let (err, counts) = run_precedence_job(["good", "bad"], &["bad"], "secondfails");
    let msg = format!("{err}");
    assert!(msg.contains(ERR_SECOND), "root cause lost, got: {msg}");
    assert_eq!(counts, vec![("good".to_string(), STUB_PACKETS)]);
}

#[test]
fn earliest_document_order_error_wins_when_several_outputs_fail() {
    // Both outputs fail with distinct messages. The surfaced error must
    // be the FIRST output's in document order — deterministically,
    // regardless of which thread hit its sink error first.
    let (err, counts) = run_precedence_job(["bad1", "bad2"], &["bad1", "bad2"], "bothfail");
    let msg = format!("{err}");
    assert!(
        msg.contains(ERR_FIRST) && !msg.contains(ERR_SECOND),
        "error precedence must follow document order, got: {msg}"
    );
    assert!(counts.is_empty());
}

#[test]
fn waves_after_a_failure_never_start() {
    // 4 outputs on a 2-thread budget → waves of 2 in document order:
    // [bad, g1] then [g2, g3]. The failure in wave 1 must (a) let g1
    // finish, and (b) prevent wave 2's outputs from ever starting —
    // their sinks are never opened, matching the sequential contract
    // that outputs after a failure don't run.
    let ctx = stub_ctx();
    let mut json = String::from("{");
    for (i, name) in ["bad", "g1", "g2", "g3"].iter().enumerate() {
        let src = common::stub::touch(&format!("mop_wavestop_{name}"));
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            r#""{name}": {{"audio": [{{"from": "{}"}}]}}"#,
            src.display()
        ));
    }
    json.push('}');
    let job = Job::from_json(&json).expect("parse job");
    let mut exec = Executor::new(&job, &ctx).with_threads(2);
    exec = exec.with_sink_override(
        "bad",
        Box::new(FailingSink {
            ok_payloads: 3,
            seen: Arc::new(AtomicUsize::new(0)),
            message: ERR_FIRST,
        }),
    );
    let mut sinks = Vec::new();
    for name in ["g1", "g2", "g3"] {
        let (sink, started, payloads) = CountingSink::boxed();
        exec = exec.with_sink_override(name, sink);
        sinks.push((name, started, payloads));
    }
    let err = exec.run().expect_err("wave-1 failure must surface");
    let msg = format!("{err}");
    assert!(msg.contains(ERR_FIRST), "root cause lost, got: {msg}");
    // Wave-mate g1 completed.
    assert!(sinks[0].1.load(Ordering::SeqCst), "g1 should have started");
    assert_eq!(sinks[0].2.load(Ordering::SeqCst), STUB_PACKETS);
    // Wave-2 outputs never started.
    for (name, started, payloads) in &sinks[1..] {
        assert!(
            !started.load(Ordering::SeqCst),
            "{name} started although its wave follows the failure"
        );
        assert_eq!(payloads.load(Ordering::SeqCst), 0, "{name} received data");
    }
}
