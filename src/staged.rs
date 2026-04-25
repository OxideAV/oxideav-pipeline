//! Pipelined (stage-per-thread) executor.
//!
//! Called by [`Executor::run`] when the thread budget is `≥ 2`. Spawns
//! one worker thread per pipeline stage per track, connected by bounded
//! `mpsc::sync_channel`s, and drives the mux/sink loop on the caller's
//! thread. Sinks therefore don't need to be `Send`.
//!
//! Data flow per output:
//!
//! ```text
//!   [one dmx thread per URI] ──► per-track packet channel ─┐
//!                                                           ├─► decode ─► filter… ─► encode ─► output channel
//!                                                           ┴─► (copy mode: output channel directly)
//!
//!   main thread (mux loop): recv across all output channels → sink.write_packet
//! ```
//!
//! End-of-stream is signalled with [`Msg::Eof`] rather than by dropping
//! the sender, so downstream stages can reliably flush their internal
//! buffers before exiting. Errors in any stage are funnelled through
//! [`AbortState`]; the first error wins, other stages bail cleanly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use oxideav_core::Demuxer;
use oxideav_core::{Decoder, Encoder};
use oxideav_core::{Error, Frame, MediaType, Packet, Result, StreamInfo, TimeBase};

use crate::executor::{
    drain_decoder, flush_frame_stage_emit, run_frame_stage_emit, ExecutorStats, FrameStage,
    JobSink, TrackRuntime,
};

/// Flow-barrier kind in [`Msg::Barrier`]. Today there is exactly one
/// variant — `SeekFlush` — broadcast by the demuxer stage when it
/// receives a [`SeekCmd`] from the [`crate::ExecutorHandle`]. Adding
/// new kinds is non-breaking: every worker treats unknown kinds as
/// "forward unchanged".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BarrierKind {
    /// Seek-induced flush. Workers reset codec / filter state, then
    /// forward this barrier downstream so the sink can drop any in-
    /// flight frames buffered above it.
    ///
    /// `generation` is incremented by the demuxer on every seek so the
    /// engine can correlate `seek()` calls with their corresponding
    /// barrier emission and ignore pre-seek payload still in flight.
    SeekFlush { generation: u32 },
}

/// Command sent to the demuxer stage by [`crate::ExecutorHandle::seek`].
/// The demuxer increments its local generation, broadcasts a
/// `SeekFlush` barrier on every route, then calls `demuxer.seek_to`.
#[derive(Clone, Copy, Debug)]
pub struct SeekCmd {
    pub stream_idx: u32,
    pub pts: i64,
    pub time_base: TimeBase,
}

/// Per-frame progress event consumed by [`crate::ExecutorHandle::try_progress`].
/// Updated by the mux loop on every `Msg::Data` carrying frame/packet pts;
/// the engine polls this once per tick for the status bar.
#[derive(Clone, Copy, Debug, Default)]
pub struct Progress {
    pub pts: Option<i64>,
    pub frames: u64,
    pub eof: bool,
}

/// Packet-channel depth. Small enough that a stalled consumer back-pressures
/// the demuxer before memory blows up; large enough to amortise the mutex
/// cost on each send.
const PACKET_CAP: usize = 16;

/// Frame-channel depth. Smaller than `PACKET_CAP` because decoded frames
/// are much larger than compressed packets.
const FRAME_CAP: usize = 8;

/// Messages across channels.
///
/// * `Data` — payload (packet/frame).
/// * `Barrier` — flow-control marker. Today only `SeekFlush` is in use;
///   workers reset codec/filter state and forward unchanged.
/// * `Eof` — in-band end-of-stream so downstream stages can flush state
///   before exiting.
enum Msg<T> {
    Data(T),
    Barrier(BarrierKind),
    Eof,
}

/// Shared counters. Each worker increments its relevant field; the mux
/// thread reads them out at the end into [`ExecutorStats`].
#[derive(Default)]
struct PipelineCounters {
    packets_read: AtomicU64,
    packets_copied: AtomicU64,
    packets_encoded: AtomicU64,
    frames_decoded: AtomicU64,
    frames_written: AtomicU64,
}

impl PipelineCounters {
    fn snapshot(&self) -> ExecutorStats {
        ExecutorStats {
            packets_read: self.packets_read.load(Ordering::SeqCst),
            packets_copied: self.packets_copied.load(Ordering::SeqCst),
            packets_encoded: self.packets_encoded.load(Ordering::SeqCst),
            frames_decoded: self.frames_decoded.load(Ordering::SeqCst),
            frames_written: self.frames_written.load(Ordering::SeqCst),
        }
    }
}

/// Shared state used to coordinate clean shutdown across all worker
/// threads in one output's pipeline. Held inside an `Arc` so each
/// worker can poll the flag and so [`crate::ExecutorHandle`] can
/// flip it from the outside.
pub(crate) struct AbortState {
    /// Set by any worker that errors out (or by the mux thread at EOF).
    /// Workers poll it between iterations and bail cleanly.
    pub(crate) abort: AtomicBool,
    /// First `Err(_)` seen. Later errors are dropped so the caller
    /// gets the root cause rather than a cascading symptom.
    first_err: Mutex<Option<Error>>,
}

impl AbortState {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            abort: AtomicBool::new(false),
            first_err: Mutex::new(None),
        })
    }

    pub(crate) fn is_aborted(&self) -> bool {
        self.abort.load(Ordering::SeqCst)
    }

    pub(crate) fn request_abort(&self) {
        self.abort.store(true, Ordering::SeqCst);
    }

    fn record_error(&self, e: Error) {
        let mut slot = self.first_err.lock().unwrap();
        if slot.is_none() {
            *slot = Some(e);
        }
        self.abort.store(true, Ordering::SeqCst);
    }

    fn take_error(&self) -> Option<Error> {
        self.first_err.lock().unwrap().take()
    }
}

/// One per-track output channel item — retains the track index so the
/// mux thread can tag packets with the right stream index.
struct OutputItem {
    track_index: u32,
    kind: MediaType,
    payload: OutputPayload,
}

enum OutputPayload {
    Packet(Packet),
    Frame(Frame),
}

/// Optional control bundle for [`run_pipelined`]. `seek_rx` is consumed
/// by the (single) demuxer thread that picks it up; `progress_tx` is
/// updated by the mux loop on every data/barrier event.
///
/// Both fields are independent — a caller can wire only one if needed.
/// Used today by [`crate::Executor::spawn`]; the synchronous
/// [`crate::Executor::run`] passes `None` and gets the legacy behaviour.
pub(crate) struct PipelineControl {
    pub seek_rx: Option<Receiver<SeekCmd>>,
    pub progress_tx: Option<SyncSender<Progress>>,
    pub abort: Option<Arc<AbortState>>,
}

/// Run one output's pipeline. The caller has already instantiated all
/// decoders/filters/encoders via `TrackRuntime::instantiate`, opened the
/// demuxers, and prepared the sink (but not called `start` on it).
pub(crate) fn run_pipelined(
    pipelines: Vec<TrackRuntime>,
    dmx_by_uri: HashMap<String, Box<dyn Demuxer>>,
    sink: Box<dyn JobSink + Send>,
    out_streams: Vec<StreamInfo>,
) -> Result<ExecutorStats> {
    run_pipelined_inner(
        pipelines,
        dmx_by_uri,
        sink,
        out_streams,
        PipelineControl {
            seek_rx: None,
            progress_tx: None,
            abort: None,
        },
    )
}

/// Like [`run_pipelined`] but with explicit control wiring — used by
/// [`crate::Executor::spawn`] to plumb the seek + progress + abort
/// channels through to the demuxer / mux loop.
pub(crate) fn run_pipelined_with_control(
    pipelines: Vec<TrackRuntime>,
    dmx_by_uri: HashMap<String, Box<dyn Demuxer>>,
    sink: Box<dyn JobSink + Send>,
    out_streams: Vec<StreamInfo>,
    control: PipelineControl,
) -> Result<ExecutorStats> {
    run_pipelined_inner(pipelines, dmx_by_uri, sink, out_streams, control)
}

pub(crate) fn run_pipelined_inner(
    mut pipelines: Vec<TrackRuntime>,
    dmx_by_uri: HashMap<String, Box<dyn Demuxer>>,
    mut sink: Box<dyn JobSink + Send>,
    out_streams: Vec<StreamInfo>,
    control: PipelineControl,
) -> Result<ExecutorStats> {
    sink.start(&out_streams)?;

    // External abort takes precedence so callers (e.g. `ExecutorHandle`)
    // can pre-arm cancellation before the workers spawn.
    let abort = control.abort.unwrap_or_else(AbortState::new);
    let counters = Arc::new(PipelineCounters::default());
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    let progress_tx = control.progress_tx;
    let mut seek_rx = control.seek_rx;

    // Per-track output channel: stage workers send processed packets /
    // frames on tx; the mux loop on the caller thread reads rx.
    let mut track_output_rx: Vec<Receiver<Msg<OutputItem>>> = Vec::new();
    let mut track_output_tx: Vec<SyncSender<Msg<OutputItem>>> = Vec::new();
    for _ in 0..pipelines.len() {
        let (tx, rx) = mpsc::sync_channel::<Msg<OutputItem>>(PACKET_CAP);
        track_output_tx.push(tx);
        track_output_rx.push(rx);
    }

    // Route table: per source URI, the list of (source_stream, packet_tx)
    // pairs the demuxer thread fans packets out to.
    type Route = (u32, SyncSender<Msg<Packet>>);
    let mut routes_by_uri: HashMap<String, Vec<Route>> = HashMap::new();

    // Build + spawn each track's stage chain. We consume the Vec so the
    // decoder/encoder/filters can be moved into worker threads.
    for (track_idx, mut pl) in pipelines.drain(..).enumerate() {
        let out_tx = track_output_tx[track_idx].clone();
        let kind = pl.kind;
        let source_uri = pl.source_uri.clone();
        let source_stream = pl.source_stream;

        // Every track has a packet-input channel from the demuxer
        // regardless of copy / transcode — the demuxer thread doesn't
        // need to know which mode each consumer uses.
        let (pkt_tx, pkt_rx) = mpsc::sync_channel::<Msg<Packet>>(PACKET_CAP);
        routes_by_uri
            .entry(source_uri)
            .or_default()
            .push((source_stream, pkt_tx));

        if pl.copy {
            let abort_c = abort.clone();
            let counters_c = counters.clone();
            let name = format!("copy-{track_idx}");
            handles.push(spawn_stage(abort_c, name, move |abort| {
                run_copy_stage(pkt_rx, out_tx, track_idx as u32, kind, abort, counters_c)
            }));
            continue;
        }

        // Transcode: decoder → frame stages → encoder-or-fanout.
        // Each FrameStage runs on its own worker thread so audio
        // filters, pixel-format converts, and future video filters
        // can overlap the encoder's back-pressure.
        let decoder = pl.decoder.take().ok_or_else(|| {
            Error::other("pipeline: non-copy track without a decoder is not supported")
        })?;
        let frame_stages = std::mem::take(&mut pl.frame_stages);
        let encoder = pl.encoder.take();

        let (frame0_tx, frame0_rx) = mpsc::sync_channel::<Msg<Frame>>(FRAME_CAP);
        {
            let abort_d = abort.clone();
            let counters_d = counters.clone();
            let name = format!("decode-{track_idx}");
            handles.push(spawn_stage(abort_d, name, move |abort| {
                run_decode_stage(decoder, pkt_rx, frame0_tx, abort, counters_d)
            }));
        }

        // Count extras as we go: the first filter stage on this track
        // starts at the track's `extras_base_for_this_track`, the next
        // filter picks up where the previous left off. Non-filter
        // stages (PixConvert) never emit extras but still advance the
        // index so downstream sinks remain consistent.
        let extras_base_for_track: u32 = pl.extras_base_index;
        let mut running_extras_base = extras_base_for_track;
        let extra_port_counts: Vec<u32> = pl.extra_output_port_counts.clone().into_iter().collect();
        let mut extra_counts_iter = extra_port_counts.into_iter();

        let mut upstream: Receiver<Msg<Frame>> = frame0_rx;
        for (fidx, stage) in frame_stages.into_iter().enumerate() {
            let (ftx, frx) = mpsc::sync_channel::<Msg<Frame>>(FRAME_CAP);
            let label = match &stage {
                FrameStage::Filter(_) => "filter",
                FrameStage::PixConvert(_) => "convert",
            };
            let name = format!("{label}-{track_idx}-{fidx}");
            let abort_f = abort.clone();

            // Wire an extras channel only for Filter stages that
            // declared extra output ports.
            let (stage_extras_tx, stage_extras_base) = if matches!(stage, FrameStage::Filter(_)) {
                match extra_counts_iter.next() {
                    Some(n) if n > 0 => {
                        let base = running_extras_base;
                        running_extras_base += n;
                        (Some(out_tx.clone()), base)
                    }
                    _ => (None, 0),
                }
            } else {
                (None, 0)
            };

            handles.push(spawn_stage(abort_f, name, move |abort| {
                run_frame_stage_worker(
                    stage,
                    upstream,
                    ftx,
                    stage_extras_tx,
                    stage_extras_base,
                    abort,
                )
            }));
            upstream = frx;
        }

        if let Some(enc) = encoder {
            let abort_e = abort.clone();
            let counters_e = counters.clone();
            let out_tx = out_tx.clone();
            let name = format!("encode-{track_idx}");
            handles.push(spawn_stage(abort_e, name, move |abort| {
                run_encode_stage(
                    enc,
                    upstream,
                    out_tx,
                    track_idx as u32,
                    kind,
                    abort,
                    counters_e,
                )
            }));
        } else {
            // No encoder — raw frames flow into the mux (player scenario).
            let abort_r = abort.clone();
            let out_tx = out_tx.clone();
            let name = format!("frame-fanout-{track_idx}");
            handles.push(spawn_stage(abort_r, name, move |abort| {
                run_frame_fanout(upstream, out_tx, track_idx as u32, kind, abort)
            }));
        }
    }

    // Drop the master copies of the output channels; only workers hold
    // senders now so `recv_timeout` sees RecvTimeoutError::Disconnected
    // when every stage has finished.
    drop(track_output_tx);

    // Spawn one demuxer thread per URI. The seek_rx (if any) is given
    // to the FIRST demuxer that has routes — multi-URI seek is a
    // follow-up (the engine only ever drives one source today, so a
    // single seek receiver is enough to cover all of plain playback).
    for (uri, dmx) in dmx_by_uri {
        let routes = routes_by_uri.remove(&uri).unwrap_or_default();
        if routes.is_empty() {
            continue;
        }
        let abort_d = abort.clone();
        let counters_d = counters.clone();
        let name = format!("demux-{uri}");
        let dmx_seek_rx = seek_rx.take();
        handles.push(spawn_stage(abort_d, name, move |abort| {
            run_demuxer_stage(dmx, routes, abort, counters_d, dmx_seek_rx)
        }));
    }

    // Mux loop on the caller thread — recv across every track output
    // channel until all are EOF or abort is set.
    let mut eof_count = 0usize;
    let total = track_output_rx.len();
    let mut i = 0usize;
    while eof_count < total {
        if abort.is_aborted() {
            break;
        }
        let rx = &track_output_rx[i];
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(Msg::Data(item)) => {
                let pts = match &item.payload {
                    OutputPayload::Packet(p) => p.pts,
                    OutputPayload::Frame(f) => match f {
                        Frame::Audio(a) => a.pts,
                        Frame::Video(v) => v.pts,
                        _ => None,
                    },
                };
                match item.payload {
                    OutputPayload::Packet(mut p) => {
                        p.stream_index = item.track_index;
                        if let Err(e) = sink.write_packet(item.kind, &p) {
                            abort.record_error(e);
                            break;
                        }
                    }
                    OutputPayload::Frame(f) => {
                        if let Err(e) = sink.write_frame(item.kind, &f) {
                            abort.record_error(e);
                            break;
                        }
                        counters.frames_written.fetch_add(1, Ordering::SeqCst);
                    }
                }
                if let Some(tx) = &progress_tx {
                    let frames = counters.frames_written.load(Ordering::SeqCst);
                    let _ = tx.try_send(Progress {
                        pts,
                        frames,
                        eof: false,
                    });
                }
            }
            Ok(Msg::Barrier(kind)) => {
                if let Err(e) = sink.barrier(kind) {
                    abort.record_error(e);
                    break;
                }
            }
            Ok(Msg::Eof) => {
                eof_count += 1;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // Producer panicked or exited without sending Eof —
                // count as EOF to avoid hanging. Any error was already
                // recorded on the abort state.
                eof_count += 1;
            }
        }
        i = (i + 1) % total;
    }

    // Drain abort flag + wait for workers regardless of exit path.
    abort.abort.store(true, Ordering::SeqCst);
    // Drop the mux-end receivers BEFORE joining workers. Upstream
    // stages (copy / decode / filter / pix-convert / demux) may be
    // blocked inside `SyncSender::send()` because the bounded
    // channel is full — setting the abort flag alone doesn't wake
    // them. Dropping the receivers turns every pending send into an
    // `Err(SendError)`, the worker's `tx.send().is_err()` branch
    // breaks its loop, and the cascade propagates up to the demuxer.
    // Without this, `h.join()` below deadlocks on any abort-path
    // exit (quit event, sink error, encoder fail).
    drop(track_output_rx);
    for h in handles {
        let _ = h.join();
    }
    if let Some(err) = abort.take_error() {
        return Err(err);
    }
    sink.finish()?;
    if let Some(tx) = &progress_tx {
        let frames = counters.frames_written.load(Ordering::SeqCst);
        let _ = tx.try_send(Progress {
            pts: None,
            frames,
            eof: true,
        });
    }
    Ok(counters.snapshot())
}

/// Spawn a worker thread that runs `work` under `abort`. If `work`
/// returns `Err`, record it on `abort` (first-wins) and flip the abort
/// flag so peers can bail.
fn spawn_stage<F>(abort: Arc<AbortState>, name: String, work: F) -> JoinHandle<()>
where
    F: FnOnce(Arc<AbortState>) -> Result<()> + Send + 'static,
{
    thread::Builder::new()
        .name(format!("oxideav-job:{name}"))
        .spawn(move || {
            if let Err(e) = work(abort.clone()) {
                abort.record_error(e);
            }
        })
        .expect("pipeline: thread spawn")
}

// ───────────────────────── stage workers ─────────────────────────

/// Demuxer thread: read packets until EOF, fan out to each route whose
/// source_stream matches. Broadcasts `Msg::Eof` to every route on EOF.
///
/// Optional `seek_rx` carries [`SeekCmd`]s from
/// [`crate::ExecutorHandle::seek`]. On each iteration we
/// non-blocking-poll the channel; on a SeekCmd we bump `generation`,
/// fan a `Msg::Barrier(SeekFlush)` out on every route, then call
/// `dmx.seek_to`. The barrier flows downstream through every worker
/// (which resets its codec/filter state) and lands on the mux loop,
/// which calls `sink.barrier(kind)`.
fn run_demuxer_stage(
    mut dmx: Box<dyn Demuxer>,
    routes: Vec<(u32, SyncSender<Msg<Packet>>)>,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
    seek_rx: Option<Receiver<SeekCmd>>,
) -> Result<()> {
    let mut generation: u32 = 0;
    loop {
        if abort.is_aborted() {
            break;
        }
        // Drain any pending seeks before reading the next packet.
        if let Some(rx) = &seek_rx {
            while let Ok(cmd) = rx.try_recv() {
                generation = generation.wrapping_add(1);
                let kind = BarrierKind::SeekFlush { generation };
                for (_, tx) in &routes {
                    if tx.send(Msg::Barrier(kind)).is_err() {
                        abort.abort.store(true, Ordering::SeqCst);
                        return Ok(());
                    }
                }
                match dmx.seek_to(cmd.stream_idx, cmd.pts) {
                    Ok(_landed) => {}
                    Err(e) => return Err(e),
                }
            }
        }
        match dmx.next_packet() {
            Ok(pkt) => {
                counters.packets_read.fetch_add(1, Ordering::SeqCst);
                for (stream_idx, tx) in &routes {
                    if *stream_idx == pkt.stream_index && tx.send(Msg::Data(pkt.clone())).is_err() {
                        // Consumer gone; likely aborted.
                        abort.abort.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            }
            Err(Error::Eof) => break,
            Err(e) => return Err(e),
        }
    }
    for (_, tx) in routes {
        let _ = tx.send(Msg::Eof);
    }
    Ok(())
}

/// Copy track: packets straight to the output channel.
fn run_copy_stage(
    rx: Receiver<Msg<Packet>>,
    out_tx: SyncSender<Msg<OutputItem>>,
    track_index: u32,
    kind: MediaType,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(pkt)) => {
                if out_tx
                    .send(Msg::Data(OutputItem {
                        track_index,
                        kind,
                        payload: OutputPayload::Packet(pkt),
                    }))
                    .is_err()
                {
                    break;
                }
                counters.packets_copied.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Msg::Barrier(b)) => {
                // Copy stages have no internal state — just forward.
                if out_tx.send(Msg::Barrier(b)).is_err() {
                    break;
                }
            }
            Ok(Msg::Eof) | Err(_) => break,
        }
    }
    let _ = out_tx.send(Msg::Eof);
    Ok(())
}

/// Decoder stage: packets -> frames.
fn run_decode_stage(
    mut decoder: Box<dyn Decoder>,
    rx: Receiver<Msg<Packet>>,
    tx: SyncSender<Msg<Frame>>,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
) -> Result<()> {
    let mut scratch = ExecutorStats::default();
    loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(pkt)) => {
                decoder.send_packet(&pkt)?;
                let frames = drain_decoder(decoder.as_mut(), &mut scratch)?;
                counters
                    .frames_decoded
                    .fetch_add(frames.len() as u64, Ordering::SeqCst);
                for f in frames {
                    if tx.send(Msg::Data(f)).is_err() {
                        abort.abort.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            }
            Ok(Msg::Barrier(b)) => {
                // SeekFlush: drop any in-flight buffered frames + reset
                // codec state so reference frames from the pre-seek
                // segment can't leak into the post-seek output.
                let _ = decoder.reset();
                if tx.send(Msg::Barrier(b)).is_err() {
                    break;
                }
            }
            Ok(Msg::Eof) => {
                decoder.flush()?;
                let frames = drain_decoder(decoder.as_mut(), &mut scratch)?;
                counters
                    .frames_decoded
                    .fetch_add(frames.len() as u64, Ordering::SeqCst);
                for f in frames {
                    let _ = tx.send(Msg::Data(f));
                }
                break;
            }
            Err(_) => break,
        }
    }
    let _ = tx.send(Msg::Eof);
    Ok(())
}

/// Frame-stage worker: consumes frames, runs them through an audio
/// filter or pixel-format conversion, and forwards to the next stage.
/// Used for both `FrameStage::Filter` and `FrameStage::PixConvert`.
///
/// If the stage is a multi-port filter, per-extra-port frames are sent
/// straight to the output channel tagged with the extra stream's
/// global index (starting at `extras_base`) — they bypass the rest of
/// the frame-stage chain and land on the sink directly.
fn run_frame_stage_worker(
    mut stage: FrameStage,
    rx: Receiver<Msg<Frame>>,
    tx: SyncSender<Msg<Frame>>,
    extras_tx: Option<SyncSender<Msg<OutputItem>>>,
    extras_base: u32,
    abort: Arc<AbortState>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(frame)) => {
                let emissions = run_frame_stage_emit(&mut stage, frame)?;
                dispatch_extras(&emissions, &extras_tx, extras_base, &abort);
                for o in emissions.primary {
                    if tx.send(Msg::Data(o)).is_err() {
                        abort.abort.store(true, Ordering::SeqCst);
                        break;
                    }
                }
            }
            Ok(Msg::Barrier(b)) => {
                // Filter stages may hold rolling-window state (spectrogram
                // columns, resampler tail) — drop it. Pixel-format
                // converts are stateless so they no-op. The barrier also
                // flows to the extras channel so a multi-port filter's
                // sink (e.g. spectrogram's video output) gets a chance
                // to drop in-flight extras.
                reset_frame_stage(&mut stage);
                if let Some(etx) = &extras_tx {
                    let _ = etx.send(Msg::Barrier(b));
                }
                if tx.send(Msg::Barrier(b)).is_err() {
                    break;
                }
            }
            Ok(Msg::Eof) => {
                let emissions = flush_frame_stage_emit(&mut stage)?;
                dispatch_extras(&emissions, &extras_tx, extras_base, &abort);
                for o in emissions.primary {
                    let _ = tx.send(Msg::Data(o));
                }
                break;
            }
            Err(_) => break,
        }
    }
    let _ = tx.send(Msg::Eof);
    Ok(())
}

/// Drop internal state of a [`FrameStage`] on a `SeekFlush` barrier.
/// Filters delegate to [`oxideav_core::StreamFilter::reset`] (default no-op);
/// pixel-format converts hold no state.
fn reset_frame_stage(stage: &mut FrameStage) {
    match stage {
        FrameStage::Filter(f) => {
            let _ = f.inner.reset();
        }
        FrameStage::PixConvert(_) => {}
    }
}

/// Push `emissions.extras` onto the sink's output channel (if present).
/// Extras are tagged with indices starting at `extras_base`; port 1
/// becomes `extras_base`, port 2 `extras_base + 1`, etc.
fn dispatch_extras(
    emissions: &crate::executor::FilterEmissions,
    extras_tx: &Option<SyncSender<Msg<OutputItem>>>,
    extras_base: u32,
    abort: &Arc<AbortState>,
) {
    let Some(tx) = extras_tx else {
        return;
    };
    // The extras vec carries entries in port-1,2,3,… order as emitted
    // by the filter, but a single `push` may emit multiple frames per
    // port. We can't recover the port number from the (kind, frame)
    // tuple alone, so we tag every extra with `extras_base` + its
    // media-kind slot. For the single-extra-port case (spectrogram)
    // this is equivalent to `extras_base`.
    for (kind, frm) in &emissions.extras {
        let item = OutputItem {
            track_index: extras_base,
            kind: *kind,
            payload: OutputPayload::Frame(frm.clone()),
        };
        if tx.send(Msg::Data(item)).is_err() {
            abort.abort.store(true, Ordering::SeqCst);
            return;
        }
    }
}

/// Encoder stage: frames -> packets -> OutputItem.
fn run_encode_stage(
    mut encoder: Box<dyn Encoder>,
    rx: Receiver<Msg<Frame>>,
    out_tx: SyncSender<Msg<OutputItem>>,
    track_index: u32,
    kind: MediaType,
    abort: Arc<AbortState>,
    counters: Arc<PipelineCounters>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(frame)) => {
                encoder.send_frame(&frame)?;
                drain_and_send(encoder.as_mut(), &out_tx, track_index, kind, &counters)?;
            }
            Ok(Msg::Barrier(b)) => {
                // The encoder trait has no `reset()` today — flush
                // anything pending and forward the barrier. A future
                // extension can plumb codec-specific reset (e.g.
                // dropping the GOP) once needed.
                let _ = encoder.flush();
                drain_and_send(encoder.as_mut(), &out_tx, track_index, kind, &counters)?;
                if out_tx.send(Msg::Barrier(b)).is_err() {
                    break;
                }
            }
            Ok(Msg::Eof) => {
                encoder.flush()?;
                drain_and_send(encoder.as_mut(), &out_tx, track_index, kind, &counters)?;
                break;
            }
            Err(_) => break,
        }
    }
    let _ = out_tx.send(Msg::Eof);
    Ok(())
}

/// Frame fan-out (no encoder): just forwards raw frames to the mux /
/// sink. Used when the output sink is something like the SDL2 player.
fn run_frame_fanout(
    rx: Receiver<Msg<Frame>>,
    out_tx: SyncSender<Msg<OutputItem>>,
    track_index: u32,
    kind: MediaType,
    abort: Arc<AbortState>,
) -> Result<()> {
    loop {
        if abort.is_aborted() {
            break;
        }
        match rx.recv() {
            Ok(Msg::Data(f)) => {
                if out_tx
                    .send(Msg::Data(OutputItem {
                        track_index,
                        kind,
                        payload: OutputPayload::Frame(f),
                    }))
                    .is_err()
                {
                    break;
                }
            }
            Ok(Msg::Barrier(b)) => {
                if out_tx.send(Msg::Barrier(b)).is_err() {
                    break;
                }
            }
            Ok(Msg::Eof) | Err(_) => break,
        }
    }
    let _ = out_tx.send(Msg::Eof);
    Ok(())
}

fn drain_and_send(
    encoder: &mut dyn Encoder,
    out_tx: &SyncSender<Msg<OutputItem>>,
    track_index: u32,
    kind: MediaType,
    counters: &PipelineCounters,
) -> Result<()> {
    loop {
        match encoder.receive_packet() {
            Ok(p) => {
                if out_tx
                    .send(Msg::Data(OutputItem {
                        track_index,
                        kind,
                        payload: OutputPayload::Packet(p),
                    }))
                    .is_err()
                {
                    return Ok(()); // consumer gone; caller will see abort
                }
                counters.packets_encoded.fetch_add(1, Ordering::SeqCst);
            }
            Err(Error::NeedMore) | Err(Error::Eof) => return Ok(()),
            Err(e) => return Err(e),
        }
    }
}
