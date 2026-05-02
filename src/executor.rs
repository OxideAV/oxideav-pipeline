//! DAG executor. Two modes share the same DAG-resolution + track-runtime
//! building code:
//!
//! - **Serial mode** (`threads == 1`): one packet pulled, one packet routed,
//!   one frame at a time. The original path — minimal, predictable, easy
//!   to reason about.
//! - **Pipelined mode** (`threads ≥ 2`, the default when
//!   `available_parallelism()` reports ≥ 2 cores): one thread per stage
//!   per track, bounded mpsc channels in between, mux on the caller
//!   thread so sinks don't need to be `Send`. See [`crate::pipeline`].
//!
//! Outputs within one job still run sequentially — multi-output
//! parallelism is a deliberate follow-up.

use std::collections::HashMap;
use std::path::PathBuf;

use oxideav_core::{
    CodecId, CodecParameters, CodecRegistry, Decoder, Demuxer, Encoder, Error, ExecutionContext,
    FilterContext, FilterRegistry, Frame, FrameSource, MediaType, Packet, PacketSource,
    PixelFormat, PortParams, PortSpec, Rational, ReadSeek, Result, RuntimeContext, SampleFormat,
    SourceOutput, StreamFilter, StreamInfo, TimeBase,
};
use oxideav_pixfmt::{convert as pixfmt_convert, ConvertOptions};

use crate::dag::{codec_accepted_pixel_formats, Dag, DagNode, MuxTrack, ResolvedSelector};
use crate::schema::{is_reserved_sink, Job};
use crate::sinks::{open_file_write, FileSink, NullSink};
use crate::staged;

/// One opened source, branched on the registered shape. Held in the
/// executor's per-URI map so the run loops can pump packets or frames
/// straight into the per-track stages without re-opening the source.
///
/// `Demuxer` is the historical shape (bytes → container probe → demuxer).
/// `Packets` skips the container layer (RTMP, RTSP, …). `Frames` skips
/// both demux + decode (synthetic generators).
pub(crate) enum SourcePump {
    Demuxer(Box<dyn Demuxer>),
    Packets(Box<dyn PacketSource>),
    Frames {
        source: Box<dyn FrameSource>,
        /// Synthesised single-stream descriptor — frame sources expose
        /// only `params()`, but the rest of the executor (selector
        /// matching, mux output stream layout) wants a `StreamInfo`.
        streams: Vec<StreamInfo>,
    },
}

impl SourcePump {
    pub(crate) fn streams(&self) -> &[StreamInfo] {
        match self {
            Self::Demuxer(d) => d.streams(),
            Self::Packets(p) => p.streams(),
            Self::Frames { streams, .. } => streams.as_slice(),
        }
    }
}

/// Build a one-element `StreamInfo` list for a [`FrameSource`]. The
/// selector logic in [`select_stream`] always picks index 0 for
/// frame-shape sources.
fn synth_stream_info(params: &CodecParameters) -> StreamInfo {
    // Time base: prefer the audio sample rate, else the video frame
    // rate's reciprocal, else microsecond ticks.
    let time_base = match params.media_type {
        MediaType::Audio => match params.sample_rate {
            Some(sr) if sr > 0 => TimeBase::new(1, sr as i64),
            _ => TimeBase::new(1, 1_000_000),
        },
        MediaType::Video => match params.frame_rate {
            Some(fr) if fr.num != 0 && fr.den != 0 => TimeBase::new(fr.den, fr.num),
            _ => TimeBase::new(1, 1_000_000),
        },
        _ => TimeBase::new(1, 1_000_000),
    };
    StreamInfo {
        index: 0,
        time_base,
        duration: None,
        start_time: Some(0),
        params: params.clone(),
    }
}

/// A user-installable output sink. Implementations receive either raw
/// packets (copy path) or decoded frames (transcode path without an
/// encoder node, e.g. live-play).
///
/// The trait itself does NOT require `Send` — the synchronous
/// [`Executor::run`] keeps the sink on the caller thread, and some
/// real-world sinks hold non-`Send` resources (e.g. a libloading
/// audio device). Sinks that want to participate in
/// [`Executor::spawn`] should additionally be `Send`; the spawn path
/// requires `Box<dyn JobSink + Send>` directly.
pub trait JobSink {
    /// Called once after all encoders are constructed and the output
    /// stream layout is known. Muxer-style sinks usually write the
    /// container header here.
    fn start(&mut self, streams: &[StreamInfo]) -> Result<()>;
    fn write_packet(&mut self, kind: MediaType, pkt: &Packet) -> Result<()>;
    fn write_frame(&mut self, kind: MediaType, frm: &Frame) -> Result<()>;
    /// Drain any remaining internal state and finalise the output.
    fn finish(&mut self) -> Result<()>;

    /// Flow-barrier hook. Called by the mux loop when a worker
    /// forwards a [`crate::BarrierKind`] (today only `SeekFlush`).
    /// Sinks that buffer frames (a player queueing audio /
    /// video frames separately) should drop their pre-barrier state
    /// here so post-barrier frames are presented at the new wall-clock
    /// position.
    ///
    /// Default: no-op — file/null sinks don't buffer anything past the
    /// muxer's own packet queue.
    fn barrier(&mut self, _kind: crate::BarrierKind) -> Result<()> {
        Ok(())
    }
}

/// Job runner. Constructed with a validated `Job` and a unified
/// [`RuntimeContext`] that bundles every registry the framework needs
/// (codec / container / source / filter); dispatches to serial or
/// pipelined execution based on the effective thread budget.
pub struct Executor<'a> {
    job: &'a Job,
    ctx: &'a RuntimeContext,
    sink_overrides: HashMap<String, Box<dyn JobSink + Send>>,
    /// Explicit thread budget from the caller. `None` = resolve from
    /// `job.threads` or autodetect. `Some(0)` is treated as auto as well.
    explicit_threads: Option<usize>,
}

impl<'a> Executor<'a> {
    /// Construct an executor over `job` using the registries bundled in
    /// `ctx`. Callers configure `ctx.filters` (and any optional codec
    /// or container registrations) before constructing the Executor —
    /// the executor itself doesn't mutate the context.
    pub fn new(job: &'a Job, ctx: &'a RuntimeContext) -> Self {
        Self {
            job,
            ctx,
            sink_overrides: HashMap::new(),
            explicit_threads: None,
        }
    }

    /// Replace the sink for a named output. Typically used to bind a
    /// live-playback sink to `@display`/`@out`.
    ///
    /// The sink is required to be `Send` so it can move onto the
    /// background mux thread used by [`Self::spawn`]. Sinks consumed
    /// only by the synchronous [`Self::run`] path can still satisfy
    /// this with a wrapper that owns its non-`Send` resources via
    /// `Send`-able indirection.
    pub fn with_sink_override(mut self, name: &str, sink: Box<dyn JobSink + Send>) -> Self {
        self.sink_overrides.insert(name.to_string(), sink);
        self
    }

    /// Override the thread budget. `0` means "auto" (use the value from the
    /// job's `threads` key, or fall back to `available_parallelism()`).
    /// `1` forces strictly serial execution; `≥ 2` requests pipelined.
    pub fn with_threads(mut self, n: usize) -> Self {
        self.explicit_threads = Some(n);
        self
    }

    /// Validate, resolve, and run the job. Processes outputs in their
    /// document order.
    pub fn run(mut self) -> Result<ExecutorStats> {
        self.job.validate()?;
        let dag = self.job.to_dag()?;
        let threads = self.resolve_threads();
        let mut stats = ExecutorStats::default();
        let names: Vec<String> = dag.roots.keys().cloned().collect();
        for name in names {
            let out_stats = if threads >= 2 {
                self.run_output_pipelined(&dag, &name, threads)?
            } else {
                self.run_output(&dag, &name)?
            };
            stats.merge(&out_stats);
        }
        Ok(stats)
    }

    /// Spawn the executor on a background thread and return a handle
    /// for live control (seek, abort, progress). Unlike [`Self::run`],
    /// this only supports a single output (the typical playback case
    /// — one `@display` / `@out`). For multi-output transcodes,
    /// continue to use [`Self::run`].
    ///
    /// All the demuxer-open / decoder-instantiate / sink-resolve work
    /// happens synchronously up front — the returned handle is live by
    /// the time `spawn` returns. The mux + worker threads run in the
    /// background until either the demuxer hits EOF, [`ExecutorHandle::stop`]
    /// is called, or any worker reports an error.
    pub fn spawn(mut self) -> Result<ExecutorHandle> {
        self.job.validate()?;
        let dag = self.job.to_dag()?;
        let threads = self.resolve_threads().max(2);
        let names: Vec<String> = dag.roots.keys().cloned().collect();
        let name = match names.as_slice() {
            [n] => n.clone(),
            [] => return Err(Error::invalid("job: no outputs to spawn")),
            _ => {
                return Err(Error::invalid(
                    "job: spawn() supports a single output today; use run() for multi-output jobs",
                ));
            }
        };
        let prep = self.prepare_pipelined_run(&dag, &name, threads)?;
        Ok(ExecutorHandle::start(prep))
    }

    /// Resolve the effective thread budget. Priority: explicit
    /// `with_threads(n > 0)` > `job.threads` > `available_parallelism()`
    /// > `1`.
    fn resolve_threads(&self) -> usize {
        if let Some(n) = self.explicit_threads {
            if n > 0 {
                return n;
            }
        }
        if let Some(n) = self.job.threads {
            if n > 0 {
                return n;
            }
        }
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
    }

    fn run_output(&mut self, dag: &Dag, name: &str) -> Result<ExecutorStats> {
        let mut dag = dag.clone();
        // Probe every Demuxer-shape leaf and rewrite to the typed
        // variant (PacketSource / FrameSource) when the registry hands
        // back a non-bytes opener. Returns the opened sources keyed by
        // URI so we don't re-open below.
        let mut sources_by_uri = self.resolve_source_shapes(&mut dag)?;
        let dag = &dag;
        let root_id = dag.roots[name];
        let tracks: Vec<MuxTrack> = match dag.node(root_id) {
            DagNode::Mux { tracks, .. } => tracks.clone(),
            other => {
                return Err(Error::invalid(format!(
                    "job: output {name}: expected Mux root, got {other:?}"
                )));
            }
        };

        // Walk each track's upstream chain to find the source leaf + the
        // stack of stages (select/decode/filter/encode) to apply.
        let mut pipelines: Vec<TrackRuntime> = Vec::new();
        for t in &tracks {
            pipelines.push(self.build_track_runtime(dag, t)?);
        }

        // Expand `all:` tracks (kind == Unknown, selector == any) into
        // one TrackRuntime per source stream now that the source can
        // be queried for its stream list.
        pipelines = expand_all_tracks_pump(pipelines, &sources_by_uri);

        // Resolve each pipeline's stream index now that we can inspect
        // the source's stream list. For frame sources `select_stream`
        // always returns the synthetic index 0.
        for pl in &mut pipelines {
            let pump = sources_by_uri.get(&pl.source_uri).unwrap();
            pl.source_stream = select_stream(pump.streams(), &pl.selector)?;
            let info = pump
                .streams()
                .iter()
                .find(|s| s.index == pl.source_stream)
                .ok_or_else(|| Error::invalid("selected stream not in source"))?;
            pl.input_params = info.params.clone();
            pl.input_time_base = info.time_base;
        }

        // Auto-insert pixel-format conversion stages now that we know
        // the source stream's pixel format. Runs after the source is
        // open and before codec instantiation.
        for pl in &mut pipelines {
            pl.apply_pixel_format_auto_insert(&self.ctx.codecs);
        }

        // Instantiate decoders / filters / encoders for each track. The
        // serial path tells codecs to stay single-threaded; the pipelined
        // path passes its own thread budget below.
        let ctx = ExecutionContext::serial();
        for pl in &mut pipelines {
            pl.instantiate(&self.ctx.codecs, &ctx, &self.ctx.filters)?;
        }

        // Build the per-track output stream infos + open (or replace) the sink.
        // Multi-port filters add extra streams after their primary — the sink
        // sees all of them in `start()` before any frame flows.
        let out_streams = build_output_streams(&mut pipelines);

        let mut sink = self.open_sink(name, &out_streams)?;
        sink.start(&out_streams)?;

        // Main pump. Read packets / frames from every source in
        // round-robin until all are EOF. Routes vary by source shape:
        //   - Demuxer / Packets → next_packet()  → feed_packet()
        //   - Frames            → next_frame()   → feed_frame()
        let mut stats = ExecutorStats::default();
        let mut eof: HashMap<String, bool> =
            sources_by_uri.keys().map(|k| (k.clone(), false)).collect();
        let uris: Vec<String> = sources_by_uri.keys().cloned().collect();
        while eof.values().any(|e| !e) {
            for uri in &uris {
                if eof[uri] {
                    continue;
                }
                let pump = sources_by_uri.get_mut(uri).unwrap();
                match pump {
                    SourcePump::Demuxer(dmx) => match dmx.next_packet() {
                        Ok(pkt) => {
                            stats.packets_read += 1;
                            for (track_idx, pl) in pipelines.iter_mut().enumerate() {
                                if pl.source_uri != *uri {
                                    continue;
                                }
                                if pkt.stream_index != pl.source_stream {
                                    continue;
                                }
                                pl.feed_packet(&pkt, track_idx as u32, sink.as_mut(), &mut stats)?;
                            }
                        }
                        Err(Error::Eof) => {
                            eof.insert(uri.clone(), true);
                            continue;
                        }
                        Err(e) => return Err(e),
                    },
                    SourcePump::Packets(pkts) => match pkts.next_packet() {
                        Ok(pkt) => {
                            stats.packets_read += 1;
                            for (track_idx, pl) in pipelines.iter_mut().enumerate() {
                                if pl.source_uri != *uri {
                                    continue;
                                }
                                if pkt.stream_index != pl.source_stream {
                                    continue;
                                }
                                pl.feed_packet(&pkt, track_idx as u32, sink.as_mut(), &mut stats)?;
                            }
                        }
                        Err(Error::Eof) => {
                            eof.insert(uri.clone(), true);
                            continue;
                        }
                        Err(e) => return Err(e),
                    },
                    SourcePump::Frames { source, .. } => match source.next_frame() {
                        Ok(frame) => {
                            // Frame sources have a single synthetic
                            // stream — every track on this URI consumes
                            // the same frame. We clone for each track
                            // beyond the first so ownership works out.
                            stats.frames_decoded += 1;
                            let mut consumers: Vec<usize> = Vec::new();
                            for (track_idx, pl) in pipelines.iter().enumerate() {
                                if pl.source_uri == *uri {
                                    consumers.push(track_idx);
                                }
                            }
                            for &track_idx in &consumers {
                                pipelines[track_idx].feed_frame(
                                    frame.clone(),
                                    track_idx as u32,
                                    sink.as_mut(),
                                    &mut stats,
                                )?;
                            }
                        }
                        Err(Error::Eof) => {
                            eof.insert(uri.clone(), true);
                            continue;
                        }
                        Err(e) => return Err(e),
                    },
                }
            }
        }
        // EOF — drain each pipeline.
        for (track_idx, pl) in pipelines.iter_mut().enumerate() {
            pl.drain(track_idx as u32, sink.as_mut(), &mut stats)?;
        }
        sink.finish()?;
        Ok(stats)
    }

    pub(crate) fn build_track_runtime(&self, dag: &Dag, track: &MuxTrack) -> Result<TrackRuntime> {
        // Walk upstream chain, accumulating stages in reverse (top-down).
        // The chain ends at a Demuxer / PacketSource / FrameSource leaf.
        // The pixel-format auto-insert pass runs later, once the source
        // is open and its `CodecParameters.pixel_format` is known —
        // see `TrackRuntime::apply_pixel_format_auto_insert`.
        let mut stages: Vec<StageSpec> = Vec::new();
        let mut leaf_is_frames = false;
        let mut cur = track.upstream;
        let (source_uri, selector) = loop {
            match dag.node(cur) {
                DagNode::Demuxer { source } => {
                    break (source.clone(), ResolvedSelector::any());
                }
                DagNode::PacketSource { source } => {
                    // PacketSource is a packet-producing leaf — same
                    // shape as Demuxer for the rest of the pipeline.
                    break (source.clone(), ResolvedSelector::any());
                }
                DagNode::FrameSource { source } => {
                    // FrameSource is a frame-producing leaf — no decode
                    // stage in the upstream chain. The runtime detects
                    // this via the `Frames` SourcePump variant and
                    // routes frames straight into the filter chain.
                    leaf_is_frames = true;
                    break (source.clone(), ResolvedSelector::any());
                }
                DagNode::Select { upstream, selector } => match dag.node(*upstream) {
                    DagNode::Demuxer { source } | DagNode::PacketSource { source } => {
                        break (source.clone(), selector.clone());
                    }
                    DagNode::FrameSource { source } => {
                        // A `Select` over a frame source is degenerate —
                        // there's exactly one (synthetic) stream — but
                        // honour the kind constraint so a job that asks
                        // for `audio:` on a video frame source still
                        // errors loudly downstream rather than silently
                        // muxing a video frame as audio.
                        leaf_is_frames = true;
                        break (source.clone(), selector.clone());
                    }
                    _ => {
                        return Err(Error::other(
                            "job: nested Select above non-source-leaf is not yet supported",
                        ));
                    }
                },
                DagNode::Decode { upstream } => {
                    stages.push(StageSpec::Decode);
                    cur = *upstream;
                }
                DagNode::Filter {
                    upstream,
                    name,
                    params,
                } => {
                    stages.push(StageSpec::Filter {
                        name: name.clone(),
                        params: params.clone(),
                    });
                    cur = *upstream;
                }
                DagNode::PixConvert { upstream, target } => {
                    stages.push(StageSpec::Convert { target: *target });
                    cur = *upstream;
                }
                DagNode::Encode {
                    upstream,
                    codec,
                    params,
                } => {
                    stages.push(StageSpec::Encode {
                        codec: codec.clone(),
                        params: params.clone(),
                    });
                    cur = *upstream;
                }
                DagNode::Mux { .. } => {
                    return Err(Error::other(
                        "job: walked into a Mux node while building track runtime",
                    ));
                }
            }
        };
        stages.reverse();
        // FrameSource leaves emit decoded frames directly — drop any
        // `Decode` stage that the resolver inserted between the source
        // and the first filter / encode. Without this, the runtime
        // would try to feed frames into a decoder that expects packets.
        if leaf_is_frames {
            stages.retain(|s| !matches!(s, StageSpec::Decode));
        }
        Ok(TrackRuntime::new(
            source_uri, selector, track.kind, track.copy, stages,
        ))
    }

    /// Open a source URI into a [`SourcePump`]. Branches on the
    /// [`SourceOutput`] returned by the source registry — bytes-shape
    /// sources go through the historical container-probe + demuxer-open
    /// dance; packet-shape and frame-shape sources are wrapped directly.
    pub(crate) fn open_source(&self, uri: &str) -> Result<SourcePump> {
        match self.ctx.sources.open(uri)? {
            SourceOutput::Bytes(bytes) => {
                // The trait alias `BytesSource: Read + Seek + Send` and
                // `ReadSeek: Read + Seek` differ only in the explicit
                // `Send` bound; container open wants `Box<dyn ReadSeek>`.
                // Re-box through a shim so the lifetimes line up without
                // adding a `Send` bound to the existing demuxer trait.
                let mut file: Box<dyn ReadSeek> = Box::new(bytes);
                let ext = ext_from_uri(uri);
                let format = self
                    .ctx
                    .containers
                    .probe_input(&mut *file, ext.as_deref())?;
                let dmx = self
                    .ctx
                    .containers
                    .open_demuxer(&format, file, &self.ctx.codecs)?;
                Ok(SourcePump::Demuxer(dmx))
            }
            SourceOutput::Packets(p) => Ok(SourcePump::Packets(p)),
            SourceOutput::Frames(f) => {
                let streams = vec![synth_stream_info(f.params())];
                Ok(SourcePump::Frames { source: f, streams })
            }
        }
    }

    /// Convenience: open a source URI and assert it's bytes-shape.
    /// Retained for callers that strictly want a `Box<dyn Demuxer>` —
    /// today only a couple of legacy code paths and tests. New code
    /// should prefer [`open_source`](Self::open_source) and branch on
    /// the returned [`SourcePump`].
    pub(crate) fn open_demuxer(&self, uri: &str) -> Result<Box<dyn Demuxer>> {
        match self.open_source(uri)? {
            SourcePump::Demuxer(d) => Ok(d),
            SourcePump::Packets(_) | SourcePump::Frames { .. } => Err(Error::unsupported(format!(
                "source {uri}: opener returned non-bytes shape; use open_source() instead"
            ))),
        }
    }

    /// Open every unique source URI referenced by a `Demuxer` node in
    /// `dag`, then rewrite the node to `PacketSource` / `FrameSource`
    /// when the registry hands back a non-bytes shape. Returns the
    /// opened sources keyed by URI so the caller can move them into the
    /// run loop without re-opening.
    ///
    /// Bytes-shape sources keep their `Demuxer` node — the container
    /// probe + demuxer-open already happened during this probe call,
    /// and the resulting `Box<dyn Demuxer>` is returned in the map.
    pub(crate) fn resolve_source_shapes(
        &self,
        dag: &mut Dag,
    ) -> Result<HashMap<String, SourcePump>> {
        // Collect unique source URIs first to avoid double-opening when
        // the same source appears in multiple places (alias inlining,
        // multi-track outputs all reading the same input).
        let mut uris: Vec<(usize, String)> = Vec::new();
        for (idx, node) in dag.nodes().iter().enumerate() {
            if let DagNode::Demuxer { source } = node {
                uris.push((idx, source.clone()));
            }
        }
        let mut opened: HashMap<String, SourcePump> = HashMap::new();
        for (node_idx, uri) in uris {
            if !opened.contains_key(&uri) {
                let pump = self.open_source(&uri)?;
                opened.insert(uri.clone(), pump);
            }
            // Rewrite the node based on what the source actually is.
            match opened.get(&uri).unwrap() {
                SourcePump::Demuxer(_) => {
                    // Stay as Demuxer — historical path.
                }
                SourcePump::Packets(_) => {
                    dag.nodes_mut()[node_idx] = DagNode::PacketSource {
                        source: uri.clone(),
                    };
                }
                SourcePump::Frames { .. } => {
                    dag.nodes_mut()[node_idx] = DagNode::FrameSource {
                        source: uri.clone(),
                    };
                }
            }
        }
        Ok(opened)
    }

    /// Pipelined counterpart to [`Self::run_output`]. Builds the same
    /// per-track runtimes + demuxers + sink, then hands them off to
    /// [`crate::staged::run_pipelined`] which spawns a stage-per-thread
    /// worker graph.
    ///
    /// Falls back to [`Self::run_output`] when any source resolves to a
    /// non-bytes shape (`PacketSource` / `FrameSource`). The pipelined
    /// runner today only knows how to drive a `Box<dyn Demuxer>`; the
    /// staged-worker variants for the typed-source shapes are tracked as
    /// follow-up work and not blocking for the typed-source pipeline
    /// to be useful (RTMP + generator both run fine on the serial path).
    fn run_output_pipelined(
        &mut self,
        dag: &Dag,
        name: &str,
        threads: usize,
    ) -> Result<ExecutorStats> {
        // Probe sources without committing to the pipelined path —
        // bytes-shape stays pipelined, anything else falls back.
        let mut probed = dag.clone();
        let opened = self.resolve_source_shapes(&mut probed)?;
        let any_typed = opened
            .values()
            .any(|p| !matches!(p, SourcePump::Demuxer(_)));
        if any_typed {
            // Drop the pre-opened sources so `run_output` can re-open
            // them itself. Re-opening is cheap for the in-tree drivers
            // (file is mmap-friendly, generators are deterministic by
            // URI, RTMP follow-up may need to refactor this when it
            // lands as the second consumer).
            drop(opened);
            return self.run_output(&probed, name);
        }
        // Bytes-only path — everything below is unchanged.
        drop(opened);
        let prep = self.prepare_pipelined_run(dag, name, threads)?;
        staged::run_pipelined(prep.pipelines, prep.dmx_by_uri, prep.sink, prep.out_streams)
    }

    /// Shared prep used by both [`Self::run_output_pipelined`] and
    /// [`Self::spawn`]. Builds + instantiates the per-track runtimes,
    /// opens the demuxers, and resolves the sink. The returned struct
    /// is fully owned (no `'a`-borrowed members) so it can be moved
    /// onto a background thread.
    fn prepare_pipelined_run(
        &mut self,
        dag: &Dag,
        name: &str,
        threads: usize,
    ) -> Result<PreparedRun> {
        let root_id = dag.roots[name];
        let tracks: Vec<MuxTrack> = match dag.node(root_id) {
            DagNode::Mux { tracks, .. } => tracks.clone(),
            other => {
                return Err(Error::invalid(format!(
                    "job: output {name}: expected Mux root, got {other:?}"
                )));
            }
        };

        let mut pipelines: Vec<TrackRuntime> = Vec::new();
        for t in &tracks {
            pipelines.push(self.build_track_runtime(dag, t)?);
        }
        let mut dmx_by_uri: HashMap<String, Box<dyn Demuxer>> = HashMap::new();
        for pl in &pipelines {
            if !dmx_by_uri.contains_key(&pl.source_uri) {
                let dmx = self.open_demuxer(&pl.source_uri)?;
                dmx_by_uri.insert(pl.source_uri.clone(), dmx);
            }
        }
        // Expand `all:` tracks (kind == Unknown, selector == any) into
        // one TrackRuntime per source stream. Done after demuxers open
        // so the stream kinds are authoritative.
        pipelines = expand_all_tracks(pipelines, &dmx_by_uri);
        for pl in &mut pipelines {
            let dmx = dmx_by_uri.get(&pl.source_uri).unwrap();
            pl.source_stream = select_stream(dmx.streams(), &pl.selector)?;
            let info = dmx
                .streams()
                .iter()
                .find(|s| s.index == pl.source_stream)
                .ok_or_else(|| Error::invalid("selected stream not in demuxer"))?;
            pl.input_params = info.params.clone();
            pl.input_time_base = info.time_base;
        }
        // Auto-insert pixel-format conversion stages now that we know
        // the source stream's pixel format.
        for pl in &mut pipelines {
            pl.apply_pixel_format_auto_insert(&self.ctx.codecs);
        }
        let ctx = ExecutionContext::with_threads(threads);
        for pl in &mut pipelines {
            pl.instantiate(&self.ctx.codecs, &ctx, &self.ctx.filters)?;
        }
        let out_streams = build_output_streams(&mut pipelines);
        let sink = self.open_sink(name, &out_streams)?;
        Ok(PreparedRun {
            pipelines,
            dmx_by_uri,
            sink,
            out_streams,
        })
    }

    pub(crate) fn open_sink(
        &mut self,
        name: &str,
        out_streams: &[StreamInfo],
    ) -> Result<Box<dyn JobSink + Send>> {
        if let Some(s) = self.sink_overrides.remove(name) {
            return Ok(s);
        }
        if name == "@null" {
            return Ok(Box::new(NullSink::new()));
        }
        if name.starts_with('@') {
            if is_reserved_sink(name) {
                return Err(Error::unsupported(format!(
                    "job: no handler registered for reserved sink {name} (use with_sink_override)"
                )));
            }
            return Err(Error::invalid(format!(
                "job: alias {name} cannot be used as an output sink"
            )));
        }
        // File sink — open a muxer matching the path extension.
        let path = PathBuf::from(name);
        let fout = open_file_write(&path)?;
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .ok_or_else(|| Error::invalid(format!("job: output {name}: no extension")))?;
        let format = self
            .ctx
            .containers
            .container_for_extension(ext)
            .ok_or_else(|| {
                Error::FormatNotFound(format!("no muxer registered for extension .{ext}"))
            })?
            .to_owned();
        let muxer = self.ctx.containers.open_muxer(&format, fout, out_streams)?;
        Ok(Box::new(FileSink::new(path, muxer)))
    }
}

// ───────────────────────── per-track runtime ─────────────────────────

#[derive(Clone, Debug)]
pub(crate) enum StageSpec {
    Decode,
    Filter {
        name: String,
        params: serde_json::Value,
    },
    /// Pixel-format conversion stage (video only). Inserted either from
    /// an explicit `DagNode::PixConvert` or by the auto-insert pass in
    /// [`Executor::build_track_runtime`] when a codec declares
    /// `accepted_pixel_formats`.
    Convert {
        target: PixelFormat,
    },
    Encode {
        codec: String,
        params: serde_json::Value,
    },
}

/// One track's execution state: decoder + per-frame stage chain +
/// encoder, plus the resolved source URI + selected stream index. Used
/// by both the serial executor and the pipelined runner in
/// [`crate::pipeline`].
pub(crate) struct TrackRuntime {
    pub(crate) source_uri: String,
    pub(crate) selector: ResolvedSelector,
    pub(crate) source_stream: u32,
    pub(crate) kind: MediaType,
    pub(crate) copy: bool,
    pub(crate) stages: Vec<StageSpec>,
    pub(crate) input_params: CodecParameters,
    pub(crate) input_time_base: TimeBase,
    pub(crate) decoder: Option<Box<dyn Decoder>>,
    /// Per-frame stages in order (filters + pixel-format conversions)
    /// between the decoder and the encoder. The pipelined runner
    /// drains this vector to spawn one worker per stage.
    pub(crate) frame_stages: Vec<FrameStage>,
    pub(crate) encoder: Option<Box<dyn Encoder>>,
    pub(crate) encoder_time_base: Option<TimeBase>,
    /// Additional output-stream descriptors synthesised for multi-port
    /// filters on this track (e.g. spectrogram's video-port).
    /// Each extra gets its own [`StreamInfo`] in the sink's `start()`
    /// call, and extra-port frames emitted by `push`/`flush` flow
    /// straight into the sink tagged with their [`MediaType`].
    pub(crate) extra_output_streams: Vec<StreamInfo>,
    /// Per-filter-stage count of extra (non-primary) output ports.
    /// Parallel to `frame_stages` but only lists Filter stages — a
    /// PixConvert stage contributes nothing. Used by the staged
    /// runner to allocate stable stream indices for each filter's
    /// extras.
    pub(crate) extra_output_port_counts: Vec<u32>,
    /// Global index of this track's first auto-attached extra stream
    /// in the sink's final [`StreamInfo`] list. Populated by
    /// [`build_output_streams`] before the pipelined runner spawns.
    pub(crate) extras_base_index: u32,
}

/// A runtime filter instance in `StreamFilter` form. Obtained from the
/// [`FilterRegistry`]; the executor collects emitted frames on every
/// output port and routes them to downstream stages (port 0) or
/// directly to the sink (ports ≥ 1).
pub(crate) struct RuntimeFilter {
    pub(crate) inner: Box<dyn StreamFilter>,
}

/// A per-frame stage in the decoded-frame pipeline. Decoder output
/// flows through these in order to reach the encoder.
pub(crate) enum FrameStage {
    /// Named filter. Multi-port filters (e.g. spectrogram)
    /// emit on port 0 *and* extra ports; the executor separates the
    /// two streams after `push` returns.
    Filter(RuntimeFilter),
    /// Pixel-format conversion — calls
    /// [`oxideav_pixfmt::convert`] on each video frame. Non-video
    /// frames pass through unchanged. Carries the source-side
    /// `FrameInfo` (pixel format + width + height) since that data
    /// no longer lives on `VideoFrame`; we cache it at stage-build
    /// time off the running [`CodecParameters`].
    PixConvert {
        src_info: oxideav_pixfmt::FrameInfo,
        target: PixelFormat,
    },
}

/// Classification of a filter's emitted frames. `primary` goes to the
/// next frame stage (or the encoder); `extras` carry `(media_kind,
/// Frame)` pairs destined for the sink as auto-attached streams.
#[derive(Default)]
pub(crate) struct FilterEmissions {
    pub(crate) primary: Vec<Frame>,
    pub(crate) extras: Vec<(MediaType, Frame)>,
}

/// In-place `FilterContext` used by the executor. Collects emitted
/// frames into an `EmissionBuffer` so the stage worker can route them
/// after `push`/`flush` returns.
pub(crate) struct CollectCtx<'a> {
    pub(crate) port_kinds: &'a [MediaType],
    pub(crate) buf: FilterEmissions,
}

impl<'a> FilterContext for CollectCtx<'a> {
    fn emit(&mut self, port: usize, frame: Frame) -> Result<()> {
        match port {
            0 => self.buf.primary.push(frame),
            p if p < self.port_kinds.len() => {
                self.buf.extras.push((self.port_kinds[p], frame));
            }
            p => {
                return Err(Error::invalid(format!(
                    "filter emitted on port {p} but declared only {} ports",
                    self.port_kinds.len()
                )));
            }
        }
        Ok(())
    }
}

impl TrackRuntime {
    fn new(
        source_uri: String,
        selector: ResolvedSelector,
        kind: MediaType,
        copy: bool,
        stages: Vec<StageSpec>,
    ) -> Self {
        Self {
            source_uri,
            selector,
            source_stream: 0,
            kind,
            copy,
            stages,
            input_params: CodecParameters::audio(CodecId::new("")),
            input_time_base: TimeBase::new(1, 1),
            decoder: None,
            frame_stages: Vec::new(),
            encoder: None,
            encoder_time_base: None,
            extra_output_streams: Vec::new(),
            extra_output_port_counts: Vec::new(),
            extras_base_index: 0,
        }
    }

    /// Clone a pre-instantiate TrackRuntime — only the metadata + stage
    /// list, not the decoder / encoder / filter-instance slots (which
    /// are `None` at this point anyway). Used by
    /// [`expand_all_tracks`] when a `{kind: Unknown, selector: any}`
    /// track needs to fan out into one runtime per source stream.
    fn duplicate_pre_instantiate(&self, kind: MediaType, selector: ResolvedSelector) -> Self {
        assert!(
            self.decoder.is_none() && self.encoder.is_none() && self.frame_stages.is_empty(),
            "duplicate_pre_instantiate called post-instantiate"
        );
        TrackRuntime::new(
            self.source_uri.clone(),
            selector,
            kind,
            self.copy,
            self.stages.clone(),
        )
    }

    pub(crate) fn instantiate(
        &mut self,
        codecs: &CodecRegistry,
        ctx: &ExecutionContext,
        filters: &FilterRegistry,
    ) -> Result<()> {
        // Track running frame format through the stage stack so the encoder
        // can be constructed with a realistic parameter set.
        let mut running = self.input_params.clone();
        // Track the running PortSpec that feeds into the next filter. For
        // audio tracks this starts from `input_params`; for video from
        // `input_params`. We rebuild it each iteration so chained filters
        // see the immediately-upstream port, not the original demuxer port.
        let mut extra_stream_index_base: u32 = 1;
        for stage in &self.stages {
            match stage {
                StageSpec::Decode => {
                    if self.decoder.is_none() {
                        let mut d = codecs.make_decoder(&self.input_params)?;
                        d.set_execution_context(ctx);
                        self.decoder = Some(d);
                    }
                }
                StageSpec::Filter { name, params } => {
                    let in_port = port_spec_from_params(&running, self.input_time_base);
                    let filter = filters.make(name, params, std::slice::from_ref(&in_port))?;
                    // Inspect output ports: port 0 feeds downstream; every
                    // extra port becomes an auto-attached output stream.
                    let out_ports = filter.output_ports().to_vec();
                    let in_ports = filter.input_ports().to_vec();
                    if out_ports.is_empty() {
                        return Err(Error::invalid(format!(
                            "filter '{name}' declares zero output ports"
                        )));
                    }
                    // Port 0 rewrites `running` when the filter's port-0
                    // output differs from its port-0 input (i.e. the filter
                    // is actually changing shape — resample, resize, edge).
                    // Pass-through port-0 (spectrogram audio, volume) leaves
                    // `running` untouched so encoders see the actual source
                    // params rather than a filter's placeholder.
                    let input_matches_output = !in_ports.is_empty()
                        && port_params_equal(&in_ports[0].params, &out_ports[0].params);
                    if !input_matches_output {
                        running = port_params_to_codec_params(&out_ports[0].params, &running);
                    }
                    // Register StreamInfo for each extra port so the sink
                    // sees them before any frame flows.
                    for (i, port) in out_ports.iter().enumerate().skip(1) {
                        let (params_cp, time_base) = extra_port_stream(&port.params);
                        self.extra_output_streams.push(StreamInfo {
                            index: extra_stream_index_base + (i - 1) as u32,
                            time_base,
                            duration: None,
                            start_time: Some(0),
                            params: params_cp,
                        });
                    }
                    let extra_count = out_ports.len().saturating_sub(1) as u32;
                    extra_stream_index_base += extra_count;
                    self.extra_output_port_counts.push(extra_count);
                    self.frame_stages
                        .push(FrameStage::Filter(RuntimeFilter { inner: filter }));
                }
                StageSpec::Convert { target } => {
                    // Snapshot the running source shape (format + dims)
                    // to thread into pixfmt_convert — those used to live
                    // on VideoFrame but moved to CodecParameters with
                    // the slim.
                    let src_info = oxideav_pixfmt::FrameInfo::new(
                        running.pixel_format.unwrap_or(*target),
                        running.width.unwrap_or(0),
                        running.height.unwrap_or(0),
                    );
                    self.frame_stages.push(FrameStage::PixConvert {
                        src_info,
                        target: *target,
                    });
                    // Propagate the target format into the running
                    // CodecParameters so a downstream encoder sees the
                    // correct `pixel_format`.
                    running.pixel_format = Some(*target);
                }
                StageSpec::Encode { codec, params } => {
                    let mut enc_params = running.clone();
                    enc_params.codec_id = CodecId::new(codec.as_str());
                    // Map a handful of common params directly onto
                    // CodecParameters. Everything else is ignored by the
                    // generic path — codec-specific crates can read extras
                    // via their own param-parsing when we add that layer.
                    if let Some(br) = params.get("bitrate").and_then(|b| b.as_u64()) {
                        enc_params.bit_rate = Some(br);
                    }
                    if let Some(sr) = params.get("sample_rate").and_then(|b| b.as_u64()) {
                        enc_params.sample_rate = Some(sr as u32);
                    }
                    if let Some(ch) = params.get("channels").and_then(|b| b.as_u64()) {
                        enc_params.channels = Some(ch as u16);
                    }
                    if let Some(w) = params.get("width").and_then(|b| b.as_u64()) {
                        enc_params.width = Some(w as u32);
                    }
                    if let Some(h) = params.get("height").and_then(|b| b.as_u64()) {
                        enc_params.height = Some(h as u32);
                    }
                    let mut encoder = codecs.make_encoder(&enc_params)?;
                    encoder.set_execution_context(ctx);
                    let out_params = encoder.output_params().clone();
                    running = out_params.clone();
                    self.encoder_time_base = Some(match out_params.sample_rate {
                        Some(sr) if sr > 0 => TimeBase::new(1, sr as i64),
                        _ => self.input_time_base,
                    });
                    self.encoder = Some(encoder);
                }
            }
        }
        Ok(())
    }

    /// Rewrite `self.stages` to insert `StageSpec::Convert` in front of
    /// any `Encode` whose codec declares a non-empty
    /// `accepted_pixel_formats` set that does not include the currently
    /// running pixel format.
    ///
    /// Must be called after `input_params` has been populated from the
    /// demuxer and before `instantiate`. Audio tracks (where
    /// `input_params.pixel_format` is `None`) are a no-op — the pass
    /// only applies when a concrete source pixel format is available.
    ///
    /// **Caveat:** we assume filters preserve the pixel format
    /// (they do today — only audio filters exist — but this assumption
    /// will have to be revisited when video filters land and can
    /// change the pixel format).
    pub(crate) fn apply_pixel_format_auto_insert(&mut self, codecs: &CodecRegistry) {
        let src_fmt = match self.input_params.pixel_format {
            Some(f) => f,
            None => return,
        };
        let mut rewritten: Vec<StageSpec> = Vec::with_capacity(self.stages.len() + 2);
        let mut running: Option<PixelFormat> = None;
        for stage in std::mem::take(&mut self.stages).into_iter() {
            match &stage {
                StageSpec::Decode => {
                    running = Some(src_fmt);
                    rewritten.push(stage);
                }
                StageSpec::Filter { .. } => {
                    rewritten.push(stage);
                }
                StageSpec::Convert { target } => {
                    running = Some(*target);
                    rewritten.push(stage);
                }
                StageSpec::Encode { codec, .. } => {
                    if let Some(cur_fmt) = running {
                        if let Some(accepted) = codec_accepted_pixel_formats(codecs, codec) {
                            if !accepted.contains(&cur_fmt) {
                                let target = accepted[0];
                                rewritten.push(StageSpec::Convert { target });
                                running = Some(target);
                            }
                        }
                    }
                    rewritten.push(stage);
                }
            }
        }
        self.stages = rewritten;
    }

    pub(crate) fn output_params(&self) -> &CodecParameters {
        if let Some(enc) = &self.encoder {
            enc.output_params()
        } else {
            &self.input_params
        }
    }

    pub(crate) fn output_time_base(&self) -> TimeBase {
        self.encoder_time_base.unwrap_or(self.input_time_base)
    }

    /// Push one frame straight into this track's frame-stage chain.
    /// Used when the source is a [`SourcePump::Frames`] — there's no
    /// upstream decoder to drive, so we skip directly to `pump_frame`.
    pub(crate) fn feed_frame(
        &mut self,
        frame: Frame,
        track_index: u32,
        sink: &mut dyn JobSink,
        stats: &mut ExecutorStats,
    ) -> Result<()> {
        self.pump_frame(frame, track_index, sink, stats)
    }

    fn feed_packet(
        &mut self,
        pkt: &Packet,
        track_index: u32,
        sink: &mut dyn JobSink,
        stats: &mut ExecutorStats,
    ) -> Result<()> {
        if self.copy {
            // Pure copy: retag stream index + forward.
            let mut out = pkt.clone();
            out.stream_index = track_index;
            sink.write_packet(self.kind, &out)?;
            stats.packets_copied += 1;
            return Ok(());
        }
        // Stream frames through `pump_frame` as they're produced rather
        // than buffering them all into a Vec first. Streaming codecs
        // (MOD, S3M, XM tracker codecs; future live RTP / HLS feeds)
        // emit far more than one frame per packet — for songs that loop
        // via Bxx the decoder is effectively infinite. A bounded
        // drain-then-send shape here would never reach the first sink
        // call. Inner-scope borrow on `self.decoder` keeps `pump_frame`'s
        // `&mut self` borrow legal.
        if self.decoder.is_some() {
            if let Some(dec) = &mut self.decoder {
                dec.send_packet(pkt)?;
            }
            loop {
                let frame = match self.decoder.as_mut().unwrap().receive_frame() {
                    Ok(f) => f,
                    Err(Error::NeedMore) | Err(Error::Eof) => break,
                    Err(e) => return Err(e),
                };
                stats.frames_decoded += 1;
                self.pump_frame(frame, track_index, sink, stats)?;
            }
        }
        Ok(())
    }

    fn pump_frame(
        &mut self,
        frame: Frame,
        track_index: u32,
        sink: &mut dyn JobSink,
        stats: &mut ExecutorStats,
    ) -> Result<()> {
        let mut frames: Vec<Frame> = vec![frame];
        for stage in &mut self.frame_stages {
            let mut next = Vec::new();
            for f in frames {
                let produced = run_frame_stage_emit(stage, f)?;
                // Extras (multi-port filter emissions) go straight to the
                // sink as auto-attached streams.
                for (kind, frm) in produced.extras {
                    sink.write_frame(kind, &frm)?;
                    stats.frames_written += 1;
                }
                next.extend(produced.primary);
            }
            frames = next;
        }

        if let Some(enc) = &mut self.encoder {
            for frame in frames {
                enc.send_frame(&frame)?;
                loop {
                    match enc.receive_packet() {
                        Ok(mut p) => {
                            p.stream_index = track_index;
                            sink.write_packet(self.kind, &p)?;
                            stats.packets_encoded += 1;
                        }
                        Err(Error::NeedMore) | Err(Error::Eof) => break,
                        Err(e) => return Err(e),
                    }
                }
            }
        } else {
            // Raw frame to sink (player sink consumes this).
            for f in frames {
                sink.write_frame(self.kind, &f)?;
                stats.frames_written += 1;
            }
        }
        Ok(())
    }

    fn drain(
        &mut self,
        track_index: u32,
        sink: &mut dyn JobSink,
        stats: &mut ExecutorStats,
    ) -> Result<()> {
        if self.copy {
            return Ok(());
        }
        // Same streaming shape as `pump_packet` — decoder may emit any
        // number of frames during flush (MOD plays its outro after EOF).
        if self.decoder.is_some() {
            if let Some(dec) = &mut self.decoder {
                dec.flush()?;
            }
            loop {
                let frame = match self.decoder.as_mut().unwrap().receive_frame() {
                    Ok(f) => f,
                    Err(Error::NeedMore) | Err(Error::Eof) => break,
                    Err(e) => return Err(e),
                };
                stats.frames_decoded += 1;
                self.pump_frame(frame, track_index, sink, stats)?;
            }
        }
        // Flush frame stages. Filters may hold internal buffers
        // (resampler tail, noise-gate decay); pixel-format converts
        // are stateless so they drain trivially. After each stage
        // flushes, push its residual frames through the remaining
        // stages downstream.
        let mut tail: Vec<Frame> = Vec::new();
        for i in 0..self.frame_stages.len() {
            let flushed = flush_frame_stage_emit(&mut self.frame_stages[i])?;
            // Extras from a flushing filter go straight to the sink.
            for (kind, frm) in flushed.extras {
                sink.write_frame(kind, &frm)?;
                stats.frames_written += 1;
            }
            let mut primary = flushed.primary;
            // Push the flushed frames through the remaining downstream
            // stages so a later pixel-format convert still sees them.
            for j in (i + 1)..self.frame_stages.len() {
                let mut next: Vec<Frame> = Vec::new();
                for f in primary.drain(..) {
                    let produced = run_frame_stage_emit(&mut self.frame_stages[j], f)?;
                    for (kind, frm) in produced.extras {
                        sink.write_frame(kind, &frm)?;
                        stats.frames_written += 1;
                    }
                    next.extend(produced.primary);
                }
                primary = next;
            }
            tail.extend(primary);
        }
        if let Some(enc) = &mut self.encoder {
            for frame in tail {
                enc.send_frame(&frame)?;
                loop {
                    match enc.receive_packet() {
                        Ok(mut p) => {
                            p.stream_index = track_index;
                            sink.write_packet(self.kind, &p)?;
                            stats.packets_encoded += 1;
                        }
                        Err(Error::NeedMore) | Err(Error::Eof) => break,
                        Err(e) => return Err(e),
                    }
                }
            }
            enc.flush()?;
            loop {
                match enc.receive_packet() {
                    Ok(mut p) => {
                        p.stream_index = track_index;
                        sink.write_packet(self.kind, &p)?;
                        stats.packets_encoded += 1;
                    }
                    Err(Error::NeedMore) | Err(Error::Eof) => break,
                    Err(e) => return Err(e),
                }
            }
        } else {
            for f in tail {
                sink.write_frame(self.kind, &f)?;
                stats.frames_written += 1;
            }
        }
        Ok(())
    }
}

// `drain_decoder` was removed — buffering an entire decoder emission
// into a Vec is unsound for streaming codecs (MOD/S3M/XM tracker codecs
// emit until song end, songs with Bxx loops are effectively infinite),
// and it deferred all sink writes until decode completed even for
// finite codecs. Both call sites (TrackRuntime::pump_packet + drain,
// staged::run_decode_stage) now stream frames through the sink as they
// land, which both back-pressures naturally and keeps memory bounded.

/// Drive one frame through a [`FrameStage`] and return the full
/// primary + extras emission set. Pixel-format converts on a
/// non-video frame pass through unchanged; multi-port filters
/// (spectrogram) put port-1+ frames into `extras` for the caller to
/// route straight to the sink as an auto-attached output stream.
pub(crate) fn run_frame_stage_emit(
    stage: &mut FrameStage,
    frame: Frame,
) -> Result<FilterEmissions> {
    match stage {
        FrameStage::Filter(f) => run_filter(f, frame),
        FrameStage::PixConvert { src_info, target } => match frame {
            Frame::Video(v) => {
                let out = pixfmt_convert(&v, *src_info, *target, &ConvertOptions::default())?;
                Ok(FilterEmissions {
                    primary: vec![Frame::Video(out)],
                    extras: Vec::new(),
                })
            }
            other => Ok(FilterEmissions {
                primary: vec![other],
                extras: Vec::new(),
            }),
        },
    }
}

/// Flush any residual frames held inside a [`FrameStage`]. Audio
/// filters may have tail samples; pixel-format converts are stateless
/// and return an empty [`FilterEmissions`].
pub(crate) fn flush_frame_stage_emit(stage: &mut FrameStage) -> Result<FilterEmissions> {
    match stage {
        FrameStage::Filter(f) => flush_filter(f),
        FrameStage::PixConvert { .. } => Ok(FilterEmissions::default()),
    }
}

pub(crate) fn run_filter(filter: &mut RuntimeFilter, frame: Frame) -> Result<FilterEmissions> {
    let port_kinds: Vec<MediaType> = filter.inner.output_ports().iter().map(|p| p.kind).collect();
    let mut ctx = CollectCtx {
        port_kinds: &port_kinds,
        buf: FilterEmissions::default(),
    };
    filter.inner.push(&mut ctx, 0, &frame)?;
    Ok(ctx.buf)
}

pub(crate) fn flush_filter(filter: &mut RuntimeFilter) -> Result<FilterEmissions> {
    let port_kinds: Vec<MediaType> = filter.inner.output_ports().iter().map(|p| p.kind).collect();
    let mut ctx = CollectCtx {
        port_kinds: &port_kinds,
        buf: FilterEmissions::default(),
    };
    filter.inner.flush(&mut ctx)?;
    Ok(ctx.buf)
}

/// True when two [`PortParams`] describe the same concrete shape (same
/// sample rate/channels/format for audio, same dimensions/pix-fmt/time
/// base for video). Used to detect pass-through port 0 so we don't
/// overwrite the running `CodecParameters` with a filter's placeholder
/// input params.
pub(crate) fn port_params_equal(a: &PortParams, b: &PortParams) -> bool {
    match (a, b) {
        (
            PortParams::Audio {
                sample_rate: ar,
                channels: ac,
                format: af,
            },
            PortParams::Audio {
                sample_rate: br,
                channels: bc,
                format: bf,
            },
        ) => ar == br && ac == bc && af == bf,
        (
            PortParams::Video {
                width: aw,
                height: ah,
                format: af,
                time_base: atb,
            },
            PortParams::Video {
                width: bw,
                height: bh,
                format: bf,
                time_base: btb,
            },
        ) => aw == bw && ah == bh && af == bf && atb == btb,
        (PortParams::Subtitle, PortParams::Subtitle) => true,
        (PortParams::Metadata, PortParams::Metadata) => true,
        _ => false,
    }
}

/// Derive a [`PortSpec`] describing the current running format inside
/// the stage stack. Only Audio / Video variants are produced today.
/// Expand `all:`-originated tracks (kind == `MediaType::Unknown`,
/// selector without an explicit kind constraint) into one
/// [`TrackRuntime`] per source stream exposed by the open demuxer.
///
/// The DAG builder produces a single MuxTrack per `all:` entry; it
/// can't know the source's stream layout statically, so it leaves the
/// kind as `Unknown` and the selector as "any". The executor's prep
/// phase (both serial and pipelined) calls this after opening
/// demuxers so the fan-out sees the authoritative stream list.
///
/// Tracks with an explicit kind (from `audio:` / `video:` /
/// `subtitle:` lists) pass through unchanged.
pub(crate) fn expand_all_tracks(
    pipelines: Vec<TrackRuntime>,
    dmx_by_uri: &HashMap<String, Box<dyn Demuxer>>,
) -> Vec<TrackRuntime> {
    let mut out: Vec<TrackRuntime> = Vec::with_capacity(pipelines.len());
    for pl in pipelines {
        let needs_expand = pl.kind == MediaType::Unknown
            && pl.selector.kind.is_none()
            && pl.selector.index.is_none();
        if !needs_expand {
            out.push(pl);
            continue;
        }
        let Some(dmx) = dmx_by_uri.get(&pl.source_uri) else {
            out.push(pl);
            continue;
        };
        let streams = dmx.streams();
        if streams.is_empty() {
            out.push(pl);
            continue;
        }
        // One duplicate per source stream. Selector pinned to that
        // stream's index so later `select_stream` hits deterministically.
        for s in streams {
            let selector = ResolvedSelector {
                kind: Some(s.params.media_type),
                index: None,
            };
            out.push(pl.duplicate_pre_instantiate(s.params.media_type, selector));
        }
    }
    out
}

/// Like [`expand_all_tracks`] but keyed off a [`SourcePump`] map. Used
/// by the new typed-source path; mirrors the legacy demuxer-only
/// helper so the serial + pipelined runners can both call into a
/// single fan-out routine regardless of source shape.
pub(crate) fn expand_all_tracks_pump(
    pipelines: Vec<TrackRuntime>,
    sources_by_uri: &HashMap<String, SourcePump>,
) -> Vec<TrackRuntime> {
    let mut out: Vec<TrackRuntime> = Vec::with_capacity(pipelines.len());
    for pl in pipelines {
        let needs_expand = pl.kind == MediaType::Unknown
            && pl.selector.kind.is_none()
            && pl.selector.index.is_none();
        if !needs_expand {
            out.push(pl);
            continue;
        }
        let Some(pump) = sources_by_uri.get(&pl.source_uri) else {
            out.push(pl);
            continue;
        };
        let streams = pump.streams();
        if streams.is_empty() {
            out.push(pl);
            continue;
        }
        for s in streams {
            let selector = ResolvedSelector {
                kind: Some(s.params.media_type),
                index: None,
            };
            out.push(pl.duplicate_pre_instantiate(s.params.media_type, selector));
        }
    }
    out
}

pub(crate) fn port_spec_from_params(cp: &CodecParameters, tb: TimeBase) -> PortSpec {
    match cp.media_type {
        MediaType::Audio => PortSpec::audio(
            "in",
            cp.sample_rate.unwrap_or(48_000),
            cp.channels.unwrap_or(2),
            cp.sample_format.unwrap_or(SampleFormat::F32),
        ),
        _ => PortSpec::video(
            "in",
            cp.width.unwrap_or(0),
            cp.height.unwrap_or(0),
            cp.pixel_format.unwrap_or(PixelFormat::Yuv420P),
            tb,
        ),
    }
}

/// Merge a filter output port's [`PortParams`] into a running
/// [`CodecParameters`]. The codec id is left unchanged — it describes
/// *packet-level* codec, while the port describes *frame-level*
/// shape. Downstream encoders overwrite codec_id from the track
/// spec's `codec` field.
pub(crate) fn port_params_to_codec_params(
    port: &PortParams,
    prev: &CodecParameters,
) -> CodecParameters {
    let mut cp = prev.clone();
    match port {
        PortParams::Audio {
            sample_rate,
            channels,
            format,
        } => {
            cp.media_type = MediaType::Audio;
            cp.sample_rate = Some(*sample_rate);
            cp.channels = Some(*channels);
            cp.sample_format = Some(*format);
        }
        PortParams::Video {
            width,
            height,
            format,
            ..
        } => {
            cp.media_type = MediaType::Video;
            cp.width = Some(*width);
            cp.height = Some(*height);
            cp.pixel_format = Some(*format);
        }
        PortParams::Subtitle => cp.media_type = MediaType::Subtitle,
        PortParams::Metadata => cp.media_type = MediaType::Data,
    }
    cp
}

/// Build a `(CodecParameters, TimeBase)` pair for an extra (non-primary)
/// filter output port. The sink sees this as a raw frame stream —
/// `codec_id` is set to `"rawaudio"` / `"rawvideo"` matching the
/// convention used by `@display`.
pub(crate) fn extra_port_stream(port: &PortParams) -> (CodecParameters, TimeBase) {
    match port {
        PortParams::Audio {
            sample_rate,
            channels,
            format,
        } => {
            let mut cp = CodecParameters::audio(CodecId::new("rawaudio"));
            cp.sample_rate = Some(*sample_rate);
            cp.channels = Some(*channels);
            cp.sample_format = Some(*format);
            let tb = TimeBase::new(1, (*sample_rate as i64).max(1));
            (cp, tb)
        }
        PortParams::Video {
            width,
            height,
            format,
            time_base,
        } => {
            let mut cp = CodecParameters::video(CodecId::new("rawvideo"));
            cp.width = Some(*width);
            cp.height = Some(*height);
            cp.pixel_format = Some(*format);
            // Frame rate is the reciprocal of the time base.
            let tb_rat = time_base.as_rational();
            cp.frame_rate = Some(Rational::new(tb_rat.den, tb_rat.num));
            (cp, *time_base)
        }
        PortParams::Subtitle => (
            CodecParameters::audio(CodecId::new("rawsubtitle")),
            TimeBase::new(1, 1000),
        ),
        PortParams::Metadata => (
            CodecParameters::audio(CodecId::new("rawdata")),
            TimeBase::new(1, 1),
        ),
    }
}

/// Assemble the final sink-facing [`StreamInfo`] list from a set of
/// track runtimes. Primary streams come first (one per track in
/// declaration order), followed by any auto-attached extras declared
/// by multi-port filters. Indices are renumbered to be consecutive so
/// the sink sees a dense 0..N layout.
///
/// Also stamps `extras_base_index` on each [`TrackRuntime`] so the
/// staged runner can tag extra-port frames with the right stream
/// index at emission time.
pub(crate) fn build_output_streams(pipelines: &mut [TrackRuntime]) -> Vec<StreamInfo> {
    let mut out = Vec::with_capacity(pipelines.len());
    for (i, pl) in pipelines.iter().enumerate() {
        out.push(StreamInfo {
            index: i as u32,
            time_base: pl.output_time_base(),
            duration: None,
            start_time: Some(0),
            params: pl.output_params().clone(),
        });
    }
    let mut next_idx = pipelines.len() as u32;
    for pl in pipelines {
        pl.extras_base_index = next_idx;
        for extra in &pl.extra_output_streams {
            let mut e = extra.clone();
            e.index = next_idx;
            next_idx += 1;
            out.push(e);
        }
    }
    out
}

pub(crate) fn select_stream(streams: &[StreamInfo], sel: &ResolvedSelector) -> Result<u32> {
    let filtered: Vec<&StreamInfo> = streams
        .iter()
        .filter(|s| match sel.kind {
            Some(k) => s.params.media_type == k,
            None => true,
        })
        .collect();
    if filtered.is_empty() {
        return Err(Error::invalid(format!(
            "job: no streams match selector {sel:?}"
        )));
    }
    let idx = sel.index.unwrap_or(0) as usize;
    let picked = filtered
        .get(idx)
        .ok_or_else(|| Error::invalid(format!("job: selector index {idx} out of range")))?;
    Ok(picked.index)
}

pub(crate) fn ext_from_uri(uri: &str) -> Option<String> {
    let last = uri.rsplit('/').next().unwrap_or(uri);
    let last = last.split('?').next().unwrap_or(last);
    let dot = last.rfind('.')?;
    Some(last[dot + 1..].to_ascii_lowercase())
}

// ───────────────────────── handle ─────────────────────────

/// Owned bundle of everything `staged::run_pipelined_with_control` needs,
/// produced synchronously by [`Executor::prepare_pipelined_run`] so the
/// caller can either run it inline ([`Executor::run`]) or hand it to a
/// background thread ([`Executor::spawn`]). Holds no `'a`-borrowed
/// fields — moves freely across threads.
pub(crate) struct PreparedRun {
    pub(crate) pipelines: Vec<TrackRuntime>,
    pub(crate) dmx_by_uri: HashMap<String, Box<dyn Demuxer>>,
    pub(crate) sink: Box<dyn JobSink + Send>,
    pub(crate) out_streams: Vec<StreamInfo>,
}

/// Live handle to a background-running [`Executor`]. Returned by
/// [`Executor::spawn`]; supports seek, abort, and progress polling.
///
/// Dropping the handle without [`Self::stop`] sets the abort flag and
/// detaches the join handle — the worker threads tear down in the
/// background. Use [`Self::stop`] to wait for clean exit + recover
/// the [`ExecutorStats`].
pub struct ExecutorHandle {
    abort: std::sync::Arc<crate::staged::AbortState>,
    seek_tx: std::sync::mpsc::Sender<crate::staged::SeekCmd>,
    progress_rx: std::sync::mpsc::Receiver<crate::staged::Progress>,
    join: Option<std::thread::JoinHandle<Result<ExecutorStats>>>,
    finished: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl ExecutorHandle {
    pub(crate) fn start(prep: PreparedRun) -> Self {
        let abort = crate::staged::AbortState::new();
        let (seek_tx, seek_rx) = std::sync::mpsc::channel::<crate::staged::SeekCmd>();
        let (progress_tx, progress_rx) =
            std::sync::mpsc::sync_channel::<crate::staged::Progress>(64);
        let finished = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let abort_t = abort.clone();
        let finished_t = finished.clone();
        let join = std::thread::Builder::new()
            .name("oxideav-pipeline-exec".into())
            .spawn(move || {
                let result = crate::staged::run_pipelined_with_control(
                    prep.pipelines,
                    prep.dmx_by_uri,
                    prep.sink,
                    prep.out_streams,
                    crate::staged::PipelineControl {
                        seek_rx: Some(seek_rx),
                        progress_tx: Some(progress_tx),
                        abort: Some(abort_t),
                    },
                );
                finished_t.store(true, std::sync::atomic::Ordering::SeqCst);
                result
            })
            .expect("oxideav-pipeline: failed to spawn executor thread");
        Self {
            abort,
            seek_tx,
            progress_rx,
            join: Some(join),
            finished,
        }
    }

    /// Issue a seek to `(stream_idx, pts)` in `time_base` units. The
    /// demuxer thread receives the command, increments its generation,
    /// broadcasts a [`crate::BarrierKind::SeekFlush`] on every route,
    /// then calls `demuxer.seek_to`. The sink will see a `barrier(...)`
    /// callback once the flush reaches the mux loop.
    pub fn seek(&self, stream_idx: u32, pts: i64, time_base: oxideav_core::TimeBase) -> Result<()> {
        self.seek_tx
            .send(crate::staged::SeekCmd {
                stream_idx,
                pts,
                time_base,
            })
            .map_err(|_| Error::other("ExecutorHandle: seek channel closed (executor exited)"))
    }

    /// Non-blocking poll of the most recent progress message. Returns
    /// `None` if no update has arrived since the last poll.
    pub fn try_progress(&self) -> Option<crate::staged::Progress> {
        let mut latest = None;
        while let Ok(p) = self.progress_rx.try_recv() {
            latest = Some(p);
        }
        latest
    }

    /// True once the background thread has returned (EOF, abort, or
    /// error). Safe to poll concurrently with [`Self::try_progress`].
    pub fn has_finished(&self) -> bool {
        self.finished.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Set the abort flag without joining. Useful when the engine is
    /// quitting and wants the worker to wind down while it tears down
    /// its driver. Pair with [`Self::stop`] to actually collect the
    /// thread's result.
    pub fn request_abort(&self) {
        self.abort.request_abort();
    }

    /// Set the abort flag, join the worker thread, and return its
    /// final stats (or the first error it observed).
    pub fn stop(mut self) -> Result<ExecutorStats> {
        self.abort.request_abort();
        match self.join.take() {
            Some(h) => h
                .join()
                .map_err(|_| Error::other("ExecutorHandle: worker thread panicked"))?,
            None => Err(Error::other(
                "ExecutorHandle: stop() called twice or after drop",
            )),
        }
    }
}

impl Drop for ExecutorHandle {
    fn drop(&mut self) {
        self.abort.request_abort();
        // Detach: the background thread observes the abort flag and
        // tears down. We don't join here because the engine may have
        // already called `stop()` (which moved the join handle out).
    }
}

// ───────────────────────── stats ─────────────────────────

#[derive(Clone, Copy, Debug, Default)]
pub struct ExecutorStats {
    pub packets_read: u64,
    pub packets_copied: u64,
    pub packets_encoded: u64,
    pub frames_decoded: u64,
    pub frames_written: u64,
}

impl ExecutorStats {
    pub(crate) fn merge(&mut self, other: &Self) {
        self.packets_read += other.packets_read;
        self.packets_copied += other.packets_copied;
        self.packets_encoded += other.packets_encoded;
        self.frames_decoded += other.frames_decoded;
        self.frames_written += other.frames_written;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ext_from_uri_basic() {
        assert_eq!(ext_from_uri("foo.mp3").as_deref(), Some("mp3"));
        assert_eq!(
            ext_from_uri("https://x/y.mkv?token=1").as_deref(),
            Some("mkv")
        );
        assert_eq!(ext_from_uri("/no/ext"), None);
    }

    // Legacy `build_video_filter` / `build_audio_filter` unit tests
    // were removed when filter dispatch moved to `FilterRegistry`.
    // See `filter_registry::tests::*` for the current equivalents.
}
