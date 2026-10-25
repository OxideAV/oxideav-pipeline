# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.15](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.14...v0.1.15) - 2026-10-25

### Other

- Merge decoder-reported parameters into the stream at every decoder site; follow mid-stream changes

### Added

- Decoder-reported stream parameters (oxideav-core 0.1.42
  `Decoder::output_params` / `OutputParamsWatch`). Every decoder the
  pipeline builds — `transcode_simple*`, `Pipeline` routes, the serial
  and pipelined executor, the unlabelled-video pre-roll — has its
  report merged over the container's stream parameters right after
  construction, so conversion stages, encoder parameters and the
  sink's `start()` streams are planned from what the decoder emits
  (cropped size, pixel layout, sample format) rather than the
  container's claim; the watch is observed after every
  `receive_frame` (one integer compare per frame). A mid-stream change
  re-plans pixel-format conversion stages; a change a filter or an
  encoder would have to follow fails with `Error::Unsupported` naming
  both shapes; raw-frame sinks receive the new-shape frames.
- `Pipeline::stream_params(index)`: a bound stream's parameters with
  the decoder's report merged in, kept current through `run`.

### Changed

- The pre-roll wrapper and the hardware-fallback decoder forward
  `output_params` / `output_params_generation` to the decoder they
  wrap; the pre-roll is skipped whenever `output_params` already
  states the layout (it stays the fallback for decoders that answer
  `None`).

## [0.1.14](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.13...v0.1.14) - 2026-10-25

### Other

- bound the decode-side prep encode by the bench window
- announce a thread budget to every codec the pipeline builds (default auto)
- plan from the encoder that gets built; pre-roll unlabelled video to its first picture
- hardware-engine crates load OS APIs through dlsym, not libloading
- edition 2024, MSRV 1.95

### Added

- Codec thread budgets on every entry point. `transcode_simple`,
  `transcode_simple_with`, `transcode_simple_in`, `Pipeline` and the
  bench now hand each decoder / encoder they build an
  `ExecutionContext` before its first packet / frame, defaulting to
  `ExecutionContext::auto()` — previously they never called
  `set_execution_context`, so every codec ran single-threaded (a 1080p
  AV1 / VP9 / HEVC transcode used one core). New knobs:
  `transcode_simple_in_with` (explicit budget), `Pipeline::with_threads`
  / `Pipeline::with_execution_context`, `bench::run_bench_in` /
  `bench::run_bench_all_in`; helpers `budget_from_threads` (0 = auto,
  1 = serial) and `stream_budget`, the split rule (video keeps the
  budget, audio / subtitle next to video runs serial) now also applied
  per track by the pipelined executor. Output is unchanged at every
  budget.
- `bench`: an encoder that refuses the requested size but accepts the
  next 16-aligned one (the H.264 encoder at 1920x1080) is benched at
  1920x1088, and when it is the encoder synthesising the decode-side
  stream the decoders are benched on that stream — `oxideav bench h264`
  no longer reports "can't synth decode stream" at 1080p. The decode
  prep no longer keeps a second copy of the synthesised frames.
- `bench`: the decode-side prep encode stops taking frames once the
  bench window (`bench_duration_secs`) has elapsed, so a slow encoder
  (pure-Rust H.264 at 1080p, ~0.2 fps) no longer spends ~40 minutes on
  the 500-frame prep set before any decoder is timed; `oxideav bench
  h264` at defaults now finishes in about half a minute.

- `preferred_encoder_caps` / `preferred_decoder_caps`: the capabilities
  of the implementation `make_encoder_with` / `make_decoder_with` would
  build under a `CodecPreferences`, ranked the same way.
- First-picture pre-roll for re-encoded video streams whose pixel layout
  neither the container nor the decoder states up front (WebM, AVI,
  MPEG-TS, MOV): the decoder is fed until its first picture, the layout
  is taken from the decoder or read off the picture's plane geometry,
  and the packets read are replayed, so every packet still reaches the
  decoder and the muxer exactly once. Applies to the serial and pipelined
  executors and to `transcode_simple*`.

### Fixed

- Audio resample / downmix / sample-format planning and pixel-format
  negotiation read the capabilities of the first *registered* encoder
  and ignored `CodecPreferences`. On macOS that is `opus_audiotoolbox`,
  which declares no `accepted_sample_rates`, so no resample stage was
  planned and `convert tone44100.wav x.opus` failed with "unsupported
  input sample rate 44100 Hz". They now use the encoder that is actually
  built.
- Video transcodes from a container with no declared pixel layout into
  an encoder that requires one failed before the first packet
  (`rawvideo encoder: pixel_format is required` for webm / avi / ts /
  mov → y4m).
- Decoder selection, including the hardware → software
  `FallbackDecoder` candidate list, goes through the same preference
  ranking as encoder selection (one shared ranking helper).

### Changed

- edition 2024, MSRV 1.95

## [0.1.13](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.12...v0.1.13) - 2026-10-04

### Other

- probe with the representative encoder's output parameters
- pick the input streams an output container can hold
- convert an undeclared audio format for format-constrained encoders
- take the pixel layout from the decoder when the container has none
- honour CodecPreferences; build encoders without the demuxer's option bag
- negotiate the audio shape in front of encoders; drop inherited bit rate

## [0.1.12](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.11...v0.1.12) - 2026-08-20

### Other

- track-1 encode attribution + mux-loop barrier-failure record site
- attach the failing output's partial ExecutorStats to RunFailure
- hostile/edge disposal + attribution coverage; README failure sections
- failed-output disposal — opt-in JobSink::abandon + FileSink cleanup
- typed failure attribution — run_reporting/stop_reporting surface RunFailure
- pin the parallel-output gating contract on both knobs
- hostile/edge coverage for parallel outputs + multi-source seek
- fan every seek out to all routed sources
- escape Windows path separators in embedded job JSON
- run multi-output jobs concurrently in budget-clamped waves
- name the convert pixel-format strings on their own terms
- criterion graph-resolution suite + README refresh for r400 capabilities
- error-propagation coherence — failing sink/source surfaces the original error, no hang
- key-directed TrackInput deserialization — exponential nested-chain parse fixed
- native packet- and frame-source pumps — spawn() works on typed sources
- empty-codec + contradictory selector-kind + bare-@ key rejection
- pin per-kind stream ordinals in all: fan-out — dual same-kind streams no longer collapse
- defensive alias-cycle guard in to_dag — Err instead of stack overflow
- add CI / crates.io / docs.rs / MIT-license badges
- refresh to current status, drop per-round changelog cruft

### Added

- Failed-output disposal:
  `Executor::with_discard_failed_outputs(true)` opts a job into
  partial-output cleanup. A failing output's sink now receives the new
  `JobSink::abandon` hook (default no-op) INSTEAD of being silently
  dropped mid-write; the built-in `FileSink` implements it by closing
  the muxer's file handle and deleting the partially-written file, so
  a failed transcode leaves no half-file behind for a downstream
  consumer to mistake for a finished one. Scope: the output's own run
  failed after sink resolution, or — on a multi-output job — the sink
  was resolved for a wave whose PREPARATION failed before the wave
  started (its just-created zero-byte file would otherwise linger).
  Healthy wave-mates of a failing output still run to completion and
  finalise normally, a clean `ExecutorHandle::stop` (no recorded
  error) still finalises via `finish` (stop is a cancel, not a
  failure), and the default (`false`) keeps the historical
  partial-file-stays behaviour. `FileSink::abandon` is idempotent and
  later hooks report the abandonment instead of resurrecting the
  file. Pinned by `tests/discard_failed_outputs.rs` (12 contracts,
  serial + pipelined + spawn) via a new writable stub muxer whose
  header/trailer bytes let tests distinguish a finalised file from a
  torn-down one, plus a dying-demuxer stub for healthy-graph
  mid-stream source faults.

- Typed failure attribution: `Executor::run_reporting` and
  `ExecutorHandle::stop_reporting` return a `RunFailure` pairing the
  ORIGINAL first error with *where* it fired — the owning output key,
  a `FailureStage` (`Prepare` / `Source` / `Copy` / `Decode` /
  `Filter` / `Convert` / `Encode` / `Sink` / `SinkFinish`), the
  track index when the failing stage belongs to one, and the failing
  output's partial `ExecutorStats` snapshot ("failed after writing N
  frames" as data — all-zero for preparation failures, taken after
  worker teardown joins on the pipelined path so it reflects
  everything that actually reached the sink). Engines can now
  render "encoder failed on track 0 of out.mp4" (or branch
  programmatically on the failing stage) without parsing log lines.
  The historical `run()` / `stop()` surfaces are unchanged wrappers:
  `From<RunFailure> for Error` recovers the stored original error
  unwrapped — never a re-stringified copy — so error-kind matching
  behaves identically on both surfaces, and the first-error-wins
  contract pinned by `tests/error_propagation.rs` is untouched. The
  serial and pipelined executors attribute the same failure site to
  the same stage; `SinkFinish` is split from `Sink` because every
  mid-stream byte already landed when finalisation fails. Pinned by
  `tests/failure_attribution.rs`: 18 contracts covering job-level
  validation, unknown-codec prepare, mid-stream source / filter /
  encoder-send / encoder-flush faults, sink start / write / finish
  faults (each on both executor paths where both exist), multi-output
  earliest-in-document-order naming, run/run_reporting root-cause
  parity, and spawn-path attribution via `stop_reporting`.

- Multi-output parallelism: `Executor::run` on a job with several
  outputs and `threads ≥ 2` now runs the outputs concurrently instead
  of one after another (closing the long-standing "multi-output
  parallelism is a deliberate follow-up" note in the executor module
  doc). Outputs are chunked into document-order waves whose width is
  clamped through `ExecutionContext::effective_workers(n_outputs)` —
  the same clamp codecs use for internal fan-out — and each wave's
  outputs run on scoped threads with an even split of the job budget
  as their codec-internal `ExecutionContext`. Preparation stays
  sequential in document order (setup-error precedence unchanged).
  Error contract: the earliest failing output in DOCUMENT order wins
  deterministically, healthy wave-mates still deliver their complete
  streams, and waves after the failure never start. `threads == 1`
  keeps the strictly-sequential serial loop; `spawn()` remains
  single-output. Pinned by `tests/multi_output_parallel.rs`
  (completeness + serial stats parity, a rendezvous proving real
  overlap, wave-width ≤ budget, and three error-precedence /
  wave-teardown contracts).

- Multi-source seek fan-out: a seek dispatched through
  `ExecutorHandle` now reaches EVERY routed source of the job instead
  of only the first (closing the "multi-URI seek is a follow-up" note
  in the staged runner). The first routed source pump owns the
  handle's receiver and forwards each `SeekCmd` to a dedicated channel
  per sibling routed source before handling it locally; each source
  answers with its own generation-stamped barrier (`SeekFlush` with
  its landed pts, `SeekRejected` when it has no seek surface), so
  every track observes exactly one barrier per dispatched generation.
  A command addressing a stream a source doesn't route retargets that
  source at its first routed stream with the pts rescaled into that
  stream's time base (`resolve_seek_target`, unit-tested for the
  passthrough / rescale / defensive arms), landing every source on the
  same presentation instant. Pinned by `tests/seek_multi_source.rs`:
  two seekable URIs both flush at the target with one generation, and
  a seekable + unseekable mix answers one `SeekFlush` + one
  `SeekRejected` (same generation) with the stream still flowing
  after.

- Hostile/edge coverage for the two contracts above: two parallel
  outputs sharing ONE source URI (independent pumps, both deliver in
  full), the tightest channel caps + a tiny byte ceiling propagating
  to every output across waves, `spawn()`'s single-output rejection
  pinned by message, and rapid multi-source seek bursts (every
  generation produces exactly one barrier per routed source — no
  coalescing, no duplicates).

### Changed

- The executor's thread-budget autodetect goes through
  `ExecutionContext::auto()` instead of querying host parallelism
  directly — `ExecutionContext` is the framework's single threading
  authority, and every downstream clamp (codec fan-out, the new
  multi-output wave width) derives from the budget it resolves.

### Fixed

- The serial path (`threads == 1` / single-thread `run`) no longer
  pumps sources that belong to the job's OTHER outputs. The source
  probe opens every source leaf in the document, but `run_output`'s
  pump loop then drained every opened source to EOF — on a
  multi-output job every output re-read every other output's source,
  inflating `ExecutorStats::packets_read` (2× on a two-output job) and
  doing dead demux work quadratic in the output count. The pipelined
  path has always dropped route-less pumps in `prepare_pipelined_run`;
  the serial path now retains only the URIs its own tracks reference,
  restoring serial/pipelined stats parity on multi-output jobs (pinned
  by `tests/multi_output_parallel.rs`).

- Error-propagation coherence pinned by `tests/error_propagation.rs`:
  a failing sink or a mid-stream source failure surfaces the original
  typed error from `Executor::run` on both executor paths — no hang,
  first-error-wins, serial delivers all pre-failure frames, pipelined
  trades in-flight frames (bounded by channel depth) for prompt
  teardown.
- Criterion micro-benchmarks (`benches/graph.rs`) over the
  graph-resolution cold path: parse / validate / to_dag on wide (64
  tracks), deep (64 nested filters), and alias-chained (64 links)
  synthetic jobs, plus `Dag::describe` and `TrackInput::walk` / `leaf`.
- The staged (pipelined) runner now drives packet-shape and
  frame-shape sources natively. Bytes-shape URIs keep their demuxer
  thread; packet-shape URIs (RTMP-style) get an identical packet-pump
  thread without the container layer; frame-shape URIs (generators,
  rendered scenes) get a frame-pump thread that feeds the per-track
  frame-stage chains directly — no demux, no decode stage.
  Consequences: `Executor::spawn` (the playback path — live seek /
  progress / abort handle) now works over typed sources instead of
  failing with an internal "opener returned non-bytes shape" error,
  and `Executor::run` with `threads ≥ 2` no longer silently degrades
  typed-source jobs to the serial path. Seeks dispatched against a
  typed source surface `BarrierKind::SeekRejected` with the dispatch
  generation (`PacketSource` / `FrameSource` have no seek surface) and
  the stream keeps flowing. Serial/pipelined stats parity for typed
  sources is pinned by `tests/typed_source_staged.rs`.
- Validation completeness: `Job::validate` now rejects an explicitly
  empty / whitespace `codec` string with the track context (previously
  it survived to codec resolution and failed with an opaque "no codec
  registered" message), and rejects a `stream_selector` whose `kind`
  contradicts the typed track list it appears in (`video` selector
  inside `audio: [...]` — the selector kind won at DAG-build and muxed
  a mislabeled stream). Kind-free selectors in typed buckets and
  arbitrary selector kinds inside `all:` stay legal. `Job::from_json`
  additionally rejects the bare `@` top-level key (an alias whose name
  is empty after the sigil is unreferencable).

### Fixed

- `TrackInput` deserialization is now key-directed (pick the variant
  by which of `from` / `convert` / `render3d` / `filter` is present,
  in the historical priority order) instead of `#[serde(untagged)]`.
  The untagged derive re-tried every candidate variant at every
  nesting level of the recursive `input` field, making parse cost
  grow exponentially with filter-chain depth — a 64-deep chain
  effectively never finished parsing (found by the new `deep-64`
  bench, which ran 15+ minutes without completing). Key-directed
  dispatch parses the same chain in microseconds; a missing
  discriminator now reports the four expected keys instead of
  serde's generic "did not match any variant". Wire format unchanged.
- `all:` track fan-out now pins each duplicate's selector to a
  `(kind, per-kind ordinal)` pair. Pre-fix the expansion left the
  ordinal as `index: None`, so a source with two streams of the SAME
  media type (dual-audio, dual-subtitle, …) resolved BOTH duplicates
  to the first matching stream — the sink received two copies of
  stream 0 and the second stream's data silently vanished from the
  output on both the serial and pipelined paths. The two fan-out
  helpers (`expand_all_tracks` / `expand_all_tracks_pump`) now share
  one implementation. Pinned by a dual-audio stub demuxer whose
  per-stream payload fill bytes let the test attribute every delivered
  frame to its source stream (`tests/all_tracks_fan_out_same_kind.rs`,
  serial + pipelined).
- `Job::to_dag` now returns `Err(InvalidData)` on alias cycles instead
  of recursing without bound. The method's docs promised defensive
  validation, but a direct `to_dag` call on an unvalidated cyclic job
  (self-cycle, mutual cycle, or a cycle threaded through a filter
  wrapper) overflowed the stack during alias inlining. A per-track
  visiting stack in `build_source` rejects alias re-entry on the
  current descent path while still allowing legal diamond re-use of
  the same alias from different tracks.

## [0.1.11](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.10...v0.1.11) - 2026-06-09

### Other

- TrackInput typed accessors (kind_str / is_* / as_* / upstream / leaf / walk)
- drop release-plz.toml — use release-plz defaults across the workspace

### Added

- `TrackInput` typed accessors: `kind_str` (stable per-variant
  discriminator string), `is_source` / `is_filter` / `is_convert` /
  `is_render3d` predicates, `as_source` / `as_filter` / `as_convert` /
  `as_render3d` borrowing accessors, `upstream` (single-step wrapper
  descent), `leaf` (descend to terminal `Source` / `Render3D` node),
  and `walk` (outer-to-leaf visitor). Consumers building lints,
  diagnostics, or DAG transforms over a parsed `Job` no longer need
  to re-match the enum themselves. Purely additive — no existing
  variant / field / signature changed. Test count `schema::tests`
  12 → 21.

## [0.1.10](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.9...v0.1.10) - 2026-06-07

### Added

- TrackInput::Render3D — Phase C-3f schema bridge from Job to Render3D DAG

### Added

- `TrackInput::Render3D { source, backend, opts }` schema variant +
  `Render3DNode` payload — Phase C-3f schema bridge from `Job` to
  `DagNode::Render3D`. JSON shape:
  `{ "render3d": "<uri>", "backend": "<name>", "opts": { ... } }`.
  `Job::build_input` lowers this directly to the runtime DAG node;
  the executor still relies on the user-installed
  `RenderSourceFactory` (Phase C-3c) to instantiate the actual
  `FrameSource`. Unlocks consumer crates (e.g. oxideav-cli-convert)
  building Jobs that produce Render3D nodes via the standard
  `Job::to_dag()` path.

## [0.1.9](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.8...v0.1.9) - 2026-06-07

### Added

- render_source_factory callback — Phase C-3c wires Render3D execution

## [0.1.8](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.7...v0.1.8) - 2026-06-07

### Added

- DagNode::Render3D — Phase C-2 source variant for the 3D-render path

### Other

- surface copy-stage packets_copied via Progress
- surface encoder packets_encoded via Progress
- retry EOF Progress send to ride out backed-up receivers
- surface demuxer packets_read via Progress
- handle SourceOutput::MultiTitle + non-exhaustive wildcard
- surface decoder-skip count via Progress + ExecutorStats
- bump elapsed_micros drain + deadline timeouts for slow CI
- surface wall-clock progress via Progress::elapsed_micros
- surface in-flight packet bytes via Progress::queue_bytes
- memory-bounded packet queue (Executor::with_max_queue_bytes)
- configurable per-track channel-depth budget
- seek_with_generation returns the assigned generation
- extend BarrierKind::SeekFlush with landed_pts + time_base
- drain sink channel in background to unblock executor.finish
- graceful seek failure via BarrierKind::SeekRejected

## [0.1.6](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.5...v0.1.6) - 2026-05-05

### Other

- apply cargo fmt (rustfmt CI fix)
- move CodecPreferences + walker out of oxideav-core into pipeline

## [0.1.5](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.4...v0.1.5) - 2026-05-05

### Other

- multi-stream transcode_simple + CodecPreferences plumbing
- tolerate per-packet decoder errors instead of killing stream
- pipelined fallback panic on typed-source jobs (task #389)

### Fixed

- Executor: per-packet decoder errors no longer kill the entire stream. Both
  `run_decode_stage` (pipelined path, `staged.rs`) and `pump_packet` /
  `drain` (serial path, `executor.rs`) used to propagate any non-`NeedMore`
  / non-`Eof` `receive_frame` / `send_packet` error as a fatal `return
  Err(e)`, exiting the worker thread and starving every downstream sink.
  Real-world fallout: oxideplay sat at 00:00 on a real-world H.264/AAC mp4
  (`congress_mtgox_coins.mp4`) because the AAC decoder returned
  `invalid data: bitreader: out of bits` on the third packet — recoverable
  per the codec's own `pending.take()` semantics, but the executor took
  it as fatal and the audio clock never advanced. Decoders now follow the
  same logged-and-skipped policy the H.264 decoder already uses internally
  for per-slice errors. Regression coverage:
  `tests/decoder_error_tolerance.rs` exercises both serial and pipelined
  paths through a flaky stub decoder that errors on every 5th packet and
  asserts at least 35 of 50 frames still reach the sink (pre-fix: ~4).
- Executor: `run_output_pipelined` no longer panics with `Option::unwrap() on a
  None` when a typed-source job (e.g. `oxideav convert "xc:red" out.png` and
  every other `generate://` URI fed to the convert verb) falls back from the
  pipelined to the serial path. Previously the probe rewrote `Demuxer { source }`
  → `FrameSource { source }` in a clone of the DAG and handed *that* clone to
  `run_output`, but `run_output`'s own `resolve_source_shapes` only collects
  URIs from `Demuxer` nodes — so the second pass found none and
  `sources_by_uri.get(&pl.source_uri).unwrap()` blew up at executor.rs:269.
  The fix passes the *original* DAG (with its `Demuxer` leaves) to the
  fallback so `run_output` can re-discover and re-open the URIs itself.
  The `.unwrap()` is also rewritten as a descriptive `Error::invalid` so future
  resolver bugs surface as a normal error rather than a panic. Regression
  coverage in `tests/source_variants.rs` exercises both the FrameSource and
  PacketSource shapes through the pipelined path.

## [0.1.4](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.3...v0.1.4) - 2026-05-03

### Other

- bump oxideav-audio-filter dev-dep 0.0 -> 0.1
- replace never-match regex with semver_check = false

## [0.1.3](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.2...v0.1.3) - 2026-05-02

### Other

- migrate to centralized OxideAV/.github reusable workflows
- stay on 0.1.x during heavy dev (semver_check=false)
- branch on SourceOutput shape (Bytes/Packets/Frames)
- non-blocking round-robin so a slow track doesn't starve a fast one
- stream frames per-receive instead of drain-then-send
- adopt slim VideoFrame/AudioFrame shape
- pin release-plz to patch-only bumps

### Added

- DAG: `PacketSource` and `FrameSource` node variants matching the typed-source
  shapes in `oxideav-core::SourceOutput`. The executor probes every `Demuxer`
  leaf via `SourceRegistry::open`, then rewrites the node when the registry
  hands back `SourceOutput::Packets` / `SourceOutput::Frames` so downstream
  decode (for packets) or filter+sink (for frames) consumes the right shape
  without an intervening container demux or decoder.
- Executor: `SourcePump` enum + `open_source` / `resolve_source_shapes`
  helpers. `run_output` now branches per source shape — bytes goes through
  the historical demuxer chain, packet sources skip demux, frame sources
  skip both demux and decode and route directly into the filter chain (or
  the sink if no filter is declared).
- Test: `tests/source_variants.rs` exercises all three shapes end-to-end via
  in-tree mocks; each path produces the expected single audio frame.

### Changed

- Pipelined runner falls back to the serial path when any source resolves
  to a non-bytes shape. The staged-worker variants for `PacketSource` /
  `FrameSource` are tracked as follow-up work — RTMP and the generator
  both run fine on the serial path, which is the default for live playback
  on the typical thread budget.

## [0.1.2](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.1...v0.1.2) - 2026-04-25

### Other

- release v0.1.1

## [0.1.1](https://github.com/OxideAV/oxideav-pipeline/compare/v0.1.0...v0.1.1) - 2026-04-25

### Other

- drop oxideav-codec/oxideav-container shims, import from oxideav-core
- take RuntimeContext, drop separate filter registry + audio/image filter deps
- drop oxideav-basic dev-dep, use in-tree stub demuxer
- expand `all:` tracks to one runtime per source stream
- pipe input port into spectrogram factory
- auto-decode at @display / @out when no codec is declared
- drop JobSink Send super-bound, require Send only at sink-override site
- add seek-barrier flow + Executor::spawn / ExecutorHandle
- drop mux receivers before join to fix abort-path deadlock
- integration test for spectrogram multi-port output per plan § 5/10
- collapse executor to StreamFilter via FilterRegistry per plan § 6/7/8
- FilterRegistry + legacy adapters per plan § 2/3
- propagate video-filter output params (width/height/pixfmt) to encoder
- wire video filters (resize/blur/edge) via new image_filter feature
- bump oxideav-source dep to "0.1"
- absorb oxideav-job: JSON job graph + executor folded in
- release v0.0.3

## [0.1.0](https://github.com/OxideAV/oxideav-pipeline/compare/v0.0.3...v0.1.0) - 2026-04-19

### Other

- bump version to 0.1.0
