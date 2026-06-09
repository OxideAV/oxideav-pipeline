# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
