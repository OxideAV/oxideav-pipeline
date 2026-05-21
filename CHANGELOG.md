# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ExecutorHandle::seek_with_generation(stream_idx, pts, tb) -> Result<u32>`.
  The handle now owns the monotonic generation counter (previously
  private to the demuxer stage) and returns the assigned value to the
  caller. The demuxer copies it verbatim into the resulting
  `BarrierKind::SeekFlush` / `SeekRejected`, so callers can correlate
  their dispatches with the corresponding barriers without
  maintaining a parallel mirror counter that could silently desync.
  `SeekCmd` gains a `pub generation: u32` field to carry the value
  through the seek channel. Background: oxideplay's seek-pending
  bookkeeping kept a private `seek_gen_counter` that mirrored what
  the demuxer was about to do — a fragile arrangement where any
  dropped seek (channel saturated, executor torn down mid-press)
  silently desynchronised the two counters and the engine started
  treating fresh barriers as stale (or vice versa). With the
  generation returned from `seek_with_generation`, the engine just
  records the value the handle gave it and compares barriers
  one-to-one. The shorter `seek(...) -> Result<()>` form is retained
  as a discard wrapper for callers that don't need correlation, so
  this is **additive** — existing users (oxideplay's
  `apply_seek`) keep compiling unchanged. Regression coverage:
  `tests/seek_with_generation.rs` asserts (a) the first call returns
  `1` matching the prior demuxer-side counter contract, (b) the
  returned value equals the barrier's `generation` field, and (c) a
  burst of five back-to-back seeks produces barriers carrying the
  full set `{1, 2, 3, 4, 5}` with no duplicates.
- `BarrierKind::SeekRejected { generation }` variant. Emitted by the
  demuxer stage when `Demuxer::seek_to` returns an error (the typical
  case for a container that hasn't implemented seek yet, e.g. MP3 /
  MOV / AAC / AC3 before they grew dedicated impls). The pipeline now
  keeps reading from the pre-seek position instead of dying. Engines
  should match the new variant alongside `SeekFlush` and disable seek
  UI for the rest of the session on rejection. Reported by the user
  as "oxideplay can't seek on mp3 / any file" — the demuxer thread
  was propagating `seek_to`'s error and the executor exited silently.

### Changed

- Demuxer stage now calls `seek_to` first and broadcasts the matching
  barrier (Flush / Rejected) after, so workers can distinguish
  "landed seek — reset codec state" from "rejected seek — keep going".
- `BarrierKind::SeekFlush` now carries the demuxer's actual `landed_pts`
  (in the matching `time_base`) alongside `generation`. Pre-fix the
  barrier discarded `seek_to`'s `Ok(landed)` return value and the
  engine had to guess the post-seek anchor from "next audio packet's
  pts" — typically off by 50-200 ms because video lands on a keyframe
  (≤ target) while audio lands on the next packet (≥ target). With
  the payload extension, every consumer learns the exact landing
  atomically: the engine's master clock origin becomes `landed_pts`
  the instant the barrier fires, position display reads the new
  position immediately, and any subsequent A/V drift reflects only
  real-time elapsed since the seek. The barrier remains
  `Clone + Copy` because `TimeBase` is `Copy`. **API break for
  consumers that pattern-match on the variant** — most can switch to
  `SeekFlush { generation, .. }`; engines that re-anchor should
  capture the two new fields. Regression test:
  `tests/seek_flush_carries_landed_pts.rs` routes through a
  `FixedLandingStubDemuxer` whose `seek_to` returns `Ok(42)`
  regardless of the requested target; the test asserts the barrier
  surfaces `landed_pts = 42` end-to-end.



### Other

- reframe FFI claim — HW-engine crates use OS FFI by necessity
- drop needless return in system_info macos+linux branches
- iterate per device for HW backends with engine_probe set
- streaming progress events + system_info()
- per-codec throughput micro-benchmarks for `oxideav bench`

- bench: iterate per device for HW backends. Each `CodecImplementation`
  with an `engine_probe` set runs the bench loop once per probed device
  via `CodecParameters::with_device_index`. SW backends and HW backends
  without a probe keep their single-row behaviour. `BenchResult` gains
  `device_index: Option<u32>` and `device_label: Option<String>` so
  renderers can disambiguate the rows without re-running the probe.

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
