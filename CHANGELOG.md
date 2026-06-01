# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Progress::packets_read` — demuxer-progress visibility on the
  pipelined runner. The `Progress` event returned by
  `ExecutorHandle::try_progress()` now carries the cumulative count of
  packets the demuxer has read from the source, sampled live from the
  shared `PipelineCounters::packets_read` atomic on every emission.
  This mirrors `ExecutorStats::packets_read` but is sampled before EOF,
  so an engine can detect a stalled decoder while the run is still in
  flight: the demuxer keeps reading (`packets_read` climbs) but
  `frames` and `packets_skipped` stay flat, indicating the decode
  stage is wedged on a pathological packet rather than the source
  being slow. Headroom = `packets_read - frames - packets_skipped` is
  the count of demuxed packets the decode stage hasn't yet resolved
  (still in the queue, or pending inside the decoder waiting for more
  input); a value pinned at the channel-depth budget combined with a
  flat `frames` field is the diagnostic signature of a wedged
  decoder. Pre-r205 the only way to compute that was to maintain a
  parallel counter outside the executor — the `Progress` stream gave
  `frames` but not the demuxer's side of the same ratio. **Additive
  on `Progress`** — the new field defaults to `0` for any
  pattern-match consumer using the struct-update syntax
  (`Progress { pts, .. }`); the type doc-comment already documents
  that idiom for forward compatibility. Threaded through the two
  `Progress::try_send` call-sites in the mux loop alongside
  `queue_bytes` / `elapsed_micros` / `packets_skipped`. The serial
  path (`Executor::run`) doesn't wire a progress channel and never
  emits `Progress`, so the field is only ever non-zero on the
  pipelined runner reached via `Executor::spawn`. Regression coverage:
  `tests/progress_reports_packets_read.rs` asserts (a) `packets_read`
  is monotonically non-decreasing across consecutive emissions, (b)
  it is non-zero on the EOF event (catches a refactor that drops the
  thread-through), and (c) the EOF emission's value equals the final
  `ExecutorStats::packets_read` snapshot (both read from the same
  atomic; the EOF emission is sent after worker threads join, so any
  drift would mean the two paths are wired to different counters).
- `Progress::packets_skipped` and `ExecutorStats::packets_skipped` —
  cumulative count of packets the decoder swallowed under the
  per-packet error-tolerance contract pinned by
  `tests/decoder_error_tolerance.rs`. Pre-r184 the only signal that a
  decoder was log-and-skipping packets (`send_packet` errored, or the
  subsequent `receive_frame` errored before yielding a single frame)
  was an `eprintln!` line on stderr — an engine had no programmatic
  way to detect a stream that was quietly going bad, and a stress
  harness had to reverse-engineer the skip count from the gap between
  `packets_read` and `frames_decoded`. Both decode paths
  (`run_decode_stage` in `staged.rs` and `pump_packet` in
  `executor.rs`) now increment a shared counter — a thread-local
  `produced_any` flag suppresses double-counting on a `receive_frame`
  error that lands *after* one or more frames already streamed (the
  packet wasn't lost — partial output landed). The counter is read
  out on every `Progress` emission (mid-run and EOF) and on the final
  `ExecutorStats` snapshot at join time. **Additive on `Progress`** —
  the field defaults to `0` for any pattern-match consumer using the
  struct-update syntax (`Progress { pts, .. }`). **Additive on
  `ExecutorStats`** — `merge()` now sums the new field across outputs.
  Always `0` on copy-only outputs (no decoder is instantiated) and on
  a clean stream (no skips occurred). Regression coverage:
  `tests/progress_reports_packets_skipped.rs` asserts (a) the field is
  `0` end-to-end on a non-erroring decoder, (b) the counter is
  monotonically non-decreasing across consecutive emissions, and (c)
  the EOF emission's value equals the final `ExecutorStats` value
  (both read from the same atomic; the EOF emission is sent after the
  worker threads join, so any drift would mean the two paths are
  wired to different counters). `tests/decoder_error_tolerance.rs`
  adds two stats assertions: a `FlakyDecoder` that errors on every
  5th of 50 packets must report exactly 10 skips on both the serial
  and pipelined paths, and `frames + skipped` must equal the total
  packet count.
- `Progress::elapsed_micros` — wall-clock progress for the pipelined
  runner. The `Progress` event returned by
  `ExecutorHandle::try_progress()` now carries the wall-clock
  microseconds since the runner's baseline `Instant`, captured just
  before the first worker thread spawns (and therefore just before
  any packet starts flowing). Engines use this to derive three
  headline diagnostics without bracketing `spawn()`/`stop()` with
  their own `Instant::now()`: the realtime ratio
  (`pts_micros / elapsed_micros`) for a transcode-speed indicator,
  realtime-drift detection on live sources (the engine compares the
  latest `pts` to `elapsed_micros` to spot the pipeline falling behind
  the source clock before audio-ring drain surfaces it), and the EOF
  wall-clock total (the `eof: true` progress event carries the total
  runtime, so a CLI tool can print "encoded N frames in 4.21 s" by
  reading the field off the EOF event instead of wrapping the spawn).
  Values are guaranteed non-decreasing across consecutive emissions
  from the same handle (`Instant::elapsed` is monotonic). The serial
  path (`Executor::run`) doesn't wire a progress channel and never
  emits `Progress`, so the field is only ever non-zero on the
  pipelined runner reached via `Executor::spawn`. **Additive** — the
  field defaults to `0` for any pattern-match consumer using the
  struct-update syntax (`Progress { pts, .. }`); the type doc-comment
  already documents that idiom for forward compatibility. Threaded
  through the two `Progress::try_send` call-sites in the mux loop
  alongside `queue_bytes`. Regression coverage:
  `tests/progress_reports_elapsed_micros.rs` asserts (a)
  `elapsed_micros` is non-decreasing across consecutive emissions,
  (b) `elapsed_micros` is non-zero on the EOF event (a zero would
  mean the field isn't being populated), and (c) the peak mid-run
  sample is bounded above by the EOF sample (catches a refactor that
  reads two independent `started_at` baselines on the mid-run and EOF
  paths).
- `Progress::queue_bytes` — back-pressure visibility for the pipelined
  runner. The `Progress` event returned by
  `ExecutorHandle::try_progress()` now carries the current in-flight
  packet-byte total tracked by `Executor::with_max_queue_bytes(n)`'s
  shared accountant. A value pinned to the configured ceiling indicates
  the demuxer is parking on the byte budget waiting for the consumer
  to drain; a value hovering near zero means the ceiling isn't binding
  (the count caps or downstream block first). When no ceiling is
  configured (`with_max_queue_bytes(0)`, the default) the field is
  always `0` — the budget short-circuits its accounting and the
  demuxer never parks. At EOF the field returns to `0` because every
  admitted packet has a matching release by the time workers join.
  This gives engines a diagnostic surface for back-pressure without
  poking at private state: an operator can correlate audio-ring
  draining with the actual byte-budget pressure rather than guessing
  whether the demuxer or the encoder is the bottleneck. Threaded
  through the existing `QueueBudget::in_flight()` accessor + the two
  `Progress::try_send` call-sites in the mux loop. **Additive** — the
  new field defaults to `0` for any pattern-match consumer using the
  struct-update syntax (`Progress { pts, .. }`); the type doc-comment
  recommends that idiom for forward compatibility. Regression
  coverage: `tests/progress_reports_queue_bytes.rs` asserts (a) the
  field stays at `0` when no ceiling is configured (even under
  packet-flowing load — the accountant must short-circuit), (b) it
  returns to `0` at EOF (every admit has a matching release), and
  (c) it surfaces a non-zero value mid-run when back-pressure is
  actively engaged (a tight 4096-byte ceiling + a 5ms-per-write
  throttled sink).
- `Executor::with_max_queue_bytes(n)`. An orthogonal *byte* ceiling on
  the demuxer→worker packet queues for the pipelined runner.
  `with_channel_caps` bounds those queues by *element count* (at most
  `packets` packets per track), which is the right knob when packet
  sizes are uniform — but a single outsized packet (a tracker module
  delivered whole in one packet, a 4K intra keyframe, a JPEG-2000
  codestream) can be megabytes on its own, so the count cap alone lets
  resident memory swing widely with content. The byte ceiling makes the
  demuxer park before reading the next packet while the aggregate
  in-flight packet bytes are at or above `n`; the consuming stage (copy
  or decode) frees the bytes the instant it pulls the packet off the
  channel (a shared `AtomicU64` accounts admit/release). The two knobs
  compose — whichever binds first applies. A lone packet larger than
  `n` is still admitted (we cross the line by one packet rather than
  deadlock on a packet bigger than the whole budget), so `n` is a soft
  target. **Additive** — `0` (the default) disables the byte ceiling
  entirely: admit/release short-circuit and the demuxer never parks, so
  existing callers get unchanged behaviour, leaving only the count caps.
  The serial path uses no channels and ignores the ceiling. Threaded
  through `PreparedRun` / `PipelineControl` alongside `channel_caps`, so
  both `Executor::run` and `Executor::spawn` honour it. Regression
  coverage: `tests/max_queue_bytes.rs` asserts (a) a sub-packet ceiling
  (1 byte) runs to completion without deadlocking, (b) `0` matches an
  unconfigured run, (c) a tight ceiling preserves the exact payload
  count (no drops/dupes), and (d) the byte ceiling composes with the
  count caps; plus five `QueueBudget` unit tests for the admit/release/
  saturate/abort-bail accounting.
- `Executor::with_channel_caps(ChannelCaps { packets, frames })`. The
  pipelined staged executor's per-track packet- and frame-channel depth
  were previously hard-coded constants (`PACKET_CAP = 16`,
  `FRAME_CAP = 8`); the new builder exposes them to callers. Embedded
  playback can clamp to `{ packets: 1, frames: 1 }` (every queue
  degenerates to one element, source thread blocks until consumer has
  drained — smallest legal depth, since `sync_channel(0)` would be a
  rendezvous channel that serialises the entire pipeline);
  high-throughput offline transcodes can raise to e.g.
  `{ packets: 64, frames: 32 }` to let bursty decoders coast on the
  queue depth instead of blocking on the encoder. Zero is clamped up to
  one rather than panicking. The serial path uses no channels and
  ignores the cap. **Additive** — every existing caller (no
  `with_channel_caps()`) gets the historical depth via the new
  `Option<ChannelCaps>::None` plumbing through `PreparedRun` /
  `PipelineControl`. Regression coverage: `tests/channel_caps.rs`
  asserts (a) the tightest `{1, 1}` configuration runs to completion
  without deadlocking, (b) zero is clamped up to one (no panic from the
  underlying `sync_channel`), and (c) a default-via-None run processes
  the same payload count as a default-via-Some run.
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
