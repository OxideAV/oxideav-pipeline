# oxideav-pipeline

Pipeline composition for oxideav

Part of the [oxideav](https://github.com/OxideAV/oxideav-workspace) framework — a pure-Rust media transcoding and streaming stack. Codec, container, and filter crates are implemented from the spec (no C codec libraries linked or wrapped, no `*-sys` crates). Optional hardware-engine crates (`oxideav-videotoolbox` / `-audiotoolbox` / `-vaapi` / `-vdpau` / `-nvidia` / `-vulkan-video`) bridge to OS APIs via runtime `libloading`; pass `--no-hwaccel` (or omit the `hwaccel` feature) to opt out.

## Source shapes

The DAG executor branches on the [`SourceOutput`](https://docs.rs/oxideav-core)
returned by `SourceRegistry::open`:

- **Bytes** (file, http) — open container, demux, decode, encode, mux. The
  historical path; unchanged.
- **Packets** (rtmp, future srt/rtsp) — skip the container layer; pull packets
  directly from the source and feed the decoder.
- **Frames** (synthetic generators, future capture-card drivers) — skip both
  demux and decode; frames flow straight into the filter chain (or the sink
  if no filter is declared).

The shape is decided by the driver at registration time; jobs reference URIs
identically across the three.

## Per-packet decoder error tolerance

A decoder error on a single packet (e.g. an AAC frame with a recoverable
bit-stream glitch) is logged + skipped, not propagated as a fatal stream
failure. The next packet flows through normally. Same model the H.264
decoder uses internally for per-slice errors. This matches what real-world
media playback expects — a corrupt frame mid-stream should mean a single
skipped frame, not a wedged player. See `tests/decoder_error_tolerance.rs`
for the contract test.

## Seek-barrier payload

`BarrierKind::SeekFlush` carries the demuxer's actual `landed_pts` plus
the matching `time_base`, so consumers re-anchor at the precise landing
position (typically the largest keyframe ≤ requested target) instead of
guessing from the next packet's pts. Pre-fix the engine had to wait for
the first post-barrier audio frame and re-anchor there — typically 50-
200 ms off because video lands on a keyframe (≤ target) while audio
lands on the next packet (≥ target). With the payload extension,
position display reads the new position the instant the barrier fires.
See `tests/seek_flush_carries_landed_pts.rs`.

## Channel-depth budget

`Executor::with_channel_caps(ChannelCaps { packets, frames })` overrides
the per-track packet- and frame-channel depth for the pipelined runner.
Defaults are 16 packets / 8 frames — back-pressures a stalled consumer
before memory blows up, large enough to amortise the channel mutex cost
on each send.

Memory upper bound per output (rough):

```text
N_tracks * (packets * packet_size + frames * frame_size)
```

Tight (`{ packets: 1, frames: 1 }`) is the smallest legal depth — every
queue degenerates to one element so the source thread blocks until the
consumer has drained. Useful for embedded playback. Loosened
(`{ packets: 64, frames: 32 }`) lets bursty decoders coast on the queue
depth instead of blocking on the encoder — useful for high-throughput
offline transcodes. Zero is clamped up to one (`sync_channel(0)` is a
rendezvous channel and would serialise the entire pipeline). The serial
path uses no channels and ignores the cap. See
`tests/channel_caps.rs`.

## Memory-bounded packet queue

`Executor::with_max_queue_bytes(n)` adds an orthogonal *byte* ceiling on
the demuxer→worker packet queues. The channel-depth caps bound those
queues by element count — fine when packet sizes are uniform, but a
single outsized packet (a tracker module delivered whole in one packet,
a 4K intra keyframe, a JPEG-2000 codestream) can be megabytes on its
own, so the count cap alone lets resident memory swing widely with
content. The byte ceiling makes the demuxer park before reading the
next packet while the aggregate in-flight packet bytes are at or above
`n`; the consuming stage (copy or decode) frees the bytes the instant
it pulls the packet off the channel.

The two knobs compose — whichever binds first applies. A lone packet
larger than `n` is still admitted (the demuxer crosses the line by one
packet rather than deadlock on a packet bigger than the whole budget),
so `n` is a soft target, not a hard never-exceed. `0` (the default)
disables the ceiling entirely, leaving only the count caps; the serial
path ignores it. See `tests/max_queue_bytes.rs`.

## Back-pressure visibility

`ExecutorHandle::try_progress()` returns a `Progress { pts, frames, eof,
queue_bytes, elapsed_micros }`. `queue_bytes` reports the current
in-flight packet-byte total tracked by the `with_max_queue_bytes(n)`
budget — a value pinned to `n` indicates the demuxer is parking on the
byte ceiling waiting for the consumer to drain, a value hovering near
zero means the ceiling isn't binding (the count caps or downstream
block first). When no ceiling is configured the field is always `0`
(the budget short-circuits its accounting); at EOF the field returns
to `0` because every admitted packet has a matching release by the
time workers join. See `tests/progress_reports_queue_bytes.rs`.

## Wall-clock progress

`elapsed_micros` reports the monotonic wall-clock microseconds since
the pipelined runner's baseline `Instant`, captured just before the
first worker thread spawns. Engines use this to derive the realtime
ratio (`pts_micros / elapsed_micros`) without bracketing
`spawn()`/`stop()` with their own `Instant::now()`, to detect when a
live source is outpacing the pipeline (`pts < elapsed_micros` and
falling further behind every poll), and to print the EOF wall-clock
total from the `eof: true` progress event. The serial path
(`Executor::run`) doesn't wire a progress channel and never emits
`Progress`, so the field is only ever non-zero on the pipelined
runner reached via `Executor::spawn`. Values are guaranteed
non-decreasing across consecutive emissions because `Instant::elapsed`
is monotonic. See `tests/progress_reports_elapsed_micros.rs`.

## Decoder-skip visibility

`Progress::packets_skipped` and `ExecutorStats::packets_skipped`
expose the same cumulative count of packets the decoder swallowed
under the per-packet error-tolerance contract (see the
"Per-packet decoder error tolerance" section above). Pre-fix the
only signal that a stream was quietly going bad was an `eprintln!`
line on stderr — an engine had no programmatic way to display "N
decode errors" on its status bar, and a stress harness had to
reverse-engineer the skip count from `packets_read - frames_decoded`.
Both decode paths (`staged::run_decode_stage` and the inherent
`executor::pump_packet`) increment a shared counter on every
`send_packet` error and on every `receive_frame` error that landed
before a frame was produced for that packet; a `produced_any` flag
prevents double-counting when a `receive_frame` error fires *after*
partial output already streamed (the packet wasn't lost — partial
output landed). The counter is monotonically non-decreasing, equals
`0` on a clean stream and on copy-only outputs (no decoder
instantiated), and the EOF `Progress` emission's value always
matches the final `ExecutorStats` value (both read from the same
atomic; EOF emission is sent after the worker threads join). See
`tests/progress_reports_packets_skipped.rs` plus the two new stats
assertions in `tests/decoder_error_tolerance.rs`.

## Seek correlation

`ExecutorHandle::seek_with_generation(stream_idx, pts, tb)` returns the
`generation: u32` the handle assigned to the dispatch; the resulting
`BarrierKind::SeekFlush { generation, .. }` / `SeekRejected { generation }`
carries the exact same value. Engines that need to ignore stale
pre-seek payloads (or detect a missed seek across a burst — holding
`→` during scrubbing fires several seeks per second) compare against
the returned value rather than maintaining a parallel mirror counter
that could silently desync with the demuxer's. The shorter
`seek(...) -> Result<()>` form is retained as a discard wrapper for
callers that don't need correlation. See
`tests/seek_with_generation.rs`.

## Usage

```toml
[dependencies]
oxideav-pipeline = "0.0"
```

## License

MIT — see [LICENSE](LICENSE).
