# oxideav-pipeline

Pipeline composition for oxideav

Part of the [oxideav](https://github.com/OxideAV/oxideav-workspace) framework — a
100% pure Rust media transcoding and streaming stack. No C libraries, no FFI
wrappers, no `*-sys` crates.

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

## Usage

```toml
[dependencies]
oxideav-pipeline = "0.0"
```

## License

MIT — see [LICENSE](LICENSE).
