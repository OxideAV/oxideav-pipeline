//! Contract test for the `all:` track fan-out over a source with TWO
//! streams of the SAME media type.
//!
//! The DAG builder emits a single `MuxTrack` per `all:` entry (it can't
//! know the source's stream layout statically); the executor's prep
//! phase expands it into one `TrackRuntime` per source stream once the
//! source is open. The expansion must pin each duplicate's selector to
//! a distinct `(kind, per-kind ordinal)` pair — pre-fix the ordinal was
//! left as `index: None`, so BOTH duplicates of a dual-audio source
//! resolved to the first audio stream: the sink received two copies of
//! stream 0 and stream 1's data silently vanished from the output.
//!
//! The dual-stream stub demuxer (`tests/common/stub.rs`,
//! `MultiStreamStubDemuxer`) tags every packet's payload with a
//! per-stream fill byte (stream 0 → 0xAA, stream 1 → 0xBB) so the
//! observer sink can attribute every delivered frame to its source
//! stream by content alone. Both executor paths are pinned: serial
//! (`threads = 1`) and pipelined (`threads = 2`).

mod common;

use std::sync::{Arc, Mutex};

use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{Executor, Job, JobSink};

/// Per-fill-byte frame counts + the stream layout announced at start.
#[derive(Default)]
struct Observed {
    frames_s0: usize,
    frames_s1: usize,
    frames_other: usize,
    start_streams: usize,
}

struct ObserverSink {
    shared: Arc<Mutex<Observed>>,
}

impl JobSink for ObserverSink {
    fn start(&mut self, streams: &[StreamInfo]) -> Result<()> {
        self.shared.lock().unwrap().start_streams = streams.len();
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, frm: &Frame) -> Result<()> {
        let mut s = self.shared.lock().unwrap();
        let first_byte = match frm {
            Frame::Audio(a) => a.data.first().and_then(|p| p.first()).copied(),
            _ => None,
        };
        match first_byte {
            Some(common::stub::MULTI_FILL_S0) => s.frames_s0 += 1,
            Some(common::stub::MULTI_FILL_S1) => s.frames_s1 += 1,
            _ => s.frames_other += 1,
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

fn run_all_track_job(name: &str, threads: usize) -> Observed {
    let src = common::stub::touch_multi(name);

    let mut ctx = oxideav_core::RuntimeContext::new();
    common::stub::register(&mut ctx.codecs, &mut ctx.containers);
    oxideav_source::register(&mut ctx);

    // `@display` is a playback sink: an `all:` track without a codec
    // gets an auto-inserted decode stage, so the sink observes decoded
    // frames whose payload bytes are the demuxer's per-stream fill.
    let job_json = format!(
        r#"{{"@display": {{"all": [{{"from": "{}"}}]}}}}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Job::from_json(&job_json).expect("parse job");

    let shared = Arc::new(Mutex::new(Observed::default()));
    let sink = Box::new(ObserverSink {
        shared: shared.clone(),
    });
    Executor::new(&job, &ctx)
        .with_threads(threads)
        .with_sink_override("@display", sink)
        .run()
        .expect("executor run");

    let s = shared.lock().unwrap();
    Observed {
        frames_s0: s.frames_s0,
        frames_s1: s.frames_s1,
        frames_other: s.frames_other,
        start_streams: s.start_streams,
    }
}

fn assert_both_streams_delivered(o: &Observed, label: &str) {
    let per_stream = common::stub::MULTI_PACKETS_PER_STREAM as usize;
    assert_eq!(
        o.start_streams, 2,
        "{label}: expected the expanded track list to announce 2 output \
         streams, got {}",
        o.start_streams
    );
    assert_eq!(
        o.frames_other, 0,
        "{label}: {} frames carried neither stream's fill byte",
        o.frames_other
    );
    assert_eq!(
        o.frames_s0, per_stream,
        "{label}: stream 0 should deliver exactly {per_stream} frames, \
         got {} (a doubled count means both fan-out duplicates selected \
         the same source stream)",
        o.frames_s0
    );
    assert_eq!(
        o.frames_s1, per_stream,
        "{label}: stream 1 should deliver exactly {per_stream} frames, \
         got {} (0 means the second same-kind stream was silently \
         replaced by a copy of the first)",
        o.frames_s1
    );
}

#[test]
fn all_track_fans_out_two_audio_streams_serial() {
    let o = run_all_track_job("fanout_serial", 1);
    assert_both_streams_delivered(&o, "serial");
}

#[test]
fn all_track_fans_out_two_audio_streams_pipelined() {
    let o = run_all_track_job("fanout_pipelined", 2);
    assert_both_streams_delivered(&o, "pipelined");
}
