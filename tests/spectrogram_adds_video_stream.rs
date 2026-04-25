//! Integration test for plan § 4/7/8: dropping a `spectrogram` filter
//! onto an audio-only source must cause the executor to synthesise a
//! video [`StreamInfo`] on top of the existing audio stream, and to
//! route video frames emitted on the filter's second port straight to
//! the sink.
//!
//! Uses a stub demuxer/decoder (see `tests/common/stub.rs`) so this
//! test stays in `oxideav-pipeline` without dragging a real codec or
//! container into the crate's dev-deps.
//!
//! Asserts:
//! 1. `sink.start(streams)` saw both an audio stream **and** a video
//!    stream whose params come from the spectrogram filter's video
//!    output port.
//! 2. After the run, the sink received at least 10 video frames
//!    (60 s × 30 fps expected ~1800, but timing depends on decoder
//!    chunking, so we just gate on ≥ 10).

mod common;

use std::sync::{Arc, Mutex};

use oxideav_codec::CodecRegistry;
use oxideav_container::ContainerRegistry;
use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{Executor, Job, JobSink};
use oxideav_source::SourceRegistry;

/// Observer sink: captures the `StreamInfo` list passed to `start` and
/// every frame seen via `write_frame`, keyed by `MediaType`.
#[derive(Default)]
struct ObserverState {
    started_streams: Vec<StreamInfo>,
    audio_frames: usize,
    video_frames: usize,
    packets: usize,
}

struct ObserverSink {
    shared: Arc<Mutex<ObserverState>>,
}

impl JobSink for ObserverSink {
    fn start(&mut self, streams: &[StreamInfo]) -> Result<()> {
        let mut s = self.shared.lock().unwrap();
        s.started_streams = streams.to_vec();
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        self.shared.lock().unwrap().packets += 1;
        Ok(())
    }
    fn write_frame(&mut self, kind: MediaType, _frm: &Frame) -> Result<()> {
        let mut s = self.shared.lock().unwrap();
        match kind {
            MediaType::Audio => s.audio_frames += 1,
            MediaType::Video => s.video_frames += 1,
            _ => {}
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn spectrogram_adds_video_stream_to_display_output() {
    let src = common::stub::touch("spectrogram_vid");

    let mut codecs = CodecRegistry::new();
    let mut containers = ContainerRegistry::new();
    common::stub::register(&mut codecs, &mut containers);
    let sources = SourceRegistry::with_defaults();

    // `@display` is a reserved sink — our override observer picks up
    // the stream layout + frame counts.
    let job_json = format!(
        r#"{{
            "@in": {{"all": [{{"from": "{}"}}]}},
            "@display": {{
                "audio": [{{
                    "filter": "spectrogram",
                    "params": {{
                        "width": 128,
                        "height": 64,
                        "fft_size": 256,
                        "hop_size": 64,
                        "fps": 30
                    }},
                    "input": {{"from": "@in"}}
                }}]
            }}
        }}"#,
        src.display().to_string().replace('\\', "\\\\"),
    );
    let job = Job::from_json(&job_json).expect("parse job");

    let shared = Arc::new(Mutex::new(ObserverState::default()));
    let sink = Box::new(ObserverSink {
        shared: shared.clone(),
    });

    // Force the serial executor so extras routing is exercised on the
    // synchronous pump path; the pipelined path has its own coverage
    // via the parity tests but depends on the same machinery.
    Executor::new(&job, &codecs, &containers, &sources)
        .with_threads(1)
        .with_sink_override("@display", sink)
        .run()
        .expect("executor run");

    let state = shared.lock().unwrap();

    // 1. Both an audio and a video StreamInfo reached the sink.
    let kinds: Vec<MediaType> = state
        .started_streams
        .iter()
        .map(|s| s.params.media_type)
        .collect();
    assert!(
        kinds.contains(&MediaType::Audio),
        "sink.start never saw an audio stream: {kinds:?}"
    );
    assert!(
        kinds.contains(&MediaType::Video),
        "sink.start never saw a video stream — \
         spectrogram should have synthesised one: {kinds:?}"
    );

    // The synthesised video stream should reflect the spectrogram's
    // options (width/height and 30 fps time base).
    let video = state
        .started_streams
        .iter()
        .find(|s| s.params.media_type == MediaType::Video)
        .unwrap();
    assert_eq!(video.params.width, Some(128));
    assert_eq!(video.params.height, Some(64));

    // 2. The run produced at least 10 video frames.
    assert!(
        state.video_frames >= 10,
        "expected ≥ 10 spectrogram video frames, got {}",
        state.video_frames
    );
    // Audio passthrough should also have fired at least once.
    assert!(state.audio_frames > 0, "no audio passthrough frames");
}
