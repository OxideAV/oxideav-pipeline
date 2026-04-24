//! Integration test for plan § 4/7/8: dropping a `spectrogram` filter
//! onto an audio-only source must cause the executor to synthesise a
//! video [`StreamInfo`] on top of the existing audio stream, and to
//! route video frames emitted on the filter's second port straight to
//! the sink.
//!
//! The test fabricates a 1-second 440 Hz mono WAV, runs it through a
//! `@display`-style reserved sink override implemented here, and
//! asserts that:
//!
//! 1. `sink.start(streams)` saw both an audio stream **and** a video
//!    stream whose params come from the spectrogram filter's video
//!    output port.
//! 2. After the run, the sink received at least 10 video frames
//!    (1 s × 30 fps expected around 30, but the exact count depends
//!    on decoder chunking, so we require only ≥ 10).

use std::io::Write;
use std::sync::{Arc, Mutex};

use oxideav_codec::CodecRegistry;
use oxideav_container::ContainerRegistry;
use oxideav_core::{Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_pipeline::{Executor, Job, JobSink};
use oxideav_source::SourceRegistry;

fn build_sine_wav(path: &std::path::Path, sample_rate: u32, ms: u32, freq: f32) {
    // Minimal RIFF/WAV with 16-bit mono PCM carrying a sine wave.
    let n_samples = (sample_rate as u64 * ms as u64 / 1000) as u32;
    let byte_rate = sample_rate * 2;
    let data_sz = n_samples * 2;
    let riff_sz = 36 + data_sz;
    let mut f = std::fs::File::create(path).unwrap();
    f.write_all(b"RIFF").unwrap();
    f.write_all(&riff_sz.to_le_bytes()).unwrap();
    f.write_all(b"WAVE").unwrap();
    f.write_all(b"fmt ").unwrap();
    f.write_all(&16u32.to_le_bytes()).unwrap();
    f.write_all(&1u16.to_le_bytes()).unwrap(); // PCM
    f.write_all(&1u16.to_le_bytes()).unwrap(); // mono
    f.write_all(&sample_rate.to_le_bytes()).unwrap();
    f.write_all(&byte_rate.to_le_bytes()).unwrap();
    f.write_all(&2u16.to_le_bytes()).unwrap(); // block align
    f.write_all(&16u16.to_le_bytes()).unwrap(); // bits per sample
    f.write_all(b"data").unwrap();
    f.write_all(&data_sz.to_le_bytes()).unwrap();
    for i in 0..n_samples {
        let t = i as f32 / sample_rate as f32;
        let s = (2.0 * std::f32::consts::PI * freq * t).sin();
        let v = (s * 0.5 * i16::MAX as f32) as i16;
        f.write_all(&v.to_le_bytes()).unwrap();
    }
}

fn registries() -> (CodecRegistry, ContainerRegistry) {
    let mut codecs = CodecRegistry::new();
    let mut containers = ContainerRegistry::new();
    oxideav_basic::register_codecs(&mut codecs);
    oxideav_basic::register_containers(&mut containers);
    (codecs, containers)
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("oxideav_pipeline_{name}"));
    let _ = std::fs::create_dir_all(&p);
    p
}

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
    // 1-second 440 Hz sine at 44.1 kHz mono — matches the
    // `sine_frame` pattern in the audio-filter unit tests, just
    // encapsulated in a WAV container so the demuxer can handle it.
    let dir = tmp_dir("spectrogram_vid");
    let src = dir.join("sine.wav");
    build_sine_wav(&src, 44_100, 1_000, 440.0);

    let (codecs, containers) = registries();
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

    // 2. The run produced at least 10 video frames (≈ 30 at 1 s × 30
    //    fps, but the actual count depends on decoder chunking).
    assert!(
        state.video_frames >= 10,
        "expected ≥ 10 spectrogram video frames, got {}",
        state.video_frames
    );
    // Audio passthrough should also have fired at least once.
    assert!(state.audio_frames > 0, "no audio passthrough frames");
}
