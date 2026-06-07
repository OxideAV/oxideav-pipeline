//! Phase C-3f end-to-end integration test.
//!
//! Builds a [`Job`] from JSON whose track input is the new
//! [`TrackInput::Render3D`](oxideav_pipeline::TrackInput) variant, runs
//! it through [`Executor::run`] with a stub
//! [`RenderSourceFactory`](oxideav_pipeline::RenderSourceFactory)
//! installed, and asserts both that the factory was invoked with the
//! verbatim `(source, backend, opts)` triple from the Job JSON and that
//! the resulting [`Frame`] was delivered to the sink.
//!
//! This proves the full schema bridge: JSON → `Job::build_input` →
//! `DagNode::Render3D` → `resolve_source_shapes` → installed factory →
//! `SourcePump::Frames` → sink. The lower-level pieces are covered by
//! dedicated unit tests in `src/schema.rs`, `src/dag.rs`, and the
//! `Phase C-3c` test pair in `src/executor.rs`.
//!
//! The factory emits a single video frame then EOF; the sink counts
//! video frames so the test can assert "the frame reached the sink".

use std::sync::{Arc, Mutex};

use oxideav_core::{
    CodecId, CodecParameters, Error, Frame, FrameSource, MediaType, Packet, PixelFormat, Rational,
    Result, RuntimeContext, StreamInfo, VideoFrame, VideoPlane,
};
use oxideav_pipeline::{Executor, Job, JobSink, RenderSourceFactory};

/// 1×1 single-plane frame source. Emits exactly one `Frame::Video`,
/// then EOF.
struct OneVideoFrame {
    params: CodecParameters,
    emitted: bool,
}

impl OneVideoFrame {
    fn new() -> Self {
        let mut params = CodecParameters::video(CodecId::new("rawvideo"));
        params.width = Some(1);
        params.height = Some(1);
        params.pixel_format = Some(PixelFormat::Gray8);
        params.frame_rate = Some(Rational::new(1, 1));
        Self {
            params,
            emitted: false,
        }
    }
}

impl FrameSource for OneVideoFrame {
    fn params(&self) -> &CodecParameters {
        &self.params
    }
    fn next_frame(&mut self) -> Result<Frame> {
        if self.emitted {
            return Err(Error::Eof);
        }
        self.emitted = true;
        Ok(Frame::Video(VideoFrame {
            pts: Some(0),
            planes: vec![VideoPlane {
                stride: 1,
                data: vec![0u8],
            }],
        }))
    }
}

#[derive(Default)]
struct SinkCounters {
    video_frames: usize,
}

struct CountingSink {
    shared: Arc<Mutex<SinkCounters>>,
}

impl JobSink for CountingSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        Ok(())
    }
    fn write_frame(&mut self, kind: MediaType, _frm: &Frame) -> Result<()> {
        if matches!(kind, MediaType::Video) {
            self.shared.lock().unwrap().video_frames += 1;
        }
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

#[test]
fn render3d_job_json_drives_factory_and_sink_via_executor_run() {
    // The Job JSON carries the new TrackInput::Render3D variant. The
    // executor walks it, calls the factory with the exact (source,
    // backend, opts) triple, and routes the produced frame into the
    // sink.
    let job = Job::from_json(
        r#"{
            "@display": {
                "video": [{
                    "render3d": "scene.gltf",
                    "backend": "scanline",
                    "opts": {"width": 1, "height": 1}
                }]
            }
        }"#,
    )
    .expect("parse Job JSON containing TrackInput::Render3D");

    // Track every (source, backend, opts) the factory sees so the test
    // can assert it was invoked once with the verbatim Job arguments.
    let captured: Arc<Mutex<Vec<(String, String, serde_json::Value)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let captured_c = captured.clone();
    let factory: RenderSourceFactory = Box::new(move |s, b, o| {
        captured_c
            .lock()
            .unwrap()
            .push((s.to_string(), b.to_string(), o.clone()));
        Ok(Box::new(OneVideoFrame::new()) as Box<dyn FrameSource>)
    });

    let shared = Arc::new(Mutex::new(SinkCounters::default()));
    let sink = Box::new(CountingSink {
        shared: shared.clone(),
    });

    let ctx = RuntimeContext::new();
    Executor::new(&job, &ctx)
        // Force serial path so we don't need to argue about pipelined
        // FrameSource fallback semantics here — that lane is covered by
        // tests/source_variants.rs.
        .with_threads(1)
        .with_render_source_factory(factory)
        .with_sink_override("@display", sink)
        .run()
        .expect("Executor::run with Render3D + stub factory");

    // (a) factory was called exactly once with the verbatim Job triple
    let cap = captured.lock().unwrap();
    assert_eq!(
        cap.len(),
        1,
        "factory should be invoked once per Render3D node, got {}",
        cap.len()
    );
    let (s, b, o) = &cap[0];
    assert_eq!(s, "scene.gltf");
    assert_eq!(b, "scanline");
    assert_eq!(o["width"], 1);
    assert_eq!(o["height"], 1);

    // (b) the produced frame reached the sink
    let c = shared.lock().unwrap();
    assert_eq!(
        c.video_frames, 1,
        "expected 1 video frame routed from Render3D → sink, got {}",
        c.video_frames
    );
}

#[test]
fn render3d_job_json_without_factory_errors_pointing_at_install_api() {
    // No `with_render_source_factory` call → Executor::run must fail
    // with an Unsupported error naming the install API. This mirrors
    // the in-crate `render3d_without_factory_*` test but exercises the
    // schema → executor full path rather than the hand-built DAG.
    let job = Job::from_json(
        r#"{
            "@display": {
                "video": [{
                    "render3d": "scene.gltf",
                    "backend": "scanline"
                }]
            }
        }"#,
    )
    .unwrap();
    let shared = Arc::new(Mutex::new(SinkCounters::default()));
    let sink = Box::new(CountingSink {
        shared: shared.clone(),
    });
    let ctx = RuntimeContext::new();
    let err = Executor::new(&job, &ctx)
        .with_threads(1)
        .with_sink_override("@display", sink)
        .run()
        .expect_err("Render3D with no factory must fail");
    let msg = format!("{err}");
    assert!(
        msg.contains("with_render_source_factory"),
        "error must name the install API, got: {msg}"
    );
    assert!(
        matches!(err, Error::Unsupported(_)),
        "expected Error::Unsupported, got {err:?}"
    );
    // No frame should have reached the sink.
    assert_eq!(shared.lock().unwrap().video_frames, 0);
}
