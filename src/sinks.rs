//! Built-in [`JobSink`](crate::JobSink) implementations.
//!
//! - [`NullSink`] — accepts and discards everything; useful for dry runs
//!   and tests.
//! - [`FileSink`] — opens a muxer via the container registry and writes
//!   packets to a file; frames are rejected (the executor is expected to
//!   insert an Encode node before it hits this sink).

use std::path::PathBuf;

use oxideav_core::{Error, Frame, MediaType, Packet, Result, StreamInfo};
use oxideav_core::{Muxer, WriteSeek};

use crate::executor::JobSink;

/// Discarding sink. Keeps a counter of packets/frames received for test
/// assertions.
#[derive(Default)]
pub struct NullSink {
    pub packets: u64,
    pub frames: u64,
}

impl NullSink {
    pub fn new() -> Self {
        Self::default()
    }
}

impl JobSink for NullSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, _pkt: &Packet) -> Result<()> {
        self.packets += 1;
        Ok(())
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        self.frames += 1;
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// File-backed sink. The executor is responsible for opening the muxer
/// (since it knows the output stream parameters); this struct takes that
/// muxer + the output path for diagnostics.
///
/// The muxer lives in an `Option` so [`FileSink::abandon`] can drop it
/// (closing its file handle) before unlinking the partial output — on
/// platforms where an open handle blocks deletion the drop must come
/// first. Every other hook errors with a descriptive message if it is
/// called after `abandon`.
pub struct FileSink {
    pub path: PathBuf,
    muxer: Option<Box<dyn Muxer>>,
    header_written: bool,
}

impl FileSink {
    pub fn new(path: PathBuf, muxer: Box<dyn Muxer>) -> Self {
        Self {
            path,
            muxer: Some(muxer),
            header_written: false,
        }
    }

    fn muxer_mut(&mut self) -> Result<&mut Box<dyn Muxer>> {
        self.muxer
            .as_mut()
            .ok_or_else(|| Error::other("FileSink: output was abandoned"))
    }
}

impl JobSink for FileSink {
    fn start(&mut self, _streams: &[StreamInfo]) -> Result<()> {
        if !self.header_written {
            self.muxer_mut()?.write_header()?;
            self.header_written = true;
        }
        Ok(())
    }
    fn write_packet(&mut self, _kind: MediaType, pkt: &Packet) -> Result<()> {
        if !self.header_written {
            self.muxer_mut()?.write_header()?;
            self.header_written = true;
        }
        self.muxer_mut()?.write_packet(pkt)
    }
    fn write_frame(&mut self, _kind: MediaType, _frm: &Frame) -> Result<()> {
        Err(Error::other(
            "FileSink expects packets, not frames; add a codec to the track",
        ))
    }
    fn finish(&mut self) -> Result<()> {
        if !self.header_written {
            self.muxer_mut()?.write_header()?;
            self.header_written = true;
        }
        self.muxer_mut()?.write_trailer()
    }
    /// Failed-output disposal: drop the muxer (closing the underlying
    /// file handle) and unlink the partially-written file. Idempotent —
    /// a second call (or an already-missing file) reports `Ok`.
    fn abandon(&mut self) -> Result<()> {
        self.muxer = None;
        match std::fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// Convenience: hold an output file handle until the muxer owns it.
pub fn open_file_write(path: &std::path::Path) -> Result<Box<dyn WriteSeek>> {
    let f = std::fs::File::create(path)?;
    Ok(Box::new(f))
}
