//! Thread budgets for the codecs the framework builds on the caller's
//! behalf.
//!
//! `oxideav-core`'s threading contract is opt-in: a codec runs serial
//! until [`Decoder::set_execution_context`] /
//! [`Encoder::set_execution_context`] hands it a budget. Every helper
//! in this crate that constructs a codec ([`crate::Pipeline`],
//! [`crate::transcode_simple`] and friends, the [`crate::Executor`])
//! therefore announces one before the first packet / frame, defaulting
//! to [`ExecutionContext::auto`] (every core the host reports).
//!
//! # Splitting one budget across streams
//!
//! [`stream_budget`] is the rule every driver applies:
//!
//! * a **video** codec gets the whole budget — pictures are where the
//!   work is, and codecs bound their own fan-out with
//!   [`ExecutionContext::effective_workers`];
//! * an **audio / subtitle / data** codec gets the whole budget only
//!   when the run carries no video; next to a video stream it runs
//!   serial (its per-frame work is tiny, and a worker pool there only
//!   competes with the picture codec for cores).
//!
//! A budget of 1 is serial everywhere, which is how callers ask for
//! single-threaded, deterministic-scheduling runs. Output never
//! depends on the budget: codecs must produce identical results at
//! every thread count.
//!
//! [`Decoder::set_execution_context`]: oxideav_core::Decoder::set_execution_context
//! [`Encoder::set_execution_context`]: oxideav_core::Encoder::set_execution_context

use oxideav_core::{ExecutionContext, MediaType};

/// The budget for a thread-count knob: `0` = [`ExecutionContext::auto`],
/// `1` = serial, `n` = at most `n` workers.
pub fn budget_from_threads(threads: usize) -> ExecutionContext {
    if threads == 0 {
        ExecutionContext::auto()
    } else {
        ExecutionContext::with_threads(threads)
    }
}

/// The share of `budget` a codec of `media_type` gets in a run that
/// does (`run_has_video`) or does not carry a video stream. See the
/// [module docs](self) for the rule.
pub fn stream_budget(
    budget: &ExecutionContext,
    media_type: MediaType,
    run_has_video: bool,
) -> ExecutionContext {
    if media_type == MediaType::Video || !run_has_video {
        budget.clone()
    } else {
        ExecutionContext::serial()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_threads_is_auto_one_is_serial() {
        assert_eq!(
            budget_from_threads(0).threads,
            ExecutionContext::auto().threads
        );
        assert_eq!(budget_from_threads(1).threads, 1);
        assert_eq!(budget_from_threads(6).threads, 6);
    }

    #[test]
    fn video_keeps_the_budget_audio_yields_to_video() {
        let b = ExecutionContext::with_threads(8);
        assert_eq!(stream_budget(&b, MediaType::Video, true).threads, 8);
        assert_eq!(stream_budget(&b, MediaType::Audio, true).threads, 1);
        assert_eq!(stream_budget(&b, MediaType::Audio, false).threads, 8);
        assert_eq!(stream_budget(&b, MediaType::Subtitle, true).threads, 1);
    }
}
