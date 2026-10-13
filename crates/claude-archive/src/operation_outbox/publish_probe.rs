//! A seam that lets a test stop the publication pass at a named point and fail one commit.
//!
//! Two clones share one set of signals: the pass announces that it reached a pause and then waits
//! to be released, so a test can run a deletion in exactly that window with two real connections.
//! The pattern is that of `privacy_deletion::EraseGate`. Production code never builds one
//! (XR-021 CONTRACTS.md R6-03 rule 7).

use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// A point at which the publication pass can be held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishPause {
    /// The candidates are chosen and no row is locked yet.
    AfterCandidates,
    /// The broker acknowledged a row; the mark is not written and nothing is committed.
    AfterPubAck,
}

#[derive(Debug, Default)]
struct Pause {
    armed: AtomicBool,
    reached: Notify,
    release: Notify,
}

/// The test seam of [`crate::OutboxPublisher`].
#[derive(Debug, Default)]
pub struct PublishProbe {
    after_candidates: Pause,
    after_pub_ack: Pause,
    fail_commit: AtomicBool,
}

impl PublishProbe {
    /// Creates a probe that pauses nowhere and fails nothing until a test arms it.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Holds the next pass that reaches `at`, once.
    pub fn pause_at(&self, at: PublishPause) {
        self.pause(at).armed.store(true, Ordering::Release);
    }

    /// Resolves once a pass is held at `at`.
    pub async fn reached(&self, at: PublishPause) {
        self.pause(at).reached.notified().await;
    }

    /// Lets a held pass continue.
    pub fn release(&self, at: PublishPause) {
        self.pause(at).release.notify_one();
    }

    /// Makes the next first publication fail its commit after the broker acknowledged it, as a
    /// connection that dropped at that moment would.
    pub fn fail_commit_once(&self) {
        self.fail_commit.store(true, Ordering::Release);
    }

    fn pause(&self, at: PublishPause) -> &Pause {
        match at {
            PublishPause::AfterCandidates => &self.after_candidates,
            PublishPause::AfterPubAck => &self.after_pub_ack,
        }
    }

    pub(super) async fn park(&self, at: PublishPause) {
        let pause = self.pause(at);
        if pause.armed.swap(false, Ordering::AcqRel) {
            pause.reached.notify_one();
            pause.release.notified().await;
        }
    }

    pub(super) fn take_commit_fault(&self) -> bool {
        self.fail_commit.swap(false, Ordering::AcqRel)
    }
}
