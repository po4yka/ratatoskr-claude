//! The telemetry contract: the configured level gates what is emitted.
//!
//! Harness helpers run outside `#[test]` bodies, so the suite-wide test
//! allowances do not reach them; this file states that once instead of
//! scattering per-function expectations.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]
#![expect(
    clippy::unwrap_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::sync::{Arc, Mutex};

use ratatoskr_claude_archive::telemetry::build_filter;

/// A shared in-memory writer capturing what the formatter produces.
#[derive(Clone)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Capture {
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Vec::new())))
    }

    fn snapshot(&self) -> String {
        let guard = self.0.lock().unwrap();
        String::from_utf8(guard.clone()).expect("captured logs are UTF-8")
    }
}

struct CaptureWriter<'a>(&'a Capture);

impl std::io::Write for CaptureWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.0.lock().unwrap().write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.0.lock().unwrap().flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = CaptureWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        CaptureWriter(self)
    }
}

/// Builds a scoped subscriber whose JSON output lands in the capture, filtered
/// by the expression under test through the production constructor.
fn capture_for(filter_expression: &str) -> (tracing::Dispatch, Capture) {
    let filter = build_filter(filter_expression).expect("the filter expression parses");
    let capture = Capture::new();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_writer(capture.clone())
        .finish();
    (tracing::Dispatch::new(subscriber), capture)
}

#[test]
fn filter_honors_configured_level() {
    let (dispatch, capture) = capture_for("info");
    tracing::dispatcher::with_default(&dispatch, || {
        tracing::debug!("below the configured level");
    });
    assert!(
        !capture.snapshot().contains("below the configured level"),
        "an event below the configured level is not emitted"
    );

    tracing::dispatcher::with_default(&dispatch, || {
        tracing::info!("at the configured level");
    });
    assert!(
        capture.snapshot().contains("at the configured level"),
        "an event at the configured level is emitted"
    );

    let (debug_dispatch, debug_capture) = capture_for("debug");
    tracing::dispatcher::with_default(&debug_dispatch, || {
        tracing::debug!("raised to the configured level");
    });
    assert!(
        debug_capture
            .snapshot()
            .contains("raised to the configured level"),
        "the same event is emitted once the level admits it"
    );
}
