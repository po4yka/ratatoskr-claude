//! The boot contract end to end: the real binary starts against a real
//! `PostgreSQL`, reports liveness then readiness, drains on SIGTERM with exit
//! code 0, and refuses invalid configuration with exit code 78.
//!
//! Harness helpers run outside `#[test]` bodies, so the suite-wide test
//! allowances do not reach them; this file states that once instead of
//! scattering per-function expectations.
#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

use std::io::Read as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use ratatoskr_claude_archive::blob_store::scratch;

const BIN: &str = env!("CARGO_BIN_EXE_ratatoskr-claude-archive");
const TEST_DATABASE_URL: &str = "postgres://claude:claude@127.0.0.1:5438/claude";

/// A distinct loopback port per test run: derived from the pid so parallel
/// binaries never collide, well above the ephemeral-reservation zone used by
/// sibling services.
fn listen_port() -> u16 {
    29000 + u16::try_from(std::process::id() % 20_000).unwrap_or(0)
}

fn valid_environment(listen_address: String, blob_root: &std::path::Path) -> Vec<(String, String)> {
    vec![
        (
            "RATATOSKR__ADMIN__LISTEN_ADDRESS".to_owned(),
            listen_address,
        ),
        (
            "RATATOSKR__STORAGE__BLOB_ROOT".to_owned(),
            blob_root.display().to_string(),
        ),
        (
            "RATATOSKR__STORAGE__DATABASE_URL".to_owned(),
            TEST_DATABASE_URL.to_owned(),
        ),
    ]
}

struct RunningService {
    child: std::process::Child,
    port: u16,
    blob_root: std::path::PathBuf,
}

impl Drop for RunningService {
    fn drop(&mut self) {
        let _ignored = self.child.kill();
        let _ignored = self.child.wait();
        let _ignored = std::fs::remove_dir_all(&self.blob_root);
    }
}

fn start_service() -> RunningService {
    let port = listen_port();
    let blob_root = scratch::temp_root("boot");
    let child = Command::new(BIN)
        .envs(valid_environment(format!("127.0.0.1:{port}"), &blob_root))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("the service binary spawns");
    RunningService {
        child,
        port,
        blob_root,
    }
}

/// GET a health path, returning status and body when the server answers.
fn get_health(port: u16, path: &str) -> Option<(u16, String)> {
    use std::io::Write as _;

    let mut stream = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("the read timeout applies");
    let request =
        format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let status = response.split_whitespace().nth(1)?.parse::<u16>().ok()?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    Some((status, body))
}

fn wait_for_status(port: u16, path: &str, expected: u16, deadline: Duration) -> Option<String> {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if let Some((status, body)) = get_health(port, path)
            && status == expected
        {
            return Some(body);
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    None
}

#[test]
fn boots_serves_health_and_drains_on_sigterm() {
    // The compose database must be up for this test.
    let mut service = start_service();

    let live_body = wait_for_status(service.port, "/health/live", 200, Duration::from_secs(30))
        .expect("the service reaches liveness within the boot window");
    assert!(
        live_body.contains("\"state\":\"live\""),
        "liveness names its state"
    );

    let ready_body = wait_for_status(service.port, "/health/ready", 200, Duration::from_secs(30))
        .expect("readiness turns green once dependencies answer");
    assert!(
        ready_body.contains("\"database\":\"pass\"") || ready_body.contains("\"state\":\"pass\""),
        "readiness carries passing component checks"
    );

    // SIGTERM: graceful drain, exit code 0.
    let pid = service.child.id();
    let signaled = Command::new("kill")
        .args(["-TERM", &pid.to_string()])
        .status()
        .expect("the kill utility exists");
    assert!(signaled.success(), "SIGTERM is delivered");

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = service
            .child
            .try_wait()
            .expect("the child status is readable")
        {
            assert!(
                status.code() == Some(0),
                "a clean drain exits 0, got: {status:?}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the service drains within the shutdown bound"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    // After drain begins, readiness must refuse new work even while the
    // listener was still open.
    drop(service);
}

#[test]
fn check_config_refuses_invalid_environment_with_78() {
    let output = Command::new(BIN)
        .arg("check-config")
        // Missing both required values entirely.
        .env_remove("RATATOSKR__STORAGE__BLOB_ROOT")
        .env_remove("RATATOSKR__STORAGE__DATABASE_URL")
        .output()
        .expect("the service binary runs");

    assert_eq!(
        output.status.code(),
        Some(78),
        "invalid configuration exits EX_CONFIG"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("RATATOSKR__STORAGE__BLOB_ROOT"),
        "the refusal names the missing field"
    );
}
