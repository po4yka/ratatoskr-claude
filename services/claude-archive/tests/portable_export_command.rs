//! Portable-export operator command contract.
#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "one process-boundary test keeps the full argument grammar together"
)]

use std::cell::RefCell;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use ratatoskr_claude_archive_service::lifecycle_commands::{
    LifecycleCommandResult, PortableExportCommand, PortableExportExecution,
    run_portable_export_command,
};

fn args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

fn completed() -> PortableExportExecution {
    PortableExportExecution::Completed {
        archive_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            .to_owned(),
        byte_size: 42,
    }
}

fn assert_invalid(values: &[&str]) {
    let result = run_portable_export_command(args(values), |_| completed());
    assert_eq!(result.exit_code, 2, "invalid invocation: {values:?}");
    assert!(result.stdout.is_empty(), "usage errors have no data output");
    assert!(
        result.stderr.ends_with(b"\n") && !result.stderr.is_empty(),
        "usage diagnostic is one newline-terminated record"
    );
    let stderr = String::from_utf8(result.stderr).expect("usage diagnostic is UTF-8");
    assert!(!stderr.contains("tenant-private"));
    assert!(!stderr.contains("/private/output"));
}

fn assert_one_json_document(result: &LifecycleCommandResult) {
    assert!(result.stdout.ends_with(b"\n"));
    assert!(!result.stdout[..result.stdout.len() - 1].contains(&b'\n'));
    serde_json::from_slice::<serde_json::Value>(&result.stdout)
        .expect("stdout is exactly one newline-terminated JSON document");
}

#[test]
fn portable_export_command_requires_tenant_and_output() {
    let parsed = RefCell::new(None);
    let completed_result = run_portable_export_command(
        args(&[
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/portable.zip",
            "--project",
            "project-alpha",
            "--observed-from",
            "2026-08-27T00:00:00Z",
            "--observed-to",
            "2026-08-28T00:00:00Z",
        ]),
        |command| {
            parsed.replace(Some(command.clone()));
            completed()
        },
    );
    assert_eq!(completed_result.exit_code, 0);
    assert_eq!(
        completed_result.stdout,
        b"{\"archive_sha256\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"byte_size\":42,\"command\":\"portable-export\",\"status\":\"completed\"}\n"
    );
    assert!(completed_result.stderr.is_empty());
    assert_one_json_document(&completed_result);
    assert_eq!(
        parsed.into_inner(),
        Some(PortableExportCommand {
            tenant: "tenant-alpha".to_owned(),
            output: PathBuf::from("/tmp/portable.zip"),
            project: Some("project-alpha".to_owned()),
            observed_from: Some("2026-08-27T00:00:00Z".to_owned()),
            observed_to: Some("2026-08-28T00:00:00Z".to_owned()),
        })
    );

    let failed = run_portable_export_command(
        args(&[
            "portable-export",
            "--tenant",
            "tenant-private",
            "--output",
            "/private/output",
        ]),
        |_| PortableExportExecution::Failed {
            error_code: "output_unavailable".to_owned(),
        },
    );
    assert_eq!(failed.exit_code, 1);
    assert_eq!(
        failed.stdout,
        b"{\"command\":\"portable-export\",\"error_code\":\"output_unavailable\",\"status\":\"failed\"}\n"
    );
    assert_one_json_document(&failed);
    let failed_stderr = String::from_utf8(failed.stderr).expect("stderr is UTF-8");
    assert!(failed_stderr.contains("output_unavailable"));
    assert!(!failed_stderr.contains("tenant-private"));
    assert!(!failed_stderr.contains("/private/output"));

    for invalid in [
        vec!["portable-export", "--output", "/tmp/archive.zip"],
        vec!["portable-export", "--tenant", "tenant-alpha"],
        vec!["portable-export", "--tenant"],
        vec!["portable-export", "--tenant", "tenant-alpha", "--output"],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/archive.zip",
            "--project",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/archive.zip",
            "--observed-from",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/archive.zip",
            "--observed-to",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--tenant",
            "tenant-beta",
            "--output",
            "/tmp/archive.zip",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/one.zip",
            "--output",
            "/tmp/two.zip",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/archive.zip",
            "--project",
            "one",
            "--project",
            "two",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/archive.zip",
            "--observed-from",
            "2026-08-27T00:00:00Z",
            "--observed-from",
            "2026-08-27T01:00:00Z",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/archive.zip",
            "--observed-to",
            "2026-08-28T00:00:00Z",
            "--observed-to",
            "2026-08-28T01:00:00Z",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/archive.zip",
            "--unknown",
            "value",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/archive.zip",
            "--observed-from",
            "not-a-time",
        ],
        vec![
            "portable-export",
            "--tenant",
            "tenant-alpha",
            "--output",
            "/tmp/archive.zip",
            "--observed-from",
            "2026-08-28T00:00:00Z",
            "--observed-to",
            "2026-08-27T00:00:00Z",
        ],
    ] {
        assert_invalid(&invalid);
    }

    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt as _;

        let non_utf8 = OsString::from_vec(b"/tmp/portable-\xff.zip".to_vec());
        let observed = RefCell::new(None);
        let result = run_portable_export_command(
            vec![
                OsString::from("portable-export"),
                OsString::from("--tenant"),
                OsString::from("tenant-alpha"),
                OsString::from("--output"),
                non_utf8.clone(),
            ],
            |command| {
                observed.replace(Some(command.output.clone()));
                completed()
            },
        );
        assert_eq!(result.exit_code, 0);
        assert_eq!(observed.into_inner().as_deref(), Some(Path::new(&non_utf8)));
    }
}
