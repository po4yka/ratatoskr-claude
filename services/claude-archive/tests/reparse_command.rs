//! Process boundary contracts for explicit reparse.

use ratatoskr_claude_archive_service::lifecycle_commands::{
    ReparseCommand, ReparseExecution, run_reparse_command,
};
use std::cell::RefCell;
use uuid::Uuid;

#[test]
fn reparse_command_requires_tenant_archive_parser_and_preserves_dry_run() {
    let tenant = "018f0000-0000-7000-8000-000000000801";
    let archive = "018f0000-0000-7000-8000-000000000802";
    let parsed = RefCell::new(None);
    let result = run_reparse_command(
        [
            "reparse",
            "--tenant",
            tenant,
            "--archive",
            archive,
            "--parser",
            "claude-personal@2.0",
            "--dry-run",
        ],
        |command| {
            parsed.replace(Some(command.clone()));
            ReparseExecution::Completed(serde_json::json!({"status":"planned"}))
        },
    );
    assert_eq!(
        parsed.into_inner(),
        Some(ReparseCommand {
            tenant_id: Uuid::parse_str(tenant).unwrap(),
            archive_id: Uuid::parse_str(archive).unwrap(),
            parser_name: "claude-personal".to_owned(),
            parser_version: "2.0".to_owned(),
            dry_run: true,
        })
    );
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.stdout, b"{\"status\":\"planned\"}\n");
    for invalid in [
        vec!["reparse", "--tenant", tenant, "--archive", archive],
        vec![
            "reparse",
            "--tenant",
            tenant,
            "--archive",
            archive,
            "--parser",
            "claude-personal",
        ],
        vec![
            "reparse",
            "--tenant",
            "bad",
            "--archive",
            archive,
            "--parser",
            "p@1",
        ],
        vec![
            "reparse",
            "--tenant",
            tenant,
            "--archive",
            archive,
            "--parser",
            "p@1",
            "--dry-run",
            "--dry-run",
        ],
    ] {
        let called = std::cell::Cell::new(false);
        let invalid = run_reparse_command(invalid, |_| {
            called.set(true);
            ReparseExecution::Completed(serde_json::json!({}))
        });
        assert_eq!(invalid.exit_code, 2);
        assert!(!called.get());
    }
}
