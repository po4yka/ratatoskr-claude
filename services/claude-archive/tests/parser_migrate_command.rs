//! Process boundary contracts for parser migrations.

use ratatoskr_claude_archive_service::lifecycle_commands::{
    ParserMigrateCommand, ParserMigrateExecution, run_parser_migrate_command,
};
use std::cell::{Cell, RefCell};
use uuid::Uuid;

#[test]
fn parser_migrate_command_requires_tenant_parser_and_preserves_dry_run() {
    let tenant = "018f0000-0000-7000-8000-000000000901";
    let parsed = RefCell::new(None);
    let result = run_parser_migrate_command(
        [
            "parser-migrate",
            "--tenant",
            tenant,
            "--operation-key",
            "upgrade-2026-08",
            "--parser",
            "claude-personal@2.0",
            "--dry-run",
        ],
        |command| {
            parsed.replace(Some(command.clone()));
            ParserMigrateExecution::Completed {
                report: serde_json::json!({"status":"planned"}),
                partial: false,
            }
        },
    );
    assert_eq!(
        parsed.into_inner(),
        Some(ParserMigrateCommand {
            tenant_id: Uuid::parse_str(tenant).unwrap(),
            operation_key: "upgrade-2026-08".to_owned(),
            parser_name: "claude-personal".to_owned(),
            parser_version: "2.0".to_owned(),
            dry_run: true
        })
    );
    assert_eq!(result.exit_code, 0);
    assert_eq!(result.stdout, b"{\"status\":\"planned\"}\n");
    let calls = Cell::new(0);
    for invalid in [
        vec![
            "parser-migrate",
            "--tenant",
            tenant,
            "--operation-key",
            "key",
        ],
        vec![
            "parser-migrate",
            "--tenant",
            tenant,
            "--operation-key",
            "key",
            "--parser",
            "name",
        ],
        vec!["parser-migrate", "--tenant", tenant, "--parser", "p@1"],
    ] {
        let result = run_parser_migrate_command(invalid, |_| {
            calls.set(calls.get() + 1);
            ParserMigrateExecution::Completed {
                report: serde_json::json!({}),
                partial: false,
            }
        });
        assert_eq!(result.exit_code, 2);
    }
    assert_eq!(calls.get(), 0);
    let partial = run_parser_migrate_command(
        [
            "parser-migrate",
            "--tenant",
            tenant,
            "--operation-key",
            "partial",
            "--parser",
            "p@1",
        ],
        |_| ParserMigrateExecution::Completed {
            report: serde_json::json!({"status":"partial"}),
            partial: true,
        },
    );
    assert_eq!(partial.exit_code, 1);
    assert_eq!(partial.stdout, b"{\"status\":\"partial\"}\n");
}
