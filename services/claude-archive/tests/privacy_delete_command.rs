//! Privacy-delete operator command contract.
#![expect(
    clippy::too_many_lines,
    reason = "one process-boundary test keeps the full invalid grammar together"
)]

use std::cell::{Cell, RefCell};
use std::ffi::OsString;

use ratatoskr_claude_archive_service::lifecycle_commands::{
    LifecycleCommandResult, PrivacyDeleteExecuteCommand, PrivacyDeleteExecuteExecution,
    PrivacyDeletePlanExecution, run_privacy_delete_execute_command,
    run_privacy_delete_plan_command,
};
use uuid::Uuid;

fn plan_args(values: &[&str]) -> Vec<OsString> {
    values
        .iter()
        .enumerate()
        .flat_map(|(index, value)| {
            if index == 0 {
                vec![OsString::from(value), OsString::from("plan")]
            } else {
                vec![OsString::from(value)]
            }
        })
        .collect()
}

fn planned() -> PrivacyDeletePlanExecution {
    PrivacyDeletePlanExecution::Planned {
        request_id: "00000000-0000-0000-0000-000000000001".to_owned(),
        item_count: 3,
    }
}

fn invalid_result() -> LifecycleCommandResult {
    LifecycleCommandResult {
        exit_code: 2,
        stdout: b"{\"command\":\"privacy-delete\",\"error_code\":\"invalid_arguments\",\"status\":\"failed\"}\n".to_vec(),
        stderr: b"usage: privacy-delete plan --tenant TENANT --request-key KEY --scope tenant|export|conversation [--target UUID]\n".to_vec(),
    }
}

#[derive(Debug, PartialEq, Eq)]
struct InvalidGrammarObservation {
    execution_calls: usize,
    leaked_private_arguments: bool,
    results: Vec<LifecycleCommandResult>,
}

#[test]
fn privacy_delete_plan_requires_exactly_one_tenant_scope() {
    let invalid = [
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
        ],
        vec![
            "privacy-delete",
            "--request-key",
            "request-private",
            "--scope",
            "tenant",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--scope",
            "tenant",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "export",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "conversation",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "tenant",
            "--target",
            "target-private",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "export",
            "--target",
            "not-a-uuid",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "unsupported",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "tenant,export",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "tenant",
            "--scope",
            "export",
            "--target",
            "target-private",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--tenant",
            "tenant-other",
            "--request-key",
            "request-private",
            "--scope",
            "tenant",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--request-key",
            "request-other",
            "--scope",
            "tenant",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "export",
            "--target",
            "target-private",
            "--target",
            "target-other",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "tenant",
            "--dry-run",
        ],
        vec![
            "privacy-delete",
            "--tenant",
            "tenant-private",
            "--request-key",
            "request-private",
            "--scope",
            "tenant",
            "--unknown",
        ],
        vec!["privacy-delete", "--tenant"],
        vec!["privacy-delete", "--request-key"],
        vec!["privacy-delete", "--scope"],
        vec!["privacy-delete", "--target"],
    ];
    let execution_calls = Cell::new(0_usize);
    let results: Vec<LifecycleCommandResult> = invalid
        .iter()
        .map(|values| {
            run_privacy_delete_plan_command(plan_args(values), |_| {
                execution_calls.set(execution_calls.get() + 1);
                planned()
            })
        })
        .collect();
    let leaked_private_arguments = results.iter().any(|result| {
        ["tenant-private", "request-private", "target-private"]
            .iter()
            .any(|private| {
                result
                    .stdout
                    .windows(private.len())
                    .any(|window| window == private.as_bytes())
                    || result
                        .stderr
                        .windows(private.len())
                        .any(|window| window == private.as_bytes())
            })
    });
    let actual = InvalidGrammarObservation {
        execution_calls: execution_calls.get(),
        leaked_private_arguments,
        results,
    };
    let expected = InvalidGrammarObservation {
        execution_calls: 0,
        leaked_private_arguments: false,
        results: vec![invalid_result(); invalid.len()],
    };

    assert_eq!(
        actual, expected,
        "invalid privacy-delete plans must be rejected uniformly before execution"
    );
}

fn execute_args(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

fn execute_invalid_result() -> LifecycleCommandResult {
    LifecycleCommandResult {
        exit_code: 2,
        stdout: b"{\"command\":\"privacy-delete\",\"error_code\":\"invalid_arguments\",\"mode\":\"execute\",\"status\":\"failed\"}\n".to_vec(),
        stderr:
            b"usage: privacy-delete execute --tenant TENANT --request-id UUID --confirm\n"
                .to_vec(),
    }
}

#[test]
fn privacy_delete_execute_requires_confirmation() {
    const REQUEST: &str = "018f0000-0000-7000-8000-000000000701";
    let parsed = RefCell::new(None);
    let valid = run_privacy_delete_execute_command(
        execute_args(&[
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-alpha",
            "--request-id",
            REQUEST,
            "--confirm",
        ]),
        |command| {
            parsed.replace(Some(command.clone()));
            PrivacyDeleteExecuteExecution::Completed {
                request_id: command.request_id,
            }
        },
    );
    assert_eq!(
        parsed.into_inner(),
        Some(PrivacyDeleteExecuteCommand {
            tenant: "tenant-alpha".to_owned(),
            request_id: Uuid::parse_str(REQUEST).expect("request fixture UUID is valid"),
        })
    );
    assert_eq!(valid.exit_code, 0);
    assert_eq!(
        valid.stdout,
        b"{\"command\":\"privacy-delete\",\"mode\":\"execute\",\"request_id\":\"018f0000-0000-7000-8000-000000000701\",\"status\":\"completed\"}\n"
    );
    assert!(valid.stderr.is_empty());

    let invalid = [
        vec![
            "privacy-delete",
            "execute",
            "--request-id",
            REQUEST,
            "--confirm",
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--confirm",
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--request-id",
            REQUEST,
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--tenant",
            "tenant-other",
            "--request-id",
            REQUEST,
            "--confirm",
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--request-id",
            REQUEST,
            "--request-id",
            "018f0000-0000-7000-8000-000000000702",
            "--confirm",
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--request-id",
            REQUEST,
            "--confirm",
            "--confirm",
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--request-id",
            "not-a-uuid",
            "--confirm",
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--request-id",
            REQUEST,
            "--confirm=false",
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--request-id",
            REQUEST,
            "--confirm",
            "false",
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--request-key",
            "plan-only-key",
            "--scope",
            "tenant",
            "--confirm",
        ],
        vec![
            "privacy-delete",
            "execute",
            "--tenant",
            "tenant-private",
            "--request-id",
            REQUEST,
            "--confirm",
            "--unknown",
        ],
        vec!["privacy-delete", "execute", "--tenant"],
        vec!["privacy-delete", "execute", "--request-id"],
    ];
    let execution_calls = Cell::new(0_usize);
    let results: Vec<LifecycleCommandResult> = invalid
        .iter()
        .map(|values| {
            run_privacy_delete_execute_command(execute_args(values), |_| {
                execution_calls.set(execution_calls.get() + 1);
                PrivacyDeleteExecuteExecution::Completed {
                    request_id: Uuid::nil(),
                }
            })
        })
        .collect();
    let leaked_private_arguments = results.iter().any(|result| {
        ["tenant-private", "plan-only-key"].iter().any(|private| {
            result
                .stdout
                .windows(private.len())
                .any(|window| window == private.as_bytes())
                || result
                    .stderr
                    .windows(private.len())
                    .any(|window| window == private.as_bytes())
        })
    });
    let actual = InvalidGrammarObservation {
        execution_calls: execution_calls.get(),
        leaked_private_arguments,
        results,
    };
    let expected = InvalidGrammarObservation {
        execution_calls: 0,
        leaked_private_arguments: false,
        results: vec![execute_invalid_result(); invalid.len()],
    };

    assert_eq!(
        actual, expected,
        "privacy-delete execute must require exact durable request confirmation before execution"
    );
}
