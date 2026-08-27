//! Owner-derived fixture admission privacy and determinism gates.

#![allow(clippy::expect_used, reason = "synthetic fixture construction")]

use std::path::{Path, PathBuf};

use ratatoskr_claude_archive::fixture_admission::{FixtureAdmission, FixtureAdmissionStatus};
use uuid::Uuid;

#[test]
fn fixture_admission_rejects_raw_private_or_unapproved_candidates()
-> Result<(), Box<dyn std::error::Error>> {
    assert_rejected("raw_archive", |root| {
        std::fs::write(root.join("owner-export.zip"), b"PK private")
    })?;
    assert_rejected("forbidden_field", |root| {
        std::fs::write(
            root.join("derived.json"),
            br#"{"shape":"conversation","account_email":"owner@example.com"}"#,
        )
    })?;
    assert_rejected("unsafe_path", |root| {
        update_manifest(root, |manifest| {
            manifest["fixture"] = "../derived.json".into();
        })
    })?;
    assert_rejected("unlisted_file", |root| {
        std::fs::write(root.join("extra.json"), br#"{"unexpected":true}"#)
    })?;
    assert_rejected("missing_review", |root| {
        update_manifest(root, |manifest| {
            manifest["reviews"]["owner_approval"] = false.into();
        })
    })?;
    assert_rejected("nondeterministic", |root| {
        std::fs::write(root.join("expected-structure.json"), br#"{"records":2}"#)
    })?;

    let valid = Candidate::new()?;
    write_valid_candidate(valid.path())?;
    let report = FixtureAdmission::inspect(valid.path())?;
    assert_eq!(
        report.status,
        FixtureAdmissionStatus::Admitted,
        "{report:?}"
    );
    assert!(report.findings.is_empty());
    Ok(())
}

fn assert_rejected(
    case: &str,
    mutate: impl FnOnce(&Path) -> std::io::Result<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    let candidate = Candidate::new()?;
    write_valid_candidate(candidate.path())?;
    mutate(candidate.path())?;
    let report = FixtureAdmission::inspect(candidate.path())?;
    assert_eq!(
        report.status,
        FixtureAdmissionStatus::Rejected,
        "unsafe case {case} was admitted: {report:?}"
    );
    assert!(!report.findings.is_empty(), "{case} needs a stable finding");
    Ok(())
}

struct Candidate(PathBuf);

impl Candidate {
    fn new() -> std::io::Result<Self> {
        let path = std::env::temp_dir().join(format!("claude-fixture-{}", Uuid::now_v7()));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Candidate {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn write_valid_candidate(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::write(
        root.join("derived.json"),
        br#"{"shape":"conversation","variants":["text"]}"#,
    )?;
    std::fs::write(root.join("observed-structure.json"), br#"{"records":1}"#)?;
    std::fs::write(root.join("expected-structure.json"), br#"{"records":1}"#)?;
    std::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&valid_manifest())?,
    )?;
    Ok(())
}

fn update_manifest(
    root: &Path,
    update: impl FnOnce(&mut serde_json::Value),
) -> std::io::Result<()> {
    let bytes = std::fs::read(root.join("manifest.json"))?;
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(std::io::Error::other)?;
    update(&mut manifest);
    std::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).map_err(std::io::Error::other)?,
    )
}

fn valid_manifest() -> serde_json::Value {
    serde_json::json!({
        "format": "ratatoskr-owner-derived-fixture",
        "case_id": "synthetic-conversation-shape",
        "acquisition_mode": "consumer_export",
        "schema_id": "claude.synthetic.owner-derived",
        "private_evidence_record": "OWNER-EVIDENCE-0001",
        "fixture": "derived.json",
        "observed_structure": "observed-structure.json",
        "expected_structure": "expected-structure.json",
        "synthetic_only": true,
        "reviews": {
            "consent": true,
            "license": true,
            "secret_scan": true,
            "personal_data_scan": true,
            "path_safety": true,
            "deterministic_comparison": true,
            "independent_review": true,
            "owner_approval": true
        }
    })
}

#[test]
fn admitted_synthetic_fixture_matches_read_only_golden_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let candidate =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/owner-admission-synthetic");
    let report = FixtureAdmission::inspect(&candidate)?;
    let mut actual = serde_json::to_vec_pretty(&report)?;
    actual.push(b'\n');
    let expected = std::fs::read(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden/owner_fixture_admission.json"),
    )?;
    assert_eq!(
        actual, expected,
        "ordinary tests must only compare the reviewed golden"
    );
    Ok(())
}
