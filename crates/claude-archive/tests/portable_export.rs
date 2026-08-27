//! Deterministic portable-archive contract tests.
#![expect(
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "integration-test scaffolding and JSON contract assertions fail loudly"
)]

use std::collections::BTreeSet;
use std::io::{Cursor, Read as _};
use std::path::{Component, Path};

use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::portable_export::{
    PortableArchiveExporter, PortableArchiveState, PortableArtifact, PortableArtifactVersion,
    PortableAsset, PortableAssetAvailability, PortableConversation, PortableExportError,
    PortableExportFilter, PortableKnowledgeSource, PortableProject, PortableProvenance,
};
use ratatoskr_claude_archive::{BlobStore, MediaType, StoreError};
use sha2::Digest as _;

fn fixture_state() -> PortableArchiveState {
    PortableArchiveState {
        account_external_ref: "account-alpha".to_owned(),
        provenance: PortableProvenance {
            source_snapshot_ids: vec!["snapshot-alpha".to_owned()],
            archive_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_owned(),
            parser_name: "synthetic-conversations".to_owned(),
            parser_version: "1.0.0".to_owned(),
            observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
            completeness: "complete".to_owned(),
        },
        projects: Vec::new(),
        conversations: vec![PortableConversation {
            external_id: "conversation-alpha".to_owned(),
            project_external_id: None,
            title: Some("Portable conversation".to_owned()),
            observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
            payload: serde_json::json!({"messages": []}),
        }],
        artifacts: Vec::new(),
        assets: Vec::new(),
    }
}

fn complete_fixture(blob: ratatoskr_claude_archive::BlobRef) -> PortableArchiveState {
    let mut state = fixture_state();
    state.projects.push(PortableProject {
        external_id: "project-alpha".to_owned(),
        title: Some("Portable project".to_owned()),
        observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
        payload: serde_json::json!({"instructions": "Preserve evidence."}),
        knowledge_sources: vec![PortableKnowledgeSource {
            external_id: "knowledge-alpha".to_owned(),
            source_kind: "file".to_owned(),
            title: Some("knowledge.txt".to_owned()),
            availability: "verified".to_owned(),
            payload: serde_json::json!({"sha256": blob.digest_hex.clone()}),
        }],
    });
    state.conversations[0].project_external_id = Some("project-alpha".to_owned());
    state.conversations[0].payload = serde_json::json!({
        "messages": [
            {"external_id": "message-root", "role": "user", "parent_external_id": null,
             "parts": [{"ordinal": 0, "kind": "text", "text": "Portable graph root."}]},
            {"external_id": "message-child", "role": "assistant",
             "parent_external_id": "message-root", "parts": []}
        ]
    });
    state.artifacts.push(PortableArtifact {
        external_id: "artifact-alpha".to_owned(),
        conversation_external_id: Some("conversation-alpha".to_owned()),
        title: Some("Portable Artifact".to_owned()),
        versions: vec![
            PortableArtifactVersion {
                external_id: "artifact-version-1".to_owned(),
                previous_external_id: None,
                payload: serde_json::json!({"content": "first"}),
            },
            PortableArtifactVersion {
                external_id: "artifact-version-2".to_owned(),
                previous_external_id: Some("artifact-version-1".to_owned()),
                payload: serde_json::json!({"content": "second"}),
            },
        ],
    });
    state.assets.push(PortableAsset {
        external_id: "asset-alpha".to_owned(),
        project_external_id: Some("project-alpha".to_owned()),
        observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
        availability: PortableAssetAvailability::Verified,
        blob: Some(blob),
        media_type: Some("text/plain".to_owned()),
    });
    state
}

fn member_names(zip: &mut zip::ZipArchive<Cursor<Vec<u8>>>) -> Vec<String> {
    (0..zip.len())
        .map(|index| {
            zip.by_index(index)
                .expect("member must be readable")
                .name()
                .to_owned()
        })
        .collect()
}

fn assert_required_members(names: &[String]) {
    for required in [
        ("projects/", ".json"),
        ("projects/", ".md"),
        ("knowledge/", ".json"),
        ("conversations/", ".json"),
        ("conversations/", ".md"),
        ("artifacts/", ".json"),
        ("artifacts/", ".md"),
        ("assets/", ""),
    ] {
        assert!(
            names
                .iter()
                .any(|path| path.starts_with(required.0) && path.ends_with(required.1)),
            "portable output must contain a {}*{} member",
            required.0,
            required.1
        );
    }
}

fn read_member(zip: &mut zip::ZipArchive<Cursor<Vec<u8>>>, path: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    zip.by_name(path)
        .expect("member must exist")
        .read_to_end(&mut bytes)
        .expect("member must be readable");
    bytes
}

fn assert_manifest_entries(zip: &mut zip::ZipArchive<Cursor<Vec<u8>>>, non_manifest: &[String]) {
    let manifest_bytes = read_member(zip, "manifest.json");
    let manifest: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("manifest must be JSON");
    let entries = manifest["members"]
        .as_array()
        .expect("manifest must list non-manifest members");
    let listed_paths = entries
        .iter()
        .map(|entry| entry["path"].as_str().expect("member path"))
        .collect::<Vec<_>>();
    assert_eq!(listed_paths, non_manifest);
    for entry in entries {
        assert_manifest_entry(zip, entry);
    }
}

fn assert_manifest_entry(zip: &mut zip::ZipArchive<Cursor<Vec<u8>>>, entry: &serde_json::Value) {
    let member_bytes = read_member(zip, entry["path"].as_str().expect("member path"));
    assert_eq!(entry["byte_length"], member_bytes.len());
    assert_eq!(
        entry["sha256"],
        format!("{:x}", sha2::Sha256::digest(&member_bytes))
    );
    assert!(entry["media_type"].is_string());
    assert!(matches!(
        entry["availability"].as_str(),
        Some("verified" | "normalized")
    ));
    assert_eq!(entry["completeness"], "complete");
    assert_eq!(
        entry["provenance"]["source_snapshot_ids"],
        serde_json::json!(["snapshot-alpha"])
    );
    assert_eq!(
        entry["provenance"]["archive_sha256"],
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(
        entry["provenance"]["parser_name"],
        "synthetic-conversations"
    );
    assert_eq!(entry["provenance"]["parser_version"], "1.0.0");
}

#[test]
fn manifest_lists_json_markdown_and_verified_asset_members() {
    let root = temp_root("portable-export-manifest");
    let store = BlobStore::open(&root).expect("the fixture BlobStore opens");
    let asset_bytes = b"verified Project Knowledge bytes\n";
    let blob = store
        .store(
            MediaType::parse("text/plain").expect("the fixture media type is valid"),
            asset_bytes,
        )
        .expect("the fixture asset is stored and verified");
    let state = complete_fixture(blob);

    let bytes = PortableArchiveExporter::new()
        .export_to_bytes_with_assets(&state, &store)
        .expect("verified fixture state must export");
    remove(&root);
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes)).expect("output must be a ZIP");
    let member_names = member_names(&mut zip);
    assert_eq!(
        member_names.last().map(String::as_str),
        Some("manifest.json"),
        "the canonical manifest must be the final ZIP member"
    );

    let non_manifest = &member_names[..member_names.len() - 1];
    let mut sorted = non_manifest.to_vec();
    sorted.sort();
    assert_eq!(
        non_manifest, sorted,
        "members must be lexicographically ordered"
    );
    assert_required_members(non_manifest);

    let asset_path = non_manifest
        .iter()
        .find(|path| path.starts_with("assets/"))
        .expect("verified asset member must exist");
    let archived_asset = read_member(&mut zip, asset_path);
    assert_eq!(archived_asset, asset_bytes);
    assert_manifest_entries(&mut zip, non_manifest);
}

#[test]
fn identical_state_produces_byte_identical_zip() {
    let exporter = PortableArchiveExporter::new();
    let first = exporter
        .export_to_bytes(&fixture_state())
        .expect("fixture state must export");
    let second = exporter
        .export_to_bytes(&fixture_state())
        .expect("fixture state must export");

    assert!(!first.is_empty(), "portable archive must contain members");
    assert_eq!(
        first, second,
        "identical state must have identical ZIP bytes"
    );
    let mut archive = zip::ZipArchive::new(Cursor::new(&first)).expect("output must be a ZIP");
    let members = (0..archive.len())
        .map(|index| {
            archive
                .by_index(index)
                .expect("member must be readable")
                .name()
                .to_owned()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        members,
        [
            "conversations/conversation-alpha-d45bb53fd24decaa.json",
            "conversations/conversation-alpha-d45bb53fd24decaa.md",
            "manifest.json",
        ],
        "members must use one stable lexicographic order"
    );
    assert_eq!(
        format!("{:x}", sha2::Sha256::digest(&first)),
        "d709dd8a83d89b4a09d51333f5e324095347b4e0eca11387fb4a0f8e47b0f83a",
        "the stable ZIP layout is a portable output contract"
    );
}

fn unsafe_name_state() -> PortableArchiveState {
    let mut state = fixture_state();
    state.projects.push(PortableProject {
        external_id: "../project\u{7}".to_owned(),
        title: Some("<script>alert('project')</script>".to_owned()),
        observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
        payload: serde_json::json!({"provider_title": "/absolute/project-title"}),
        knowledge_sources: vec![PortableKnowledgeSource {
            external_id: "/absolute/knowledge\nsource".to_owned(),
            source_kind: "file".to_owned(),
            title: Some("../provider-knowledge-file.txt".to_owned()),
            availability: "missing".to_owned(),
            payload: serde_json::json!({}),
        }],
    });
    state.conversations = vec![
        PortableConversation {
            external_id: "collision/name".to_owned(),
            project_external_id: Some("../project\u{7}".to_owned()),
            title: Some("../provider-conversation-title".to_owned()),
            observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
            payload: serde_json::json!({"messages": []}),
        },
        PortableConversation {
            external_id: "collision\\name".to_owned(),
            project_external_id: Some("../project\u{7}".to_owned()),
            title: Some("/absolute/provider-title".to_owned()),
            observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
            payload: serde_json::json!({"messages": []}),
        },
        PortableConversation {
            external_id: "duplicate-identity".to_owned(),
            project_external_id: None,
            title: Some("first duplicate".to_owned()),
            observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
            payload: serde_json::json!({"messages": []}),
        },
        PortableConversation {
            external_id: "duplicate-identity".to_owned(),
            project_external_id: None,
            title: Some("second duplicate".to_owned()),
            observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
            payload: serde_json::json!({"messages": []}),
        },
    ];
    state.artifacts.push(PortableArtifact {
        external_id: "/artifact/..\u{0}unsafe".to_owned(),
        conversation_external_id: Some("collision/name".to_owned()),
        title: Some("../provider-artifact-title".to_owned()),
        versions: vec![PortableArtifactVersion {
            external_id: "../version".to_owned(),
            previous_external_id: None,
            payload: serde_json::json!({}),
        }],
    });
    state
}

fn assert_safe_member_paths(names: &[String]) {
    for name in names {
        assert!(!Path::new(name).is_absolute(), "path is absolute: {name}");
        assert!(
            Path::new(name)
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
            "member path must contain only normal components: {name}"
        );
        assert!(!name.chars().any(char::is_control));
        assert!(
            name == "manifest.json"
                || [
                    "projects/",
                    "knowledge/",
                    "conversations/",
                    "artifacts/",
                    "assets/",
                ]
                .iter()
                .any(|prefix| name.starts_with(prefix)),
            "member escaped its assigned directory: {name}"
        );
    }
}

fn assert_provider_names_are_not_paths(names: &[String]) {
    for provider_name in [
        "<script>alert('project')</script>",
        "/absolute/project-title",
        "../provider-knowledge-file.txt",
        "../provider-conversation-title",
        "/absolute/provider-title",
        "../provider-artifact-title",
    ] {
        assert!(names.iter().all(|path| !path.contains(provider_name)));
    }
}

fn is_text_member(name: &str) -> bool {
    Path::new(name).extension().is_some_and(|extension| {
        extension.eq_ignore_ascii_case("json") || extension.eq_ignore_ascii_case("md")
    })
}

#[test]
fn unsafe_names_resolve_to_inert_unique_paths() {
    let state = unsafe_name_state();

    let exported = PortableArchiveExporter::new().export_to_bytes(&state);
    assert!(
        matches!(
            &exported,
            Ok(_) | Err(PortableExportError::DuplicateStableIdentity { .. })
        ),
        "duplicate identities must be resolved or refused before ZIP assembly: {exported:?}"
    );
    let Ok(bytes) = exported else {
        return;
    };
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes)).expect("output must be a ZIP");
    let names = member_names(&mut zip);
    assert_safe_member_paths(&names);
    assert_provider_names_are_not_paths(&names);

    let unique = names.iter().collect::<BTreeSet<_>>();
    assert_eq!(
        unique.len(),
        names.len(),
        "duplicate stable identities must be deterministically resolved or refused"
    );

    let textual = names
        .iter()
        .filter(|name| is_text_member(name))
        .flat_map(|name| read_member(&mut zip, name))
        .collect::<Vec<_>>();
    let textual = String::from_utf8(textual).expect("JSON and Markdown must be UTF-8");
    assert!(textual.contains("../provider-knowledge-file.txt"));
    assert!(textual.contains("../provider-conversation-title"));
    assert!(textual.contains("../provider-artifact-title"));
    assert!(
        !textual.contains("# <script>alert('project')</script>"),
        "active provider HTML must remain inert in Markdown"
    );
}

#[test]
fn unreadable_verified_asset_aborts_without_archive() {
    let root = temp_root("portable-export-missing-asset");
    let store = BlobStore::open(&root).expect("the fixture BlobStore opens");
    let blob = store
        .store(
            MediaType::parse("text/plain").expect("the fixture media type is valid"),
            b"owned verified fixture bytes\n",
        )
        .expect("the owned fixture asset is stored");
    let expected_digest = blob.digest_hex.clone();
    let (prefix, remainder) = expected_digest.split_at(2);
    let exact_owned_object = root.join("sha256").join(prefix).join(remainder);
    std::fs::remove_file(&exact_owned_object)
        .expect("the test removes only its owned fixture blob");

    let state = complete_fixture(blob);
    let destination = root.join("portable.zip");
    let unrelated_sibling = root.join(".portable.zip.unrelated.part");
    std::fs::write(&unrelated_sibling, b"unrelated sentinel")
        .expect("the unrelated sibling fixture is written");

    let result =
        PortableArchiveExporter::new().export_to_path_with_assets(&state, &store, &destination);
    let result_debug = format!("{result:?}");
    let unavailable_digest = match &result {
        Err(PortableExportError::Blob(StoreError::Missing { digest_hex })) => {
            Some(digest_hex.clone())
        }
        _ => None,
    };
    let destination_exists = destination.exists();
    let unrelated_contents =
        std::fs::read(&unrelated_sibling).expect("the unrelated sibling must remain readable");
    let leaked_owned_parts = std::fs::read_dir(&root)
        .expect("the output directory remains listable")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path != &unrelated_sibling
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with(".portable.zip.")
                            && Path::new(name)
                                .extension()
                                .is_some_and(|extension| extension.eq_ignore_ascii_case("part"))
                    })
        })
        .collect::<Vec<_>>();
    remove(&root);

    assert_eq!(
        unavailable_digest.as_deref(),
        Some(expected_digest.as_str()),
        "the error must identify the unavailable verified asset: {result_debug}"
    );
    assert!(
        !destination_exists,
        "a failed verified-asset export must not publish its destination"
    );
    assert!(
        leaked_owned_parts.is_empty(),
        "a failed export must remove only its owned temporary sibling: {leaked_owned_parts:?}"
    );
    assert_eq!(unrelated_contents, b"unrelated sentinel");
}

fn selected_filter_state(
    selected_blob: ratatoskr_claude_archive::BlobRef,
    excluded_blob: ratatoskr_claude_archive::BlobRef,
) -> PortableArchiveState {
    let mut selected_tenant = fixture_state();
    selected_tenant.projects = vec![
        PortableProject {
            external_id: "project-selected".to_owned(),
            title: Some("Selected project".to_owned()),
            observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
            payload: serde_json::json!({"marker": "selected-project-lower-bound"}),
            knowledge_sources: vec![PortableKnowledgeSource {
                external_id: "knowledge-selected".to_owned(),
                source_kind: "text".to_owned(),
                title: Some("Selected knowledge".to_owned()),
                availability: "verified".to_owned(),
                payload: serde_json::json!({"marker": "selected-knowledge"}),
            }],
        },
        PortableProject {
            external_id: "project-other".to_owned(),
            title: Some("Excluded project".to_owned()),
            observed_at_rfc3339: "2026-08-27T12:00:00Z".to_owned(),
            payload: serde_json::json!({"marker": "excluded-project"}),
            knowledge_sources: vec![PortableKnowledgeSource {
                external_id: "knowledge-other".to_owned(),
                source_kind: "text".to_owned(),
                title: Some("Excluded knowledge".to_owned()),
                availability: "verified".to_owned(),
                payload: serde_json::json!({"marker": "excluded-knowledge"}),
            }],
        },
    ];
    selected_tenant.conversations = vec![
        PortableConversation {
            external_id: "conversation-selected".to_owned(),
            project_external_id: Some("project-selected".to_owned()),
            title: Some("Selected conversation".to_owned()),
            observed_at_rfc3339: "2026-08-28T00:00:00Z".to_owned(),
            payload: serde_json::json!({"messages": [], "marker": "selected-upper-bound"}),
        },
        PortableConversation {
            external_id: "conversation-other-project".to_owned(),
            project_external_id: Some("project-other".to_owned()),
            title: Some("Other project conversation".to_owned()),
            observed_at_rfc3339: "2026-08-27T12:00:00Z".to_owned(),
            payload: serde_json::json!({"messages": [], "marker": "excluded-project-conversation"}),
        },
        PortableConversation {
            external_id: "conversation-before".to_owned(),
            project_external_id: Some("project-selected".to_owned()),
            title: Some("Before range".to_owned()),
            observed_at_rfc3339: "2026-08-26T23:59:59Z".to_owned(),
            payload: serde_json::json!({"messages": [], "marker": "excluded-before"}),
        },
        PortableConversation {
            external_id: "conversation-after".to_owned(),
            project_external_id: Some("project-selected".to_owned()),
            title: Some("After range".to_owned()),
            observed_at_rfc3339: "2026-08-28T00:00:01Z".to_owned(),
            payload: serde_json::json!({"messages": [], "marker": "excluded-after"}),
        },
    ];
    selected_tenant.artifacts = vec![
        PortableArtifact {
            external_id: "artifact-selected".to_owned(),
            conversation_external_id: Some("conversation-selected".to_owned()),
            title: Some("Selected Artifact".to_owned()),
            versions: vec![PortableArtifactVersion {
                external_id: "artifact-selected-v1".to_owned(),
                previous_external_id: None,
                payload: serde_json::json!({"marker": "selected-artifact"}),
            }],
        },
        PortableArtifact {
            external_id: "artifact-other".to_owned(),
            conversation_external_id: Some("conversation-other-project".to_owned()),
            title: Some("Excluded Artifact".to_owned()),
            versions: vec![PortableArtifactVersion {
                external_id: "artifact-other-v1".to_owned(),
                previous_external_id: None,
                payload: serde_json::json!({"marker": "excluded-artifact"}),
            }],
        },
    ];
    selected_tenant.assets = vec![
        PortableAsset {
            external_id: "asset-selected".to_owned(),
            project_external_id: Some("project-selected".to_owned()),
            observed_at_rfc3339: "2026-08-27T00:00:00Z".to_owned(),
            availability: PortableAssetAvailability::Verified,
            blob: Some(selected_blob),
            media_type: Some("text/plain".to_owned()),
        },
        PortableAsset {
            external_id: "asset-other".to_owned(),
            project_external_id: Some("project-other".to_owned()),
            observed_at_rfc3339: "2026-08-27T12:00:00Z".to_owned(),
            availability: PortableAssetAvailability::Verified,
            blob: Some(excluded_blob),
            media_type: Some("text/plain".to_owned()),
        },
    ];
    selected_tenant
}

fn foreign_filter_state(foreign_blob: ratatoskr_claude_archive::BlobRef) -> PortableArchiveState {
    let mut foreign_tenant = fixture_state();
    "account-beta".clone_into(&mut foreign_tenant.account_external_ref);
    "conversation-foreign".clone_into(&mut foreign_tenant.conversations[0].external_id);
    foreign_tenant.conversations[0].project_external_id = Some("project-selected".to_owned());
    foreign_tenant.conversations[0].payload =
        serde_json::json!({"messages": [], "marker": "foreign-tenant"});
    foreign_tenant.assets.push(PortableAsset {
        external_id: "asset-foreign".to_owned(),
        project_external_id: Some("project-selected".to_owned()),
        observed_at_rfc3339: "2026-08-27T12:00:00Z".to_owned(),
        availability: PortableAssetAvailability::Verified,
        blob: Some(foreign_blob),
        media_type: Some("text/plain".to_owned()),
    });
    foreign_tenant
}

fn portable_filter() -> PortableExportFilter {
    PortableExportFilter {
        account_external_ref: "account-alpha".to_owned(),
        project_external_id: Some("project-selected".to_owned()),
        observed_from_rfc3339: Some("2026-08-27T00:00:00Z".to_owned()),
        observed_to_rfc3339: Some("2026-08-28T00:00:00Z".to_owned()),
    }
}

fn store_filter_blob(store: &BlobStore, bytes: &[u8]) -> ratatoskr_claude_archive::BlobRef {
    store
        .store(
            MediaType::parse("text/plain").expect("filter fixture media type"),
            bytes,
        )
        .expect("filter fixture asset stores")
}

#[test]
fn tenant_project_and_time_filters_exclude_unselected_evidence() {
    let root = temp_root("portable-export-filter");
    let store = BlobStore::open(&root).expect("the fixture BlobStore opens");
    let selected_tenant = selected_filter_state(
        store_filter_blob(&store, b"selected-boundary-asset"),
        store_filter_blob(&store, b"excluded-project-asset"),
    );
    let foreign_tenant = foreign_filter_state(store_filter_blob(&store, b"foreign-tenant-asset"));
    let filter = portable_filter();

    let bytes = PortableArchiveExporter::new()
        .export_selected_to_bytes_with_assets(&[selected_tenant, foreign_tenant], &filter, &store)
        .expect("the authenticated tenant selection must export");
    remove(&root);
    let mut zip = zip::ZipArchive::new(Cursor::new(bytes)).expect("output must be a ZIP");
    let names = member_names(&mut zip);
    let archive_evidence = names
        .iter()
        .flat_map(|name| read_member(&mut zip, name))
        .collect::<Vec<_>>();
    let archive_evidence = String::from_utf8_lossy(&archive_evidence);

    for selected in [
        "project-selected",
        "knowledge-selected",
        "conversation-selected",
        "artifact-selected",
        "selected-boundary-asset",
    ] {
        assert!(archive_evidence.contains(selected), "missing: {selected}");
    }
    for excluded in [
        "project-other",
        "knowledge-other",
        "conversation-other-project",
        "artifact-other",
        "excluded-project-asset",
        "conversation-before",
        "excluded-before",
        "conversation-after",
        "excluded-after",
        "conversation-foreign",
        "foreign-tenant",
        "foreign-tenant-asset",
    ] {
        assert!(
            !archive_evidence.contains(excluded),
            "unselected evidence leaked into portable output: {excluded}"
        );
    }

    let manifest_bytes = read_member(&mut zip, "manifest.json");
    let manifest: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("manifest must be JSON");
    assert_eq!(manifest["filters"]["account_external_ref"], "account-alpha");
    assert_eq!(
        manifest["filters"]["project_external_id"],
        "project-selected"
    );
    assert_eq!(
        manifest["filters"]["observed_from_rfc3339"],
        "2026-08-27T00:00:00Z"
    );
    assert_eq!(
        manifest["filters"]["observed_to_rfc3339"],
        "2026-08-28T00:00:00Z"
    );
}
