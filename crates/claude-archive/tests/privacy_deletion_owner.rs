//! A deletion addresses every tombstone to the owner the subject was announced under, and never
//! reads the configured account mapping (XR-021 CONTRACTS.md R5-06 rules 2 to 5).
//!
//! Every test drives the real receipt, import and deletion path against a real `PostgreSQL`. The
//! deletion request names the account by its archive account id, the reference the operator
//! supplies.
//!
//! Verified only against the documented synthetic export schema.

#![expect(
    clippy::expect_used,
    reason = "integration-test scaffolding: a failed setup step must fail the test loudly"
)]

mod synthetic_import;

use std::collections::BTreeMap;

use ratatoskr_ai_archive_contracts::{AiArchiveTombstone, AiArchiveTombstoneSubject};
use ratatoskr_claude_archive::blob_store::scratch::{remove, temp_root};
use ratatoskr_claude_archive::privacy_deletion::{
    PrivacyDeletionExecutor, PrivacyDeletionPlanner, RawExportDeletionPlanRequest,
    TenantDeletionPlanRequest,
};
use ratatoskr_claude_archive::test_support::TestDatabase;
use ratatoskr_claude_archive::{BlobStore, InitialImportWorker, MediaType, PlatformUsers};
use ratatoskr_event_envelope::EventEnvelope;
use sqlx::PgPool;
use synthetic_import::{account, receive, versions, worker, zip};
use uuid::Uuid;

const USER_A: &str = "018f0000-0000-7000-8000-0000000000a1";
const USER_B: &str = "018f0000-0000-7000-8000-0000000000b1";
const TOMBSTONED: &str = "ai_archive.subject.tombstoned.v1";

fn artifacts() -> serde_json::Value {
    serde_json::json!([{
        "id": "react-component", "type": "text/markdown", "project_id": "project-garden",
        "versions": versions()
    }])
}

fn users(user: &str, account: Uuid) -> PlatformUsers {
    PlatformUsers::new(&[(Uuid::parse_str(user).expect("user UUID"), account)])
}

struct Fixture {
    db: TestDatabase,
    store: BlobStore,
    root: std::path::PathBuf,
    account: Uuid,
}

impl Fixture {
    async fn open(label: &str) -> Self {
        let db = TestDatabase::create().await.expect("a disposable database");
        let account = account(&db).await;
        let root = temp_root(label);
        let store = BlobStore::open(&root).expect("the blob store opens");
        Self {
            db,
            store,
            root,
            account,
        }
    }

    fn pool(&self) -> &PgPool {
        self.db.database.pool()
    }

    fn worker(&self, users: PlatformUsers) -> InitialImportWorker {
        worker(&self.db, &self.store, users)
    }

    async fn import(&self, worker: &InitialImportWorker, note: &str, conversations: bool) {
        receive(
            &self.db,
            &self.store,
            self.account,
            &zip(note, artifacts(), conversations),
        )
        .await;
        assert_eq!(worker.process_pending_once().await.expect("imports"), 1);
    }

    /// The reference the operator supplies: the archive account id, as given.
    fn supplied(&self) -> String {
        self.account.to_string()
    }

    async fn execute(&self, request: Uuid) {
        let evidence = self
            .store
            .store(
                MediaType::parse("application/json").expect("canonical media type"),
                b"{}",
            )
            .expect("the evidence blob is stored");
        PrivacyDeletionExecutor::new(self.pool().clone())
            .finalize(request, &evidence, None)
            .await
            .expect("the deletion completes");
    }

    async fn delete_tenant(&self, key: &str) {
        let request = Uuid::now_v7();
        PrivacyDeletionPlanner::new(self.pool().clone())
            .plan_tenant(&TenantDeletionPlanRequest {
                tenant_ref: self.supplied(),
                account_id: self.account,
                request_id: request,
                request_key: key.to_owned(),
                correlation_id: format!("privacy-delete:{request}"),
            })
            .await
            .expect("the tenant deletion is planned");
        self.execute(request).await;
    }

    async fn delete_export(&self, export_id: Uuid, key: &str) {
        let request = Uuid::now_v7();
        PrivacyDeletionPlanner::new(self.pool().clone())
            .plan_raw_export(&RawExportDeletionPlanRequest {
                tenant_ref: self.supplied(),
                account_id: self.account,
                request_id: request,
                request_key: key.to_owned(),
                correlation_id: format!("privacy-delete:{request}"),
                export_id,
            })
            .await
            .expect("the export deletion is planned");
        self.execute(request).await;
    }

    async fn finish(self) {
        remove(&self.root);
        self.db.cleanup().await.expect("cleanup succeeds");
    }
}

/// `(subject, owner)` of every tombstone, in a stable spelling.
async fn tombstones(pool: &PgPool) -> Vec<(String, String)> {
    let rows: Vec<serde_json::Value> = sqlx::query_scalar(
        "select envelope from claude_archive.outbox_events where event_type = $1",
    )
    .bind(TOMBSTONED)
    .fetch_all(pool)
    .await
    .expect("the tombstone rows are readable");
    let mut found: Vec<(String, String)> = rows
        .into_iter()
        .map(|raw| {
            let envelope: EventEnvelope = serde_json::from_value(raw).expect("a complete envelope");
            let tombstone = envelope
                .payload_as::<AiArchiveTombstone>()
                .expect("the payload is a tombstone");
            let subject = match tombstone.subject {
                AiArchiveTombstoneSubject::Archive => {
                    format!("archive:{}", tombstone.ai_archive_id.0)
                }
                AiArchiveTombstoneSubject::Conversation { ai_conversation_id } => {
                    format!("conversation:{}", ai_conversation_id.0)
                }
                AiArchiveTombstoneSubject::Project { ai_project_id } => {
                    format!("project:{}", ai_project_id.0)
                }
                AiArchiveTombstoneSubject::Artifact {
                    external_artifact_id,
                } => format!("artifact:{external_artifact_id}"),
            };
            assert_eq!(
                envelope.tenant_id,
                Some(tombstone.owner),
                "the envelope tenant is the payload owner"
            );
            (subject, tombstone.owner.to_string())
        })
        .collect();
    found.sort();
    found
}

fn owners_of(found: &[(String, String)], prefix: &str) -> Vec<String> {
    found
        .iter()
        .filter(|(subject, _)| subject.starts_with(prefix))
        .map(|(_, owner)| owner.clone())
        .collect()
}

#[tokio::test]
async fn a_deletion_names_the_owner_the_subjects_were_announced_under() {
    let fixture = Fixture::open("owner-announced").await;
    let worker = fixture.worker(users(USER_A, fixture.account));
    fixture.import(&worker, "first", false).await;
    // The mapping no longer lists the account when the deletion runs; the deletion never reads it.
    fixture.delete_tenant("announced").await;
    let found = tombstones(fixture.pool()).await;
    fixture.finish().await;

    assert!(
        !found.is_empty(),
        "the deletion planned, executed and tombstoned the announced subjects"
    );
    for (subject, owner) in &found {
        assert_eq!(owner, &format!("user:{USER_A}"), "{subject} owner");
    }
    for kind in ["archive:", "conversation:", "project:"] {
        assert!(
            found.iter().any(|(subject, _)| subject.starts_with(kind)),
            "a {kind} tombstone was written"
        );
    }
}

#[tokio::test]
async fn archives_announced_under_two_users_are_tombstoned_to_their_own_owner() {
    let fixture = Fixture::open("owner-two-users").await;
    let first = fixture.worker(users(USER_A, fixture.account));
    fixture.import(&first, "first", false).await;
    // The mapping changed between the imports.
    let second = fixture.worker(users(USER_B, fixture.account));
    fixture.import(&second, "second", false).await;
    let archives: Vec<Uuid> =
        sqlx::query_scalar("select ai_archive_id from claude_archive.exports order by received_at")
            .fetch_all(fixture.pool())
            .await
            .expect("the archives are readable");
    fixture.delete_tenant("two-users").await;
    let found = tombstones(fixture.pool()).await;
    fixture.finish().await;

    let [first_archive, second_archive] =
        <[Uuid; 2]>::try_from(archives).expect("two archives were received");
    let by_subject: BTreeMap<&str, Vec<&str>> =
        found
            .iter()
            .fold(BTreeMap::new(), |mut grouped, (subject, owner)| {
                grouped.entry(subject.as_str()).or_default().push(owner);
                grouped
            });
    assert_eq!(
        by_subject.get(format!("archive:{first_archive}").as_str()),
        Some(&vec![format!("user:{USER_A}").as_str()]),
        "the first archive goes to the user it was announced under"
    );
    assert_eq!(
        by_subject.get(format!("archive:{second_archive}").as_str()),
        Some(&vec![format!("user:{USER_B}").as_str()]),
        "the second archive goes to the user it was announced under"
    );
    let conversation_owners = owners_of(&found, "conversation:");
    assert_eq!(
        conversation_owners,
        vec![format!("user:{USER_A}"), format!("user:{USER_B}")],
        "the shared conversation has one tombstone per owner"
    );
    let project_owners = owners_of(&found, "project:");
    assert_eq!(
        project_owners.len(),
        4,
        "two projects, each announced to both users"
    );
    assert!(
        found
            .iter()
            .all(|(_, owner)| owner == &format!("user:{USER_A}")
                || owner == &format!("user:{USER_B}")),
        "no tombstone names a third user"
    );
}

#[tokio::test]
async fn an_account_that_never_announced_completes_with_no_tombstone() {
    let fixture = Fixture::open("owner-unannounced").await;
    let unmapped = fixture.worker(PlatformUsers::default());
    fixture.import(&unmapped, "first", false).await;
    fixture.delete_tenant("unannounced").await;
    let found = tombstones(fixture.pool()).await;
    let state: String =
        sqlx::query_scalar("select state from claude_archive.privacy_deletion_requests limit 1")
            .fetch_one(fixture.pool())
            .await
            .expect("the request is readable");
    let exports: i64 = sqlx::query_scalar("select count(*) from claude_archive.exports")
        .fetch_one(fixture.pool())
        .await
        .expect("the exports are countable");
    fixture.finish().await;

    assert_eq!(state, "completed");
    assert_eq!(exports, 0, "the rows were removed");
    assert!(
        found.is_empty(),
        "nothing was announced, so nothing is tombstoned"
    );
}

#[tokio::test]
async fn the_request_is_recorded_under_the_supplied_reference() {
    let fixture = Fixture::open("owner-supplied").await;
    let worker = fixture.worker(users(USER_A, fixture.account));
    fixture.import(&worker, "first", false).await;
    fixture.delete_tenant("supplied").await;
    let recorded: String = sqlx::query_scalar(
        "select tenant_ref from claude_archive.privacy_deletion_requests limit 1",
    )
    .fetch_one(fixture.pool())
    .await
    .expect("the request is readable");
    let supplied = fixture.supplied();
    fixture.finish().await;

    assert_eq!(
        recorded, supplied,
        "the supplied reference is stored as given"
    );
}

#[tokio::test]
async fn the_ledger_rows_of_a_tombstoned_subject_are_gone_and_no_other_rows_are_touched() {
    let fixture = Fixture::open("owner-ledger-rows").await;
    let worker = fixture.worker(users(USER_A, fixture.account));
    fixture.import(&worker, "first", false).await;
    fixture.import(&worker, "second", true).await;
    let exports: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "select export_id, ai_archive_id from claude_archive.exports order by received_at",
    )
    .fetch_all(fixture.pool())
    .await
    .expect("the exports are readable");
    let [(_, first_archive), (second_export, second_archive)] =
        <[(Uuid, Uuid); 2]>::try_from(exports).expect("two exports were received");
    let pruning: Uuid = sqlx::query_scalar(
        "select conversation_id from claude_archive.conversations
         where external_conversation_id = 'conversation-pruning'",
    )
    .fetch_one(fixture.pool())
    .await
    .expect("the pruning conversation exists");
    let before: Vec<(String, String)> = sqlx::query_as(
        "select subject_kind, subject_id from claude_archive.announced_subjects
         order by subject_kind, subject_id",
    )
    .fetch_all(fixture.pool())
    .await
    .expect("the ledger is readable");
    fixture.delete_export(second_export, "ledger-rows").await;
    let after: Vec<(String, String)> = sqlx::query_as(
        "select subject_kind, subject_id from claude_archive.announced_subjects
         order by subject_kind, subject_id",
    )
    .fetch_all(fixture.pool())
    .await
    .expect("the ledger is readable");
    let found = tombstones(fixture.pool()).await;
    fixture.finish().await;

    let gone = [
        ("archive".to_owned(), second_archive.to_string()),
        ("conversation".to_owned(), pruning.to_string()),
    ];
    for row in &gone {
        assert!(before.contains(row), "{row:?} was announced");
        assert!(
            !after.contains(row),
            "{row:?} left the ledger with its tombstone"
        );
    }
    let expected: Vec<(String, String)> = before
        .iter()
        .filter(|row| !gone.contains(row))
        .cloned()
        .collect();
    assert_eq!(
        after, expected,
        "every other subject keeps its row, including the archive that stays"
    );
    assert!(
        after.contains(&("archive".to_owned(), first_archive.to_string())),
        "the retained archive keeps its row"
    );
    assert_eq!(
        owners_of(&found, "conversation:"),
        vec![format!("user:{USER_A}")],
        "only the pruning conversation, which only the deleted export held, is tombstoned"
    );
}

#[tokio::test]
async fn a_re_announced_survivor_carries_the_retained_archives_owner() {
    let fixture = Fixture::open("owner-survivor").await;
    let first = fixture.worker(users(USER_A, fixture.account));
    fixture.import(&first, "first", false).await;
    fixture.import(&first, "second", false).await;
    // A later import under another user must not become the survivors' owner.
    let later = fixture.worker(users(USER_B, fixture.account));
    fixture.import(&later, "third", false).await;
    let deleted: Uuid = sqlx::query_scalar(
        "select export_id from claude_archive.exports order by received_at limit 1",
    )
    .fetch_one(fixture.pool())
    .await
    .expect("the first export exists");
    fixture.delete_export(deleted, "survivor").await;
    let follow_up: Vec<(String, Option<String>)> = sqlx::query_as(
        "select event_type, tenant_ref from claude_archive.outbox_events
         where event_type like 'ai_archive.%.updated.v1'
           and occurred_at > (select max(occurred_at) from claude_archive.outbox_events
                              where event_type = 'ai_archive.subject.tombstoned.v1')
         order by occurred_at",
    )
    .fetch_all(fixture.pool())
    .await
    .expect("the follow-up facts are readable");
    fixture.finish().await;

    assert!(
        !follow_up.is_empty(),
        "the garden project and the conversation are announced again after the tombstone"
    );
    for (event_type, owner) in follow_up {
        assert_eq!(
            owner,
            Some(format!("user:{USER_A}")),
            "{event_type} carries the owner of the retained archive's ledger row"
        );
    }
}

#[test]
fn deletion_reads_no_account_mapping() {
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut scanned = 0;
    for entry in walk(&source.join("privacy_deletion")) {
        let text = std::fs::read_to_string(&entry).expect("a deletion source is readable");
        scanned += 1;
        for forbidden in [
            "require_platform_owner",
            "deletion_tenant_ref",
            "PlatformUsers",
            "tenant_ref_of",
        ] {
            assert!(
                !text.contains(forbidden),
                "{} still names {forbidden}",
                entry.display()
            );
        }
    }
    let boundary = std::fs::read_to_string(source.join("privacy_deletion.rs"))
        .expect("the deletion boundary is readable");
    assert!(!boundary.contains("require_platform_owner"));
    let reparse =
        std::fs::read_to_string(source.join("reparse.rs")).expect("the reparse source is readable");
    assert!(
        !reparse.contains("tenant_ref_of"),
        "reparse keeps its import-time use of PlatformUsers and drops the deletion lookup"
    );
    assert!(scanned >= 5, "the scan covered the deletion modules");
}

fn walk(directory: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(directory).expect("the directory is readable") {
        let path = entry.expect("an entry is readable").path();
        if path.is_dir() {
            files.extend(walk(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    files
}
