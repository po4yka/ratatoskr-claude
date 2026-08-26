//! Durable, resumable state for one import pass over a received export.
//!
//! Stub module: the types exist so the failing tests compile, and every
//! operation reports [`ImportError::Unimplemented`] until the guarded
//! transition machine lands.

use sqlx::Row as _;
use uuid::Uuid;

use crate::database::Database;

/// The recorded progress of one import run, mirroring the schema CHECK
/// vocabulary exactly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportState {
    /// Bytes are being received.
    Received,
    /// The raw archive is durably stored.
    Stored,
    /// The container is being inspected within limits.
    Inspecting,
    /// A schema has been detected and a parser chosen.
    SchemaDetected,
    /// Entries are being extracted into isolation.
    Extracting,
    /// Records are being staged for validation.
    Staging,
    /// Relationships and assets are being validated.
    Validating,
    /// Normalized records are being reconciled.
    Reconciling,
    /// Results and events are being published.
    Publishing,
    /// Terminal: the pass finished completely.
    Completed,
    /// Terminal: the pass finished with visible warnings.
    Partial,
    /// Terminal: the pass failed terminally.
    Failed,
    /// Terminal: the run was set aside pending human decision.
    Quarantined,
}

impl ImportState {
    /// The database text for this state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Stored => "stored",
            Self::Inspecting => "inspecting",
            Self::SchemaDetected => "schema_detected",
            Self::Extracting => "extracting",
            Self::Staging => "staging",
            Self::Validating => "validating",
            Self::Reconciling => "reconciling",
            Self::Publishing => "publishing",
            Self::Completed => "completed",
            Self::Partial => "partial",
            Self::Failed => "failed",
            Self::Quarantined => "quarantined",
        }
    }

    /// Parses the database text for a state.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        let state = match text {
            "received" => Self::Received,
            "stored" => Self::Stored,
            "inspecting" => Self::Inspecting,
            "schema_detected" => Self::SchemaDetected,
            "extracting" => Self::Extracting,
            "staging" => Self::Staging,
            "validating" => Self::Validating,
            "reconciling" => Self::Reconciling,
            "publishing" => Self::Publishing,
            "completed" => Self::Completed,
            "partial" => Self::Partial,
            "failed" => Self::Failed,
            "quarantined" => Self::Quarantined,
            _ => return None,
        };
        Some(state)
    }

    /// The documented successor states: one step along the linear pipeline,
    /// and - from every non-terminal state - the visible terminal classes a
    /// pass may end in instead. A terminal state has none, so nothing can
    /// move a finished run anywhere else.
    #[must_use]
    pub const fn successors(self) -> &'static [Self] {
        match self {
            Self::Received => &[Self::Stored, Self::Partial, Self::Failed, Self::Quarantined],
            Self::Stored => &[
                Self::Inspecting,
                Self::Partial,
                Self::Failed,
                Self::Quarantined,
            ],
            Self::Inspecting => &[
                Self::SchemaDetected,
                Self::Partial,
                Self::Failed,
                Self::Quarantined,
            ],
            Self::SchemaDetected => &[
                Self::Extracting,
                Self::Partial,
                Self::Failed,
                Self::Quarantined,
            ],
            Self::Extracting => &[
                Self::Staging,
                Self::Partial,
                Self::Failed,
                Self::Quarantined,
            ],
            Self::Staging => &[
                Self::Validating,
                Self::Partial,
                Self::Failed,
                Self::Quarantined,
            ],
            Self::Validating => &[
                Self::Reconciling,
                Self::Partial,
                Self::Failed,
                Self::Quarantined,
            ],
            Self::Reconciling => &[
                Self::Publishing,
                Self::Partial,
                Self::Failed,
                Self::Quarantined,
            ],
            Self::Publishing => &[
                Self::Completed,
                Self::Partial,
                Self::Failed,
                Self::Quarantined,
            ],
            Self::Completed | Self::Partial | Self::Failed | Self::Quarantined => &[],
        }
    }

    /// Whether this state ends a run: no outgoing edge exists from here.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Partial | Self::Failed | Self::Quarantined
        )
    }
}

/// Failure of an import-run operation. Text names states and identifiers,
/// never archive content.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ImportError {
    /// No run carries the given identifier.
    #[error("no import run exists for this identifier")]
    RunMissing {
        /// The identifier that resolved to nothing.
        run_id: Uuid,
    },
    /// The recorded state text is not part of the machine, which the schema
    /// CHECK makes unreachable; handled honestly rather than panicked on.
    #[error("the recorded import state is not part of the machine")]
    CorruptState,
    /// A database operation failed.
    #[error("an import-run database operation failed")]
    Query(#[from] sqlx::Error),
    /// The recorded state differed from both the expected origin and the
    /// target: something else moved this run.
    #[error("the run is recorded in another state than the transition expects")]
    Conflict {
        /// The recorded state at refusal time.
        current: ImportState,
        /// The origin the refused command expected.
        expected_origin: ImportState,
    },
    /// The requested edge does not exist in the documented machine.
    #[error("the requested transition is not part of the import state machine")]
    InvalidTransition {
        /// The origin of the refused edge.
        from: ImportState,
        /// The target of the refused edge.
        to: ImportState,
    },
}

/// What one guarded transition actually did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionOutcome {
    /// The recorded state moved from the expected origin to the target.
    Advanced,
    /// The target was already the recorded state; nothing changed.
    AlreadyApplied,
}

/// Durable access to import-run rows.
#[derive(Debug, Clone)]
pub struct ImportRunStore {
    database: Database,
}

impl ImportRunStore {
    /// Builds a store over the owned database pool.
    #[must_use]
    pub fn new(database: &Database) -> Self {
        Self {
            database: database.clone(),
        }
    }

    /// Creates the initial run for an export in the initial state.
    ///
    /// # Errors
    ///
    /// Returns [`ImportError::Query`] when the insert fails.
    pub async fn create_initial(
        &self,
        run_id: Uuid,
        export_id: Uuid,
    ) -> Result<ImportState, ImportError> {
        sqlx::query(
            "insert into claude_archive.import_runs (run_id, export_id, state)
             values ($1, $2, $3)",
        )
        .bind(run_id)
        .bind(export_id)
        .bind(ImportState::Received.as_str())
        .execute(self.database.pool())
        .await
        .map_err(ImportError::Query)?;
        Ok(ImportState::Received)
    }

    /// Reads the currently recorded state of a run.
    ///
    /// # Errors
    ///
    /// Returns [`ImportError::RunMissing`] when no run carries this
    /// identifier and [`ImportError::Query`] when the read fails.
    pub async fn current(&self, run_id: Uuid) -> Result<ImportState, ImportError> {
        let row = sqlx::query("select state from claude_archive.import_runs where run_id = $1")
            .bind(run_id)
            .fetch_optional(self.database.pool())
            .await
            .map_err(ImportError::Query)?
            .ok_or(ImportError::RunMissing { run_id })?;
        let text = row.get::<String, _>("state");
        ImportState::parse(&text).ok_or(ImportError::CorruptState)
    }

    /// Applies one guarded transition.
    ///
    /// The update lands only when the recorded state still equals `from`, so
    /// concurrent or replayed commands can never overwrite a state another
    /// writer has already moved. A zero-row result is resolved by reading:
    /// the target already recorded means an idempotent replay; anything else
    /// is a conflict naming the recorded state.
    ///
    /// # Errors
    ///
    /// Returns [`ImportError::InvalidTransition`] when `to` is not a
    /// documented successor of `from`, [`ImportError::Conflict`] when the
    /// recorded state matches neither origin nor target,
    /// [`ImportError::RunMissing`] for an unknown run, and
    /// [`ImportError::Query`] on database failure.
    pub async fn advance(
        &self,
        run_id: Uuid,
        from: ImportState,
        to: ImportState,
    ) -> Result<TransitionOutcome, ImportError> {
        if !from.successors().contains(&to) {
            return Err(ImportError::InvalidTransition { from, to });
        }

        let applied = sqlx::query(
            "update claude_archive.import_runs set state = $2
             where run_id = $1 and state = $3",
        )
        .bind(run_id)
        .bind(to.as_str())
        .bind(from.as_str())
        .execute(self.database.pool())
        .await
        .map_err(ImportError::Query)?;

        if applied.rows_affected() == 1 {
            return Ok(TransitionOutcome::Advanced);
        }
        match self.current(run_id).await? {
            current if current == to => Ok(TransitionOutcome::AlreadyApplied),
            current => Err(ImportError::Conflict {
                current,
                expected_origin: from,
            }),
        }
    }
}
