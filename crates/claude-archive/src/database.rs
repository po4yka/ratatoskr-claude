//! The owned `PostgreSQL` pool and the embedded `claude_archive` schema.

use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

const SCHEMA: &str = include_str!("../../../schema.sql");
// "ratatoskr" in hexadecimal prefixes the fleet's per-repository schema locks;
// ordinal 06 belongs to this repository (01 platform, 02 extractor, 03 github,
// 04 knowledge, 05 instagram).
const SCHEMA_LOCK: i64 = 0x7261_7461_736b_7206;

/// Archive persistence failure with no connection details.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum PersistenceError {
    /// Database connection failed.
    #[error("the database connection could not be established")]
    Connect(#[source] sqlx::Error),
    /// Current schema application failed.
    #[error("the claude_archive schema could not be applied")]
    Schema(#[source] sqlx::Error),
    /// An archive-owned query failed.
    #[error("a claude_archive database query failed")]
    Query(#[source] sqlx::Error),
}

/// One finite database pool owned by the Claude bounded context.
#[derive(Debug, Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    /// Connects the finite pool.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError::Connect`] when the database is unavailable.
    pub async fn connect(
        url: &str,
        max_connections: u32,
        acquire_timeout: Duration,
    ) -> Result<Self, PersistenceError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(acquire_timeout)
            .connect(url)
            .await
            .map_err(PersistenceError::Connect)?;
        Ok(Self { pool })
    }

    /// Applies the current editable schema definition.
    ///
    /// One transaction takes a `PostgreSQL` advisory lock, asks whether
    /// `claude_archive` exists, and applies the file only if it does not.
    /// `PostgreSQL` DDL is transactional, so a file that fails halfway leaves
    /// the database exactly as it was rather than half-applied; under the
    /// lock, absence of the schema means the file has never been applied to
    /// this database.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError::Schema`] when the database refuses the
    /// lock, the catalogue read, or a statement in the file.
    pub async fn apply_schema(&self) -> Result<(), PersistenceError> {
        let mut transaction = self.pool.begin().await.map_err(PersistenceError::Schema)?;
        lock_and_apply(&mut transaction)
            .await
            .map_err(PersistenceError::Schema)?;
        transaction.commit().await.map_err(PersistenceError::Schema)
    }

    /// Answers whether the database is usable right now.
    ///
    /// A round trip, not a pool-state inspection: a pool with idle connections
    /// to a server that is refusing queries looks healthy from the inside.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError::Query`] when the round trip fails.
    pub async fn ping(&self) -> Result<(), PersistenceError> {
        sqlx::query("select 1")
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(PersistenceError::Query)
    }

    /// Returns the owned pool for archive queries.
    #[must_use]
    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Closes the finite pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }

    #[cfg(feature = "test-support")]
    pub(crate) const fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// The body of [`Database::apply_schema`], on one connection so the lock and
/// the apply share a session.
async fn lock_and_apply(connection: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
    sqlx::query("select pg_advisory_xact_lock($1)")
        .bind(SCHEMA_LOCK)
        .execute(&mut *connection)
        .await?;

    // The first statement of the file creates this schema. Under the lock, its
    // absence means the file has never been applied to this database.
    let present: Option<String> =
        sqlx::query_scalar("select to_regnamespace('claude_archive')::text")
            .fetch_one(&mut *connection)
            .await?;

    if present.is_none() {
        sqlx::Executor::execute(connection, SCHEMA).await?;
    }

    Ok(())
}
