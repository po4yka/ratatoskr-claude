//! Disposable database support for integration tests.
//!
//! Each test creates its own throwaway database rather than sharing one: the
//! behaviors worth testing here — constraint refusal, catalog shape,
//! re-application — need a database whose contents no other test has touched.

use sqlx::Executor as _;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use uuid::Uuid;

use crate::database::{Database, PersistenceError};

/// How many connections one test may hold. The suite runs several test
/// binaries at once and each test owns a database, so larger pools exhaust
/// the server's connection budget before they make anything faster.
const TEST_POOL_SIZE: u32 = 2;

/// Where disposable databases are created.
///
/// `CLAUDE_ARCHIVE_TEST_DATABASE_URL` overrides it; the default matches
/// `compose.yaml`, so `docker compose up -d` followed by `cargo test` works
/// with no further setup.
///
/// # Panics
///
/// Never in normal operation; the environment read is the one sanctioned
/// exception to the closed-config rule because it names where tests may
/// create databases, which is not process configuration at all.
#[must_use]
#[expect(
    clippy::disallowed_methods,
    reason = "test-only database location is not process configuration"
)]
pub fn admin_url() -> String {
    match std::env::var("CLAUDE_ARCHIVE_TEST_DATABASE_URL") {
        Ok(value) => value,
        Err(_) => "postgres://claude:claude@127.0.0.1:5438/claude".to_owned(),
    }
}

/// An isolated disposable archive database.
#[derive(Debug)]
pub struct TestDatabase {
    /// Connected archive database, ready for queries.
    pub database: Database,
    name: String,
}

impl TestDatabase {
    /// Creates an isolated database and applies the current schema definition.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when database creation or application
    /// fails. A missing server is a real failure, never a skip.
    pub async fn create() -> Result<Self, PersistenceError> {
        let database = Self::create_raw().await?;
        database.database.apply_schema().await?;
        Ok(database)
    }

    /// Creates an isolated database WITHOUT applying the schema, for tests
    /// that drive application themselves (idempotency, concurrency).
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when database creation fails.
    pub async fn create_raw() -> Result<Self, PersistenceError> {
        let name = format!("claude_archive_test_{}", Uuid::now_v7().simple());
        let admin_url = admin_url();
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .map_err(PersistenceError::Connect)?;
        // The name is generated from a UUID, so it cannot carry an injection;
        // PostgreSQL has no bind parameters for identifiers in DDL.
        //
        // The locale is stated rather than inherited from template1, whose
        // collation is a property of whatever cluster happened to start:
        // ICU here matches compose.yaml, CI, and every other repository in
        // the fleet that checks text ordering against this one.
        admin
            .execute(
                format!(
                    r#"create database "{name}" template template0
                       locale_provider icu icu_locale 'und-x-icu' encoding 'UTF8'"#
                )
                .as_str(),
            )
            .await
            .map_err(PersistenceError::Query)?;
        admin.close().await;

        let options = admin_url
            .parse::<PgConnectOptions>()
            .map_err(PersistenceError::Connect)?
            .database(&name);
        let pool = PgPoolOptions::new()
            .max_connections(TEST_POOL_SIZE)
            .connect_with(options)
            .await
            .map_err(PersistenceError::Connect)?;

        Ok(Self {
            database: Database::from_pool(pool),
            name,
        })
    }

    /// The generated database name, for assertions about existence.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Closes the pool and drops the database.
    ///
    /// Explicit rather than a `Drop` impl: dropping requires async work, and
    /// a blocking drop inside a Tokio worker deadlocks. A test that panics
    /// leaves its database behind on purpose while the failure is read.
    ///
    /// # Errors
    ///
    /// Returns [`PersistenceError`] when cleanup fails.
    pub async fn cleanup(self) -> Result<(), PersistenceError> {
        self.database.close().await;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url())
            .await
            .map_err(PersistenceError::Connect)?;
        admin
            .execute(format!(r#"drop database if exists "{}" with (force)"#, self.name).as_str())
            .await
            .map_err(PersistenceError::Query)?;
        admin.close().await;
        Ok(())
    }
}
