use anyhow::Result;
use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use refinery::embed_migrations;
use thiserror::Error;
use tokio_postgres::{Config as PgConfig, NoTls};
use tracing::{debug, error, trace};

embed_migrations!("./src/db/postgres/migrations");

#[derive(Error, Debug)]
pub enum PostgresError {
    #[error(transparent)]
    Database(#[from] tokio_postgres::Error),

    #[error("connection pool error: {0}")]
    Pool(String),
}

#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresStore {
    pub async fn new(database_url: &str) -> Result<Self, PostgresError> {
        let config: PgConfig = database_url
            .parse()
            .map_err(|e| PostgresError::Pool(format!("invalid config: {}", e)))?;

        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder()
            .build(manager)
            .await
            .map_err(|e| PostgresError::Pool(e.to_string()))?;

        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), PostgresError> {
        trace!("start run migrations");
        let mut client = self.get_client().await?;

        let runner = migrations::runner();

        for migration in runner.get_migrations() {
            debug!(
                name = migration.name(),
                checksum = migration.checksum(),
                "migration checksum",
            );
        }

        runner.run_async(&mut *client).await.map_err(|e| {
            error!(error = ?e, "fail migration");
            PostgresError::Pool(format!("migration failed: {}", e))
        })?;

        debug!("finish run migrations");
        Ok(())
    }

    pub async fn get_client(
        &self,
    ) -> Result<PooledConnection<'_, PostgresConnectionManager<NoTls>>, PostgresError> {
        self.pool
            .get()
            .await
            .map_err(|e| PostgresError::Pool(e.to_string()))
    }
}
