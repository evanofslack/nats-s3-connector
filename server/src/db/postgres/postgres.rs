use anyhow::Result;
use bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use refinery::embed_migrations;
use tokio_postgres::{Config as PgConfig, NoTls};
use tracing::{debug, error, info};

use crate::db::JobStoreError;

embed_migrations!("./src/db/postgres/migrations");

#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresStore {
    pub async fn new(database_url: &str) -> Result<Self, JobStoreError> {
        let config: PgConfig = database_url
            .parse()
            .map_err(|e| JobStoreError::Pool(format!("invalid config: {}", e)))?;

        let manager = PostgresConnectionManager::new(config, NoTls);
        let pool = Pool::builder()
            .build(manager)
            .await
            .map_err(|e| JobStoreError::Pool(e.to_string()))?;

        Ok(Self { pool })
    }

    pub async fn migrate(&self) -> Result<(), JobStoreError> {
        debug!("start run migrations");
        let mut client = self.get_client().await?;

        migrations::runner()
            .run_async(&mut *client)
            .await
            .map_err(|e| {
                error!("migration failed: {}", e);
                JobStoreError::Pool(format!("migration failed: {}", e))
            })?;

        info!("finish run migrations");
        Ok(())
    }

    pub async fn get_client(
        &self,
    ) -> Result<PooledConnection<'_, PostgresConnectionManager<NoTls>>, JobStoreError> {
        self.pool
            .get()
            .await
            .map_err(|e| JobStoreError::Pool(e.to_string()))
    }
}
