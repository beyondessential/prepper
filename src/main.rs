use std::{error::Error, time::Duration};

use pg_replicate::pipeline::{
    batching::{data_pipeline::BatchDataPipeline, BatchConfig},
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt as _};

mod audit_sink;
mod postgres_sink;

#[tokio::main]

async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "prepper=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let source = PostgresSource::new(
        "/var/run/postgresql",
        5432,
        "prepper",
        "passcod",
        None,
        Some("prepper_slot".into()),
        TableNamesFrom::Publication("prepper_pub".into()),
    )
    .await?;

    let sink = audit_sink::AuditSink::new("history".into());

    let batch_config = BatchConfig::new(100, Duration::from_secs(10));
    let mut pipeline = BatchDataPipeline::new(source, sink, PipelineAction::Both, batch_config);

    pipeline.start().await?;

    Ok(())
}
