use std::{path::PathBuf, time::Duration};

use clap::{CommandFactory, Parser, ValueHint};
use miette::{IntoDiagnostic, Result};
use pg_connection_string::ConnectionString;
use pg_replicate::pipeline::{
    batching::{data_pipeline::BatchDataPipeline, BatchConfig},
    sources::postgres::{PostgresSource, TableNamesFrom},
    PipelineAction,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt as _};

mod audit_sink;
mod event;
mod postgres_sink;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// Connection string to Postgres in libpq format
    #[arg(short = 'P', long, value_name = "POSTGRES_CONNSTRING")]
    pg: ConnectionString,

    /// Output directory
    #[arg(short = 'O', long = "out", value_name = "PATH", value_hint = ValueHint::DirPath)]
    out_dir: PathBuf,

    /// Maximum batch size
    #[arg(long, default_value = "100")]
    max_batch_size: usize,

    /// Batch timeout in seconds
    #[arg(long, default_value = "10")]
    max_batch_time: u64,
}

#[tokio::main]

async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    let source = PostgresSource::new(
        &args.pg.hostspecs[0].host.to_string(),
        args.pg.hostspecs[0].port.unwrap_or(5432),
        &args.pg.database.unwrap_or_default(),
        &args.pg.user.unwrap_or_default(),
        args.pg.password,
        args.pg.parameters.iter().find_map(|p| {
            if p.keyword == "slot" {
                Some(p.value.clone())
            } else {
                None
            }
        }),
        TableNamesFrom::Publication(
            args.pg
                .parameters
                .iter()
                .find_map(|p| {
                    if p.keyword == "pub" {
                        Some(p.value.clone())
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| {
                    Args::command()
                        .error(
                            clap::error::ErrorKind::InvalidValue,
                            "missing parameter pub",
                        )
                        .exit()
                }),
        ),
    )
    .await
    .into_diagnostic()?;

    let sink = audit_sink::AuditSink::new(args.out_dir);

    let batch_config = BatchConfig::new(
        args.max_batch_size,
        Duration::from_secs(args.max_batch_time),
    );
    let mut pipeline = BatchDataPipeline::new(source, sink, PipelineAction::Both, batch_config);

    pipeline.start().await.into_diagnostic()?;

    Ok(())
}
