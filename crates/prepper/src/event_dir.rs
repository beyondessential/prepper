use std::path::{Path, PathBuf};

use minicbor_io::AsyncWriter;
use prepper_event::{Device, Event, Snapshot, Table, VERSION};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use crate::error::AuditSinkError;

#[derive(Debug)]
struct OpenFile {
    ts: jiff::Timestamp,
    inner: AsyncWriter<Compat<tokio::fs::File>>,
}

impl OpenFile {
    async fn new(
        root: &Path,
        device: Uuid,
        ts: Option<jiff::Timestamp>,
    ) -> Result<Self, AuditSinkError> {
        let ts = ts.unwrap_or_else(|| jiff::Timestamp::now());
        let path = root.join(format!(
            "events-{}-{}.prep",
            device.as_simple(),
            ts.as_nanosecond()
        ));
        debug!(?ts, ?path, "opening file");

        let file = tokio::fs::File::create_new(path).await?;
        let inner = AsyncWriter::new(file.compat_write());
        Ok(Self { ts, inner })
    }
}

#[derive(Debug)]
pub struct EventDir {
    root: PathBuf,
    device_id: Uuid,
    rotate_interval: jiff::Span,
    current: Option<OpenFile>,
}

impl EventDir {
    #[instrument(level = "debug")]
    pub fn new(root: PathBuf, device_id: Uuid) -> Self {
        Self {
            root,
            device_id,
            rotate_interval: jiff::Span::new().minutes(1),
            current: None,
        }
    }

	pub fn root(&self) -> &Path {
		self.root.as_path()
	}

    fn device(&self) -> Device {
        Device {
            id: self.device_id,
            ts: jiff::Timestamp::now().into(),
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub fn should_rotate(&self) -> bool {
        if let Some(OpenFile { ts, .. }) = &self.current {
            ts.saturating_add(self.rotate_interval) < jiff::Timestamp::now()
        } else {
            true
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn rotate(&mut self, ts: Option<jiff::Timestamp>) -> Result<(), AuditSinkError> {
        if let Some(OpenFile { mut inner, ts, .. }) = self
            .current
            .replace(OpenFile::new(&self.root, self.device_id, ts).await?)
        {
            debug!(?ts, "closing file");
            inner.flush().await?;
            let (file, _) = inner.into_parts();
            file.into_inner().sync_data().await?;
        }

        Ok(())
    }

    #[instrument(level = "debug", skip(self, events))]
    pub async fn write_events(
        &mut self,
        mut events: impl Iterator<Item = Result<(Table, Snapshot), AuditSinkError>>,
    ) -> Result<(), AuditSinkError> {
        let first_event = match events.next() {
			Some(Ok(event)) => event,
			Some(Err(err)) => return Err(err),
			None => return Ok(()),
            // if no rows, do nothing, don't even rotate needlessly
			// this will also not process the iterator, so make sure it's lazy!
        };

        let device = self.device();

        let first_event = Event {
            version: VERSION,
            table: first_event.0,
            snapshot: first_event.1,
            device,
        };

        if self.should_rotate() {
            // use the ts of the first object to name the file
            self.rotate(Some(device.ts.0)).await?;
        }

        // UNWRAP: above rotation guarantees current is Some
        let writer = &mut self.current.as_mut().unwrap().inner;
        writer.write(first_event).await?;

        // process remaining events
        for event in events {
			let (table, snapshot) = event?;
            writer
                .write(Event {
                    table,
                    snapshot,
                    device,
                    version: VERSION,
                })
                .await?;
        }

        writer.flush().await?;
        Ok(())
    }
}
