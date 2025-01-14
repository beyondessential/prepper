use std::{backtrace::Backtrace, fmt};

use pg_replicate::pipeline::sinks::SinkError;

#[derive(Debug)]
pub enum AuditSinkError {
	Io {
		source: std::io::Error,
		backtrace: Backtrace,
	},

	Json {
		source: serde_json::Error,
		backtrace: Backtrace,
	},

	Cbor {
		source: minicbor_io::Error,
		backtrace: Backtrace,
	},

	Jiff {
		source: jiff::Error,
		backtrace: Backtrace,
	},
}

impl From<std::io::Error> for AuditSinkError {
	fn from(source: std::io::Error) -> Self {
		Self::Io {
			source,
			backtrace: Backtrace::capture(),
		}
	}
}

impl From<serde_json::Error> for AuditSinkError {
	fn from(source: serde_json::Error) -> Self {
		Self::Json {
			source,
			backtrace: Backtrace::capture(),
		}
	}
}

impl From<minicbor_io::Error> for AuditSinkError {
	fn from(source: minicbor_io::Error) -> Self {
		Self::Cbor {
			source,
			backtrace: Backtrace::capture(),
		}
	}
}

impl From<jiff::Error> for AuditSinkError {
	fn from(source: jiff::Error) -> Self {
		Self::Jiff {
			source,
			backtrace: Backtrace::capture(),
		}
	}
}

impl fmt::Display for AuditSinkError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Io { source, backtrace } => write!(f, "io: {source}\n{backtrace}"),
			Self::Json { source, backtrace } => write!(f, "json: {source}\n{backtrace}"),
			Self::Cbor { source, backtrace } => write!(f, "cbor: {source}\n{backtrace}"),
			Self::Jiff { source, backtrace } => write!(f, "jiff: {source}\n{backtrace}"),
		}
	}
}

impl std::error::Error for AuditSinkError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Self::Io { source, .. } => Some(source),
			Self::Json { source, .. } => Some(source),
			Self::Cbor { source, .. } => Some(source),
			Self::Jiff { source, .. } => Some(source),
		}
	}

	fn cause(&self) -> Option<&dyn std::error::Error> {
		self.source()
	}
}

impl SinkError for AuditSinkError {}
