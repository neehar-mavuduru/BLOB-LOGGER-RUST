/// Unified error type for the blob-logger-rust crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("buffer full — log dropped")]
    BufferFull,

    #[error("logger is closed")]
    LoggerClosed,

    #[error("mmap error: {0}")]
    Mmap(String),

    #[error("pwritev error: {0}")]
    Pwritev(String),

    #[error("GCS error: {0}")]
    Gcs(String),

    #[error("invalid event name: {0}")]
    InvalidEventName(String),

    #[error("uploader error: {0}")]
    Uploader(String),
}
