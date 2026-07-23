use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use thiserror::Error;

use crate::frame::ProducerTuple;

const CONTENT_TYPE: &str = "application/vnd.quorum-loro.delta-frame.v1";
const HEADER_NEXT_OFFSET: &str = "stream-next-offset";
const HEADER_UP_TO_DATE: &str = "stream-up-to-date";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendOutcome {
    Committed { next_offset: u64 },
    Duplicate { next_offset: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionKind {
    Invalid,
    PermissionDenied,
    NotFound,
    Conflict,
    PayloadTooLarge,
    RateLimited,
    Other,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("ambiguous Ursula result: {0}")]
    Ambiguous(String),
    #[error("Ursula rejected the operation ({kind:?}): {message}")]
    Rejected {
        kind: RejectionKind,
        message: String,
    },
    #[error("stored data failed integrity verification: {0}")]
    Integrity(String),
}

#[async_trait]
pub trait UrsulaStore: Send + Sync {
    async fn ensure_stream(&self, stream: &str) -> Result<(), StoreError>;
    async fn read_all(&self, stream: &str) -> Result<Vec<u8>, StoreError>;
    async fn read_range(
        &self,
        stream: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, StoreError>;
    async fn append(
        &self,
        stream: &str,
        producer: &ProducerTuple,
        body: &[u8],
    ) -> Result<AppendOutcome, StoreError>;
}

#[derive(Debug, Clone)]
pub struct HttpUrsulaConfig {
    pub base_url: String,
    pub bucket: String,
    pub response_timeout: Duration,
    pub read_chunk_bytes: usize,
}

impl Default for HttpUrsulaConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:4437".into(),
            bucket: "qloro".into(),
            response_timeout: Duration::from_secs(30),
            read_chunk_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HttpUrsula {
    config: HttpUrsulaConfig,
    client: reqwest::Client,
}

impl HttpUrsula {
    pub fn new(config: HttpUrsulaConfig) -> Result<Self, StoreError> {
        if config.bucket.len() < 4 {
            return Err(StoreError::Rejected {
                kind: RejectionKind::Invalid,
                message: "Ursula bucket names must contain at least four bytes".into(),
            });
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| StoreError::Ambiguous(error.to_string()))?;
        Ok(Self { config, client })
    }

    fn stream_url(&self, stream: &str) -> String {
        format!(
            "{}/{}/{}",
            self.config.base_url.trim_end_matches('/'),
            self.config.bucket,
            stream
        )
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, StoreError> {
        tokio::time::timeout(self.config.response_timeout, request.send())
            .await
            .map_err(|_| StoreError::Ambiguous("response header timeout".into()))?
            .map_err(|error| StoreError::Ambiguous(error.to_string()))
    }

    async fn read_exact_window(
        &self,
        stream: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, u64, bool), StoreError> {
        let response = self
            .send(self.client.get(self.stream_url(stream)).query(&[
                ("offset", offset.to_string()),
                ("max_bytes", max_bytes.to_string()),
            ]))
            .await?;
        if response.status() != StatusCode::OK {
            return Err(classify_rejection(response.status(), "read stream"));
        }
        let next_offset = parse_offset(response.headers(), HEADER_NEXT_OFFSET)?;
        let up_to_date = response
            .headers()
            .get(HEADER_UP_TO_DATE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let body = response
            .bytes()
            .await
            .map_err(|error| StoreError::Ambiguous(error.to_string()))?
            .to_vec();
        Ok((body, next_offset, up_to_date))
    }
}

#[async_trait]
impl UrsulaStore for HttpUrsula {
    async fn ensure_stream(&self, stream: &str) -> Result<(), StoreError> {
        let bucket_url = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            self.config.bucket
        );
        let bucket = self.send(self.client.put(bucket_url)).await?;
        if !bucket.status().is_success() {
            return Err(classify_rejection(bucket.status(), "create bucket"));
        }
        let response = self
            .send(
                self.client
                    .put(self.stream_url(stream))
                    .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE),
            )
            .await?;
        if matches!(response.status(), StatusCode::CREATED | StatusCode::OK) {
            Ok(())
        } else {
            Err(classify_rejection(response.status(), "create stream"))
        }
    }

    async fn read_all(&self, stream: &str) -> Result<Vec<u8>, StoreError> {
        let mut output = Vec::new();
        let mut offset = 0_u64;
        loop {
            let (body, next_offset, up_to_date) = self
                .read_exact_window(stream, offset, self.config.read_chunk_bytes)
                .await?;
            if next_offset < offset || (next_offset == offset && !up_to_date) {
                return Err(StoreError::Integrity("read made no offset progress".into()));
            }
            output.extend_from_slice(&body);
            offset = next_offset;
            if up_to_date {
                return Ok(output);
            }
        }
    }

    async fn read_range(
        &self,
        stream: &str,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, StoreError> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let mut output = Vec::with_capacity(length);
        let mut next = offset;
        while output.len() < length {
            let remaining = length - output.len();
            let (body, returned_next, _) = self.read_exact_window(stream, next, remaining).await?;
            if returned_next <= next || body.is_empty() {
                return Err(StoreError::Integrity(
                    "committed range ended before expected frame length".into(),
                ));
            }
            output.extend_from_slice(&body);
            next = returned_next;
        }
        if output.len() != length {
            return Err(StoreError::Integrity(
                "committed range exceeded expected frame length".into(),
            ));
        }
        Ok(output)
    }

    async fn append(
        &self,
        stream: &str,
        producer: &ProducerTuple,
        body: &[u8],
    ) -> Result<AppendOutcome, StoreError> {
        let response = self
            .send(
                self.client
                    .post(self.stream_url(stream))
                    .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE)
                    .header("producer-id", &producer.id)
                    .header("producer-epoch", producer.epoch)
                    .header("producer-seq", producer.sequence)
                    .body(body.to_vec()),
            )
            .await?;
        let status = response.status();
        if matches!(status, StatusCode::OK | StatusCode::NO_CONTENT) {
            let next_offset = parse_offset(response.headers(), HEADER_NEXT_OFFSET)?;
            if status == StatusCode::OK {
                Ok(AppendOutcome::Committed { next_offset })
            } else {
                Ok(AppendOutcome::Duplicate { next_offset })
            }
        } else if status.is_server_error()
            || matches!(
                status,
                StatusCode::REQUEST_TIMEOUT | StatusCode::TOO_MANY_REQUESTS
            )
        {
            Err(StoreError::Ambiguous(format!(
                "append returned HTTP {status}"
            )))
        } else {
            Err(classify_rejection(status, "append"))
        }
    }
}

fn parse_offset(headers: &reqwest::header::HeaderMap, name: &str) -> Result<u64, StoreError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| StoreError::Integrity(format!("missing or invalid {name} header")))
}

fn classify_rejection(status: StatusCode, operation: &str) -> StoreError {
    let kind = match status {
        StatusCode::BAD_REQUEST => RejectionKind::Invalid,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => RejectionKind::PermissionDenied,
        StatusCode::NOT_FOUND | StatusCode::GONE => RejectionKind::NotFound,
        StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED => RejectionKind::Conflict,
        StatusCode::PAYLOAD_TOO_LARGE => RejectionKind::PayloadTooLarge,
        StatusCode::TOO_MANY_REQUESTS => RejectionKind::RateLimited,
        _ => RejectionKind::Other,
    };
    StoreError::Rejected {
        kind,
        message: format!("{operation} returned HTTP {status}"),
    }
}
