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
    Unsupported,
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
    pub max_stream_bytes: usize,
    pub safe_retries: usize,
    pub retry_base_delay: Duration,
    pub retry_max_delay: Duration,
}

impl Default for HttpUrsulaConfig {
    fn default() -> Self {
        Self {
            base_url: "http://127.0.0.1:4437".into(),
            bucket: "qloro".into(),
            response_timeout: Duration::from_secs(30),
            read_chunk_bytes: 1024 * 1024,
            max_stream_bytes: 512 * 1024 * 1024,
            safe_retries: 3,
            retry_base_delay: Duration::from_millis(25),
            retry_max_delay: Duration::from_secs(1),
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
        if config.read_chunk_bytes == 0 || config.max_stream_bytes == 0 {
            return Err(StoreError::Rejected {
                kind: RejectionKind::Invalid,
                message: "Ursula read limits must be non-zero".into(),
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

    async fn send_safe<F>(
        &self,
        operation: &str,
        request: F,
    ) -> Result<reqwest::Response, StoreError>
    where
        F: Fn() -> reqwest::RequestBuilder,
    {
        for attempt in 0..=self.config.safe_retries {
            match self.send(request()).await {
                Ok(response)
                    if is_retryable_status(response.status())
                        && attempt < self.config.safe_retries =>
                {
                    self.retry_sleep(attempt).await;
                }
                Ok(response) => return Ok(response),
                Err(StoreError::Ambiguous(_)) if attempt < self.config.safe_retries => {
                    self.retry_sleep(attempt).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(StoreError::Ambiguous(format!(
            "{operation} exhausted safe retries"
        )))
    }

    async fn retry_sleep(&self, attempt: usize) {
        tokio::time::sleep(retry_delay(
            self.config.retry_base_delay,
            self.config.retry_max_delay,
            attempt,
        ))
        .await;
    }

    async fn read_body_bounded(
        &self,
        mut response: reqwest::Response,
        max_bytes: usize,
    ) -> Result<Vec<u8>, StoreError> {
        tokio::time::timeout(self.config.response_timeout, async {
            let mut output = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|error| StoreError::Ambiguous(error.to_string()))?
            {
                let next_len = output
                    .len()
                    .checked_add(chunk.len())
                    .ok_or_else(|| StoreError::Integrity("response body length overflow".into()))?;
                if next_len > max_bytes {
                    return Err(StoreError::Integrity(format!(
                        "response body exceeds requested maximum of {max_bytes} bytes"
                    )));
                }
                output.extend_from_slice(&chunk);
            }
            Ok(output)
        })
        .await
        .map_err(|_| StoreError::Ambiguous("response body timeout".into()))?
    }

    async fn read_exact_window(
        &self,
        stream: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, u64, bool), StoreError> {
        for attempt in 0..=self.config.safe_retries {
            match self.read_exact_window_once(stream, offset, max_bytes).await {
                Err(StoreError::Ambiguous(_)) if attempt < self.config.safe_retries => {
                    self.retry_sleep(attempt).await;
                }
                Err(StoreError::Rejected {
                    kind: RejectionKind::RateLimited,
                    ..
                }) if attempt < self.config.safe_retries => {
                    self.retry_sleep(attempt).await;
                }
                result => return result,
            }
        }
        Err(StoreError::Ambiguous(
            "read stream exhausted safe retries".into(),
        ))
    }

    async fn read_exact_window_once(
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
            if is_unknown_status(response.status()) {
                return Err(StoreError::Ambiguous(format!(
                    "read stream returned HTTP {}",
                    response.status()
                )));
            }
            return Err(classify_rejection(response.status(), "read stream"));
        }
        let next_offset = parse_offset(response.headers(), HEADER_NEXT_OFFSET)?;
        let up_to_date = response
            .headers()
            .get(HEADER_UP_TO_DATE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let body = self.read_body_bounded(response, max_bytes).await?;
        let body_len = u64::try_from(body.len())
            .map_err(|_| StoreError::Integrity("response body length does not fit u64".into()))?;
        let expected_next = offset
            .checked_add(body_len)
            .ok_or_else(|| StoreError::Integrity("read offset overflow".into()))?;
        if next_offset != expected_next {
            return Err(StoreError::Integrity(format!(
                "read offset mismatch: requested {offset}, received {} bytes, next offset {next_offset}",
                body.len()
            )));
        }
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
        let bucket = self
            .send_safe("create bucket", || self.client.put(&bucket_url))
            .await?;
        if !bucket.status().is_success() {
            if is_unknown_status(bucket.status()) {
                return Err(StoreError::Ambiguous(format!(
                    "create bucket returned HTTP {}",
                    bucket.status()
                )));
            }
            return Err(classify_rejection(bucket.status(), "create bucket"));
        }
        let response = self
            .send_safe("create stream", || {
                self.client
                    .put(self.stream_url(stream))
                    .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE)
            })
            .await?;
        if matches!(response.status(), StatusCode::CREATED | StatusCode::OK) {
            Ok(())
        } else if is_unknown_status(response.status()) {
            Err(StoreError::Ambiguous(format!(
                "create stream returned HTTP {}",
                response.status()
            )))
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
            let next_len = output
                .len()
                .checked_add(body.len())
                .ok_or_else(|| StoreError::Integrity("stream length overflow".into()))?;
            if next_len > self.config.max_stream_bytes {
                return Err(StoreError::Integrity(format!(
                    "stream exceeds configured limit of {} bytes",
                    self.config.max_stream_bytes
                )));
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
        } else if status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT {
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
        status if status.is_redirection() => RejectionKind::Unsupported,
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

fn is_retryable_status(status: StatusCode) -> bool {
    is_unknown_status(status) || status == StatusCode::TOO_MANY_REQUESTS
}

fn is_unknown_status(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::REQUEST_TIMEOUT
}

fn retry_delay(base: Duration, maximum: Duration, attempt: usize) -> Duration {
    if base.is_zero() || maximum.is_zero() {
        return Duration::ZERO;
    }
    let multiplier = 1_u32
        .checked_shl(attempt.min(31) as u32)
        .unwrap_or(u32::MAX);
    let cap = base.checked_mul(multiplier).unwrap_or(maximum).min(maximum);
    let cap_nanos = u64::try_from(cap.as_nanos()).unwrap_or(u64::MAX);
    let floor = cap_nanos / 2;
    let span = cap_nanos.saturating_sub(floor);
    let random = u64::from_be_bytes(
        uuid::Uuid::new_v4().as_bytes()[..8]
            .try_into()
            .unwrap_or([0; 8]),
    );
    Duration::from_nanos(floor.saturating_add(random % span.saturating_add(1)))
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use axum::Router;
    use axum::body::Body;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::HeaderValue;
    use axum::response::Response;
    use axum::routing::any;

    use super::*;

    async fn serve(router: Router, configure: impl FnOnce(&mut HttpUrsulaConfig)) -> HttpUrsula {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test Ursula");
        let address = listener.local_addr().expect("test Ursula address");
        tokio::spawn(axum::serve(listener, router).into_future());
        let mut config = HttpUrsulaConfig {
            base_url: format!("http://{address}"),
            bucket: "test".into(),
            safe_retries: 0,
            retry_base_delay: Duration::ZERO,
            retry_max_delay: Duration::ZERO,
            ..HttpUrsulaConfig::default()
        };
        configure(&mut config);
        HttpUrsula::new(config).expect("create test Ursula client")
    }

    fn read_response(status: StatusCode, next_offset: u64, body: Body) -> Response {
        Response::builder()
            .status(status)
            .header(HEADER_NEXT_OFFSET, next_offset)
            .header(HEADER_UP_TO_DATE, HeaderValue::from_static("true"))
            .body(body)
            .expect("build test response")
    }

    #[tokio::test]
    async fn rejects_offset_body_mismatch() {
        let router = Router::new().fallback(any(|| async {
            read_response(StatusCode::OK, 9, Body::from("abc"))
        }));
        let store = serve(router, |_| {}).await;

        assert!(matches!(
            store.read_all("stream").await,
            Err(StoreError::Integrity(message)) if message.contains("offset mismatch")
        ));
    }

    #[tokio::test]
    async fn rejects_oversized_response_body_and_total_stream() {
        let router = Router::new().fallback(any(|| async {
            read_response(StatusCode::OK, 3, Body::from("abc"))
        }));
        let store = serve(router, |config| config.read_chunk_bytes = 2).await;
        assert!(matches!(
            store.read_all("stream").await,
            Err(StoreError::Integrity(message)) if message.contains("response body exceeds")
        ));

        let router = Router::new().fallback(any(|| async {
            read_response(StatusCode::OK, 3, Body::from("abc"))
        }));
        let store = serve(router, |config| {
            config.read_chunk_bytes = 4;
            config.max_stream_bytes = 2;
        })
        .await;
        assert!(matches!(
            store.read_all("stream").await,
            Err(StoreError::Integrity(message)) if message.contains("stream exceeds")
        ));
    }

    #[tokio::test]
    async fn response_body_is_covered_by_timeout() {
        let router = Router::new().fallback(any(|| async {
            let stream = futures_util::stream::once(async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok::<_, Infallible>(Bytes::from_static(b"a"))
            });
            read_response(StatusCode::OK, 1, Body::from_stream(stream))
        }));
        let store = serve(router, |config| {
            config.response_timeout = Duration::from_millis(5);
        })
        .await;

        assert!(matches!(
            store.read_all("stream").await,
            Err(StoreError::Ambiguous(message)) if message.contains("body timeout")
        ));
    }

    async fn retry_handler(State(attempts): State<Arc<AtomicUsize>>) -> Response {
        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::empty())
                .expect("build retry response");
        }
        read_response(StatusCode::OK, 2, Body::from("ok"))
    }

    #[tokio::test]
    async fn retries_only_safe_reads() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .fallback(any(retry_handler))
            .with_state(attempts.clone());
        let store = serve(router, |config| config.safe_retries = 1).await;

        assert_eq!(store.read_all("stream").await.expect("retry read"), b"ok");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn classifies_redirects_and_append_rate_limits_as_definite_rejections() {
        let router = Router::new().fallback(any(|| async {
            Response::builder()
                .status(StatusCode::TEMPORARY_REDIRECT)
                .body(Body::empty())
                .expect("build redirect")
        }));
        let store = serve(router, |_| {}).await;
        assert!(matches!(
            store.read_all("stream").await,
            Err(StoreError::Rejected {
                kind: RejectionKind::Unsupported,
                ..
            })
        ));

        let router = Router::new().fallback(any(|| async {
            Response::builder()
                .status(StatusCode::TOO_MANY_REQUESTS)
                .body(Body::empty())
                .expect("build rate limit")
        }));
        let store = serve(router, |config| config.safe_retries = 5).await;
        let producer = ProducerTuple {
            id: "producer".into(),
            epoch: 0,
            sequence: 0,
        };
        assert!(matches!(
            store.append("stream", &producer, b"frame").await,
            Err(StoreError::Rejected {
                kind: RejectionKind::RateLimited,
                ..
            })
        ));
    }
}
