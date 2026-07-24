use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use reqwest::StatusCode;
use thiserror::Error;

use crate::frame::ProducerTuple;

const CONTENT_TYPE: &str = "application/vnd.quorum-loro.delta-frame.v1";
const HEADER_NEXT_OFFSET: &str = "stream-next-offset";
const HEADER_UP_TO_DATE: &str = "stream-up-to-date";
const HEADER_RAFT_LEADER_ID: &str = "x-ursula-raft-leader-id";

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
    pub redirect_base_urls: Vec<String>,
    pub max_redirects: usize,
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
            redirect_base_urls: Vec::new(),
            max_redirects: 4,
            bucket: "qloro".into(),
            response_timeout: Duration::from_secs(30),
            read_chunk_bytes: 64 * 1024,
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
    base_url: reqwest::Url,
    allowed_origins: HashSet<String>,
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
        let base_url = parse_node_base(&config.base_url)?;
        let mut allowed_origins = HashSet::new();
        allowed_origins.insert(url_origin(&base_url));
        for redirect_base_url in &config.redirect_base_urls {
            allowed_origins.insert(url_origin(&parse_node_base(redirect_base_url)?));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| StoreError::Ambiguous(error.to_string()))?;
        Ok(Self {
            config,
            client,
            base_url,
            allowed_origins,
        })
    }

    fn bucket_url(&self) -> Result<reqwest::Url, StoreError> {
        self.object_url(None)
    }

    fn stream_url(&self, stream: &str) -> Result<reqwest::Url, StoreError> {
        self.object_url(Some(stream))
    }

    fn object_url(&self, stream: Option<&str>) -> Result<reqwest::Url, StoreError> {
        let mut url = self.base_url.clone();
        let mut segments = url.path_segments_mut().map_err(|_| StoreError::Rejected {
            kind: RejectionKind::Invalid,
            message: "Ursula base URL cannot contain path segments".into(),
        })?;
        segments.clear().push(&self.config.bucket);
        if let Some(stream) = stream {
            segments.push(stream);
        }
        drop(segments);
        Ok(url)
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
        initial_url: reqwest::Url,
        request: F,
    ) -> Result<reqwest::Response, StoreError>
    where
        F: Fn(&reqwest::Url) -> reqwest::RequestBuilder,
    {
        for attempt in 0..=self.config.safe_retries {
            match self
                .send_following_redirects(operation, initial_url.clone(), &request)
                .await
            {
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

    async fn send_following_redirects<F>(
        &self,
        operation: &str,
        initial_url: reqwest::Url,
        request: &F,
    ) -> Result<reqwest::Response, StoreError>
    where
        F: Fn(&reqwest::Url) -> reqwest::RequestBuilder,
    {
        let expected_path = initial_url.path().to_owned();
        let expected_query = initial_url.query().map(str::to_owned);
        let mut current_url = initial_url;
        let mut visited = HashSet::new();
        visited.insert(current_url.as_str().to_owned());

        for redirects in 0..=self.config.max_redirects {
            let response = self.send(request(&current_url)).await?;
            if response.status() != StatusCode::TEMPORARY_REDIRECT {
                return Ok(response);
            }
            let Some(leader_id) = response.headers().get(HEADER_RAFT_LEADER_ID) else {
                return Ok(response);
            };
            let leader_id = leader_id
                .to_str()
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| {
                    StoreError::Ambiguous(format!(
                        "{operation} received malformed Ursula leader ID"
                    ))
                })?;
            if redirects >= self.config.max_redirects {
                return Err(StoreError::Ambiguous(format!(
                    "{operation} exceeded {} Ursula leader redirects",
                    self.config.max_redirects
                )));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    StoreError::Ambiguous(format!(
                        "{operation} received Ursula leader redirect without valid Location"
                    ))
                })?;
            let target = reqwest::Url::parse(location).map_err(|error| {
                StoreError::Ambiguous(format!(
                    "{operation} received malformed Ursula redirect target: {error}"
                ))
            })?;
            if !self.allowed_origins.contains(&url_origin(&target)) {
                return Err(StoreError::Ambiguous(format!(
                    "{operation} received Ursula redirect to an unconfigured origin"
                )));
            }
            if target.username() != "" || target.password().is_some() {
                return Err(StoreError::Ambiguous(format!(
                    "{operation} received Ursula redirect containing credentials"
                )));
            }
            if target.path() != expected_path || target.query() != expected_query.as_deref() {
                return Err(StoreError::Ambiguous(format!(
                    "{operation} received Ursula redirect that changed the request target"
                )));
            }
            if !visited.insert(target.as_str().to_owned()) {
                return Err(StoreError::Ambiguous(format!(
                    "{operation} detected an Ursula redirect loop"
                )));
            }
            tracing::info!(
                operation,
                leader_id,
                redirect = redirects + 1,
                target_origin = %url_origin(&target),
                "following Ursula leader redirect"
            );
            current_url = target;
        }

        Err(StoreError::Ambiguous(format!(
            "{operation} exhausted Ursula redirect handling"
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
        let mut url = self.stream_url(stream)?;
        url.query_pairs_mut()
            .append_pair("offset", &offset.to_string())
            .append_pair("max_bytes", &max_bytes.to_string());
        let response = self
            .send_following_redirects("read stream", url, &|target| {
                self.client.get(target.clone())
            })
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
        let bucket_url = self.bucket_url()?;
        let bucket = self
            .send_safe("create bucket", bucket_url, |target| {
                self.client.put(target.clone())
            })
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
        let stream_url = self.stream_url(stream)?;
        let response = self
            .send_safe("create stream", stream_url, |target| {
                self.client
                    .put(target.clone())
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
        let stream_url = self.stream_url(stream)?;
        let response = self
            .send_following_redirects("append", stream_url, &|target| {
                self.client
                    .post(target.clone())
                    .header(reqwest::header::CONTENT_TYPE, CONTENT_TYPE)
                    .header("producer-id", &producer.id)
                    .header("producer-epoch", producer.epoch)
                    .header("producer-seq", producer.sequence)
                    .body(body.to_vec())
            })
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

fn parse_node_base(value: &str) -> Result<reqwest::Url, StoreError> {
    let url = reqwest::Url::parse(value).map_err(|error| StoreError::Rejected {
        kind: RejectionKind::Invalid,
        message: format!("invalid Ursula node URL: {error}"),
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(StoreError::Rejected {
            kind: RejectionKind::Invalid,
            message: "Ursula node URL must be an HTTP(S) origin without credentials, path, query, or fragment".into(),
        });
    }
    Ok(url)
}

fn url_origin(url: &reqwest::Url) -> String {
    url.origin().ascii_serialization()
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
    use axum::http::HeaderMap;
    use axum::http::HeaderValue;
    use axum::response::Response;
    use axum::routing::any;

    use super::*;

    #[derive(Debug, Default)]
    struct AppendObservation {
        requests: usize,
        producer_id: String,
        producer_epoch: String,
        producer_sequence: String,
        content_type: String,
        body: Vec<u8>,
    }

    #[derive(Clone)]
    struct RedirectTarget {
        location: String,
        leader_id: &'static str,
    }

    async fn bind_test_server() -> (tokio::net::TcpListener, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind redirect test server");
        let address = listener.local_addr().expect("redirect test address");
        (listener, format!("http://{address}"))
    }

    fn spawn_test_server(listener: tokio::net::TcpListener, router: Router) {
        tokio::spawn(axum::serve(listener, router).into_future());
    }

    async fn observe_append(
        State(observation): State<Arc<std::sync::Mutex<AppendObservation>>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let mut observation = observation.lock().expect("append observation lock");
        observation.requests += 1;
        observation.producer_id = test_header(&headers, "producer-id");
        observation.producer_epoch = test_header(&headers, "producer-epoch");
        observation.producer_sequence = test_header(&headers, "producer-seq");
        observation.content_type = test_header(&headers, "content-type");
        observation.body = body.to_vec();
        Response::builder()
            .status(StatusCode::OK)
            .header(HEADER_NEXT_OFFSET, body.len().to_string())
            .body(Body::empty())
            .expect("build append response")
    }

    async fn redirect(State(target): State<RedirectTarget>) -> Response {
        Response::builder()
            .status(StatusCode::TEMPORARY_REDIRECT)
            .header(reqwest::header::LOCATION, target.location)
            .header(HEADER_RAFT_LEADER_ID, target.leader_id)
            .body(Body::empty())
            .expect("build leader redirect")
    }

    fn test_header(headers: &HeaderMap, name: &str) -> String {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_owned()
    }

    fn redirect_store(base_url: String, peers: Vec<String>) -> HttpUrsula {
        HttpUrsula::new(HttpUrsulaConfig {
            base_url,
            redirect_base_urls: peers,
            bucket: "test".into(),
            response_timeout: Duration::from_millis(100),
            safe_retries: 0,
            retry_base_delay: Duration::ZERO,
            retry_max_delay: Duration::ZERO,
            ..HttpUrsulaConfig::default()
        })
        .expect("create redirect test client")
    }

    fn test_producer() -> ProducerTuple {
        ProducerTuple {
            id: "producer-a".into(),
            epoch: 7,
            sequence: 11,
        }
    }

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

    #[tokio::test]
    async fn append_sent_to_leader_commits_without_redirect() {
        let (leader_listener, leader_url) = bind_test_server().await;
        let observation = Arc::new(std::sync::Mutex::new(AppendObservation::default()));
        spawn_test_server(
            leader_listener,
            Router::new()
                .fallback(any(observe_append))
                .with_state(observation.clone()),
        );
        let store = redirect_store(leader_url, Vec::new());

        assert_eq!(
            store
                .append("stream", &test_producer(), b"exact-frame")
                .await
                .expect("leader append"),
            AppendOutcome::Committed { next_offset: 11 }
        );
        assert_eq!(observation.lock().expect("observation lock").requests, 1);
    }

    #[tokio::test]
    async fn follower_redirect_preserves_exact_append_request() {
        let (leader_listener, leader_url) = bind_test_server().await;
        let observation = Arc::new(std::sync::Mutex::new(AppendObservation::default()));
        spawn_test_server(
            leader_listener,
            Router::new()
                .fallback(any(observe_append))
                .with_state(observation.clone()),
        );
        let (follower_listener, follower_url) = bind_test_server().await;
        spawn_test_server(
            follower_listener,
            Router::new()
                .fallback(any(redirect))
                .with_state(RedirectTarget {
                    location: format!("{leader_url}/test/stream"),
                    leader_id: "2",
                }),
        );
        let store = redirect_store(follower_url, vec![leader_url]);

        assert_eq!(
            store
                .append("stream", &test_producer(), b"exact-frame")
                .await
                .expect("redirected append"),
            AppendOutcome::Committed { next_offset: 11 }
        );
        let observation = observation.lock().expect("observation lock");
        assert_eq!(observation.requests, 1);
        assert_eq!(observation.producer_id, "producer-a");
        assert_eq!(observation.producer_epoch, "7");
        assert_eq!(observation.producer_sequence, "11");
        assert_eq!(observation.content_type, CONTENT_TYPE);
        assert_eq!(observation.body, b"exact-frame");
    }

    #[tokio::test]
    async fn follows_leader_changes_across_redirect_hops() {
        let (leader_listener, leader_url) = bind_test_server().await;
        spawn_test_server(
            leader_listener,
            Router::new().fallback(any(|| async {
                Response::builder()
                    .status(StatusCode::OK)
                    .header(HEADER_NEXT_OFFSET, "5")
                    .body(Body::empty())
                    .expect("final leader response")
            })),
        );
        let (former_leader_listener, former_leader_url) = bind_test_server().await;
        spawn_test_server(
            former_leader_listener,
            Router::new()
                .fallback(any(redirect))
                .with_state(RedirectTarget {
                    location: format!("{leader_url}/test/stream"),
                    leader_id: "3",
                }),
        );
        let (follower_listener, follower_url) = bind_test_server().await;
        spawn_test_server(
            follower_listener,
            Router::new()
                .fallback(any(redirect))
                .with_state(RedirectTarget {
                    location: format!("{former_leader_url}/test/stream"),
                    leader_id: "2",
                }),
        );
        let limited = HttpUrsula::new(HttpUrsulaConfig {
            base_url: follower_url.clone(),
            redirect_base_urls: vec![former_leader_url.clone(), leader_url.clone()],
            max_redirects: 1,
            bucket: "test".into(),
            safe_retries: 0,
            ..HttpUrsulaConfig::default()
        })
        .expect("create redirect-limited client");
        assert!(matches!(
            limited.append("stream", &test_producer(), b"frame").await,
            Err(StoreError::Ambiguous(message)) if message.contains("exceeded 1")
        ));
        let store = redirect_store(
            follower_url,
            vec![former_leader_url.clone(), leader_url.clone()],
        );

        assert_eq!(
            store
                .append("stream", &test_producer(), b"frame")
                .await
                .expect("append after leader changes"),
            AppendOutcome::Committed { next_offset: 5 }
        );
    }

    #[tokio::test]
    async fn rejects_redirect_loop() {
        let (first_listener, first_url) = bind_test_server().await;
        let (second_listener, second_url) = bind_test_server().await;
        spawn_test_server(
            first_listener,
            Router::new()
                .fallback(any(redirect))
                .with_state(RedirectTarget {
                    location: format!("{second_url}/test/stream"),
                    leader_id: "2",
                }),
        );
        spawn_test_server(
            second_listener,
            Router::new()
                .fallback(any(redirect))
                .with_state(RedirectTarget {
                    location: format!("{first_url}/test/stream"),
                    leader_id: "1",
                }),
        );
        let store = redirect_store(first_url, vec![second_url]);

        assert!(matches!(
            store.append("stream", &test_producer(), b"frame").await,
            Err(StoreError::Ambiguous(message)) if message.contains("redirect loop")
        ));
    }

    #[tokio::test]
    async fn rejects_malformed_or_unconfigured_redirect() {
        let (malformed_listener, malformed_url) = bind_test_server().await;
        spawn_test_server(
            malformed_listener,
            Router::new().fallback(any(|| async {
                Response::builder()
                    .status(StatusCode::TEMPORARY_REDIRECT)
                    .header(HEADER_RAFT_LEADER_ID, "2")
                    .header(reqwest::header::LOCATION, "/test/stream")
                    .body(Body::empty())
                    .expect("malformed redirect response")
            })),
        );
        let store = redirect_store(malformed_url, Vec::new());
        assert!(matches!(
            store.append("stream", &test_producer(), b"frame").await,
            Err(StoreError::Ambiguous(message)) if message.contains("malformed")
        ));

        let (foreign_listener, foreign_url) = bind_test_server().await;
        spawn_test_server(
            foreign_listener,
            Router::new()
                .fallback(any(redirect))
                .with_state(RedirectTarget {
                    location: "http://127.0.0.1:9/test/stream".into(),
                    leader_id: "9",
                }),
        );
        let store = redirect_store(foreign_url, Vec::new());
        assert!(matches!(
            store.append("stream", &test_producer(), b"frame").await,
            Err(StoreError::Ambiguous(message)) if message.contains("unconfigured origin")
        ));
    }

    #[tokio::test]
    async fn unavailable_redirect_target_is_ambiguous() {
        let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("reserve unavailable redirect target");
        let unavailable_url = format!(
            "http://{}",
            unavailable.local_addr().expect("unavailable address")
        );
        drop(unavailable);
        let (follower_listener, follower_url) = bind_test_server().await;
        spawn_test_server(
            follower_listener,
            Router::new()
                .fallback(any(redirect))
                .with_state(RedirectTarget {
                    location: format!("{unavailable_url}/test/stream"),
                    leader_id: "2",
                }),
        );
        let store = redirect_store(follower_url, vec![unavailable_url]);

        assert!(matches!(
            store.append("stream", &test_producer(), b"frame").await,
            Err(StoreError::Ambiguous(message)) if message.contains("error sending request")
        ));
    }
}
