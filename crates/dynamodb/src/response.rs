use anyhow::{Result, anyhow, ensure};
use aws_smithy_runtime_api::client::{
    interceptors::context::InterceptorContext,
    retries::classifiers::{ClassifyRetry, RetryAction},
};
use aws_smithy_runtime_api::{
    box_error::BoxError,
    client::{
        interceptors::{
            Intercept,
            context::{
                AfterDeserializationInterceptorContextRef,
                BeforeDeserializationInterceptorContextMut, BeforeTransmitInterceptorContextRef,
            },
        },
        runtime_components::RuntimeComponents,
    },
};
use aws_smithy_types::{body::SdkBody, config_bag::ConfigBag};
use http_body_util::BodyExt;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

impl ClassifyRetry for Capture {
    fn name(&self) -> &'static str {
        "DynamoDBResponseLimit"
    }
    fn classify_retry(&self, _: &InterceptorContext) -> RetryAction {
        if self.limit_exceeded.load(Ordering::Relaxed) {
            return RetryAction::RetryForbidden;
        }
        RetryAction::NoActionIndicated
    }
}

// DynamoDB's item-byte limit excludes JSON escaping and base64 expansion.
pub(crate) const RESPONSE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Default)]
pub(crate) struct Capture {
    response: Arc<Mutex<Option<HttpResponse>>>,
    limit_exceeded: Arc<AtomicBool>,
}

struct HttpResponse {
    status: u16,
    bytes: Vec<u8>,
}

impl std::fmt::Debug for Capture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DynamoDBResponse")
    }
}

impl Intercept for Capture {
    fn name(&self) -> &'static str {
        "DynamoDBResponse"
    }

    fn read_before_attempt(
        &self,
        _: &BeforeTransmitInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        // A retry can fail before receiving a response. Do not report the previous attempt.
        *self
            .response
            .lock()
            .map_err(|_| "DynamoDB response lock poisoned")? = None;
        Ok(())
    }

    fn modify_before_deserialization(
        &self,
        context: &mut BeforeDeserializationInterceptorContextMut<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let body = context.response_mut().body_mut();
        let original = std::mem::replace(body, SdkBody::empty());
        let limit_exceeded = self.limit_exceeded.clone();
        *body = SdkBody::from_body_1_x(
            http_body_util::Limited::new(original, RESPONSE_BYTES).map_err(move |error| {
                if error.is::<http_body_util::LengthLimitError>() {
                    limit_exceeded.store(true, Ordering::Relaxed);
                }
                error
            }),
        );
        Ok(())
    }

    fn read_after_deserialization(
        &self,
        context: &AfterDeserializationInterceptorContextRef<'_>,
        _: &RuntimeComponents,
        _: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        if let Some(bytes) = response.body().bytes() {
            *self
                .response
                .lock()
                .map_err(|_| "DynamoDB response lock poisoned")? = Some(HttpResponse {
                status: response.status().as_u16(),
                bytes: bytes.to_vec(),
            });
        }
        Ok(())
    }
}

impl Capture {
    pub fn finish(&self, result: Result<()>) -> Result<serde_json::Value> {
        self.finish_json(result, false)
    }

    pub(crate) fn finish_json(
        &self,
        result: Result<()>,
        raw_success: bool,
    ) -> Result<serde_json::Value> {
        ensure!(
            !self.limit_exceeded.load(Ordering::Relaxed),
            "DynamoDB response exceeds 8 MiB"
        );
        let captured = self
            .response
            .lock()
            .map_err(|_| anyhow!("DynamoDB response lock poisoned"))?
            .take();
        if let Some(HttpResponse { status, bytes }) = captured {
            if !(200..300).contains(&status) {
                return Err(anyhow!(
                    "DynamoDB HTTP {status}: {}",
                    match std::str::from_utf8(&bytes) {
                        Ok(text) => text.to_owned(),
                        Err(_) => format!("non-UTF-8 response bytes: {bytes:02x?}"),
                    }
                ));
            }
            if !raw_success {
                result?;
            }
            ensure!(
                bytes.len() <= RESPONSE_BYTES,
                "DynamoDB response exceeds 8 MiB"
            );
            return Ok(serde_json::from_slice(&bytes)?);
        }
        result?;
        Err(anyhow!("DynamoDB response body is unavailable"))
    }
}
