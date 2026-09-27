use anyhow::{Result, anyhow, ensure};
use onetui_core::{Column, PAGE_BYTES, Page, Row, Value};
use reqwest::{Method, header};
use url::Url;

pub struct Request<'a> {
    pub method: Method,
    pub path: &'a str,
    pub body: &'a str,
}

pub fn parse(text: &str) -> Result<Request<'_>> {
    let (line, rest) = text.split_once('\n').unwrap_or((text, ""));
    let mut parts = line.split_whitespace();
    let method = parts.next().ok_or_else(|| anyhow!("Enter METHOD /path"))?;
    let path = parts.next().ok_or_else(|| anyhow!("Enter METHOD /path"))?;
    ensure!(
        parts.next().is_none(),
        "Enter METHOD /path on the first line"
    );
    let method =
        Method::from_bytes(method.as_bytes()).map_err(|_| anyhow!("Invalid HTTP method"))?;
    ensure!(
        path.starts_with('/') && !path.starts_with("//"),
        "Use a path starting with /"
    );
    let body = if rest.is_empty() {
        ""
    } else {
        rest.strip_prefix('\n')
            .ok_or_else(|| anyhow!("Leave a blank line before the body"))?
    };
    Ok(Request { method, path, body })
}

pub fn target(base: &Url, request: &Request<'_>) -> Result<Url> {
    let target = base.join(request.path)?;
    ensure!(
        target.origin() == base.origin()
            && target.fragment().is_none()
            && target.username().is_empty()
            && target.password().is_none(),
        "Request path must stay on the configured endpoint"
    );
    Ok(target)
}

pub async fn send(mut outbound: reqwest::RequestBuilder, request: &Request<'_>) -> Result<Page> {
    if !request.body.is_empty() {
        outbound = outbound
            .header(header::CONTENT_TYPE, "application/json")
            .body(request.body.to_owned());
    }
    let mut response = outbound
        .send()
        .await
        .map_err(|error| anyhow!(error.without_url()))?;
    let status = response.status();
    let limit = PAGE_BYTES - 1024;
    ensure!(
        response
            .content_length()
            .is_none_or(|size| size <= limit as u64),
        "HTTP {status}: response exceeds the display limit"
    );
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| anyhow!(error.without_url()))?
    {
        ensure!(
            chunk.len() <= limit - body.len(),
            "HTTP {status}: response exceeds the display limit"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(Page {
        rows: vec![Row {
            cells: vec![Some(status.to_string().into()), Some(Value::Bytes(body))],
            target: None,
        }],
        columns: vec![
            Column {
                name: "status".into(),
                datatype: "text".into(),
            },
            Column {
                name: "body".into(),
                datatype: "bytes".into(),
            },
        ],
        ..Page::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_keeps_native_body_and_stays_on_configured_origin() {
        let request =
            parse("POST /collections/demo/points/scroll?wait=true\n\n{invalid json}").unwrap();
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.body, "{invalid json}");
        assert_eq!(
            target(
                &Url::parse("https://qdrant.example:6333").unwrap(),
                &request
            )
            .unwrap()
            .as_str(),
            "https://qdrant.example:6333/collections/demo/points/scroll?wait=true"
        );
        for text in [
            "GET https://other.example/collections",
            "GET //other.example/collections",
            "GET /\\other.example/collections",
            "GET /\\user:password@qdrant.example:6333/collections",
            "GET /collections#fragment",
            "POST /collections\n{\"name\":\"demo\"}",
        ] {
            assert!(
                parse(text)
                    .and_then(|request| target(
                        &Url::parse("https://qdrant.example:6333").unwrap(),
                        &request
                    ))
                    .is_err(),
                "{text}"
            );
        }
    }
}
