use onetui_core::{
    Page, Resource, Value,
    provider::{
        ConnectionStatus, Executor, PageRequest, Provider, QueryRequest, RequestContext,
        ShutdownContext,
    },
};
use onetui_dynamodb::{DynamoDbExecutor, DynamoDbProvider};
use serde_json::{Value as Json, json};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
    thread,
    time::Duration,
};

#[path = "protocol/streams.rs"]
mod streams;

#[path = "protocol/multi_items.rs"]
mod multi_items;

#[path = "protocol/vectors.rs"]
mod vectors;

#[path = "protocol/partiql.rs"]
mod partiql;

struct Server {
    endpoint: String,
    requests: mpsc::Receiver<(String, Json)>,
    worker: thread::JoinHandle<()>,
}

impl Server {
    fn start(replies: Vec<(u16, String)>) -> Self {
        Self::delayed(replies, Duration::ZERO)
    }

    fn delayed(replies: Vec<(u16, String)>, delay: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let (sender, requests) = mpsc::channel();
        let worker = thread::spawn(move || {
            for (status, body) in replies {
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(std::time::Instant::now() < deadline, "request not received");
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(e) => panic!("{e}"),
                    }
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                let end = loop {
                    let mut byte = [0];
                    stream.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                    assert!(bytes.len() < 65536);
                    if bytes.ends_with(b"\r\n\r\n") {
                        break bytes.len();
                    }
                };
                let headers = String::from_utf8(bytes).unwrap();
                let length: usize = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(end + length < 131072);
                let mut request = vec![0; length];
                stream.read_exact(&mut request).unwrap();
                sender
                    .send((headers, serde_json::from_slice(&request).unwrap()))
                    .unwrap();
                thread::sleep(delay);
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                // A cancellation or response-size limit closes the connection early.
                let _ = stream.write_all(response.as_bytes());
            }
        });
        Self {
            endpoint,
            requests,
            worker,
        }
    }

    fn executor(&self) -> DynamoDbExecutor {
        DynamoDbProvider.configure(&toml::from_str(&format!(
            "region='us-east-1'\nendpoint_url='{0}'\nstreams_endpoint_url='{0}'\naccess_key_id_env='TEST_KEY'\nsecret_access_key_env='TEST_SECRET'\nsession_token_env='TEST_TOKEN'", self.endpoint
        )).unwrap(), &|name| Some(match name {
            "TEST_KEY" => "fixture-access-key", "TEST_SECRET" => "fixture-secret", "TEST_TOKEN" => "fixture-session-token", _ => panic!("unexpected secret"),
        }.into())).unwrap()
    }

    fn finish(self) -> Vec<(String, Json)> {
        self.worker.join().unwrap();
        self.requests.into_iter().collect()
    }
}

fn request(id: &'static str, path: &[&str], continuation: Option<String>) -> PageRequest {
    PageRequest {
        resource: Resource::new(id, path.iter().map(|s| (*s).into()).collect()),
        continuation,
    }
}

async fn fetch(executor: &DynamoDbExecutor, request: PageRequest) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor.fetch_page(request, context).await
}

async fn query(
    executor: &DynamoDbExecutor,
    text: &str,
    continuation: Option<String>,
) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor
        .query_page(
            QueryRequest {
                page: request("dynamodb.query", &["demo"], continuation),
                text: text.into(),
            },
            context,
        )
        .await
}

fn cell(page: &Page, row: usize, name: &str) -> Option<Json> {
    let column = page.columns.iter().position(|c| c.name == name).unwrap();
    page.rows[row].cells[column]
        .as_ref()
        .map(|value| match value {
            Value::Json(text) => serde_json::from_str(text).unwrap(),
            _ => panic!("expected tagged JSON"),
        })
}

#[test]
fn credential_process_is_rejected_without_executing_a_shell() {
    const CHILD: &str = "ONETUI_DYNAMODB_PROCESS_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let executor = DynamoDbProvider
            .configure(
                &toml::from_str(
                    "region='us-east-1'\nprofile='blocked'\nendpoint_url='https://127.0.0.1:9'",
                )
                .unwrap(),
                &|_| panic!("no explicit credential references"),
            )
            .unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
            let error = executor.check(context).await.err().unwrap().to_string();
            assert!(error.contains("credentials-process"), "{error}");
        });
        return;
    }
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("config");
    let credentials = temp.path().join("credentials");
    std::fs::write(&config, "[profile blocked]\ncredential_process = exit 73\n").unwrap();
    std::fs::write(&credentials, "").unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "credential_process_is_rejected_without_executing_a_shell",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .env("AWS_CONFIG_FILE", config)
        .env("AWS_SHARED_CREDENTIALS_FILE", credentials)
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("AWS_WEB_IDENTITY_TOKEN_FILE")
        .env_remove("AWS_ROLE_ARN")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn signed_native_requests_preserve_metadata_and_lazy_menus() {
    let body = json!({"Table":{"TableName":"demo", "FutureMetadata":{"precise":"12345678901234567890"}, "Replicas":[{"RegionName":"eu-west-1","ReplicaStatus":"ACTIVE"}]}});
    let server = Server::start(vec![(200, body.to_string()), (200, body.to_string())]);
    let mut executor = server.executor();
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Configured);
    assert!(
        !fetch(&executor, request("dynamodb.resources", &[], None))
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert!(server.requests.try_recv().is_err());
    let page = fetch(&executor, request("dynamodb.table_info", &["demo"], None))
        .await
        .unwrap();
    assert_eq!(cell(&page, 0, "response"), Some(body.clone()));
    let page = fetch(&executor, request("dynamodb.replicas", &["demo"], None))
        .await
        .unwrap();
    assert_eq!(
        cell(&page, 0, "response"),
        Some(body["Table"]["Replicas"].clone())
    );
    for (headers, body) in server.finish() {
        let headers = headers.to_ascii_lowercase();
        assert!(headers.contains("x-amz-target: dynamodb_20120810.describetable"));
        assert!(headers.contains("authorization: aws4-hmac-sha256 credential=fixture-access-key/"));
        assert!(headers.contains("/us-east-1/dynamodb/aws4_request"));
        assert!(headers.contains("x-amz-security-token: fixture-session-token"));
        assert_eq!(body, json!({"TableName":"demo"}));
    }
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Closed);
    assert!(
        fetch(&executor, request("dynamodb.tables", &[], None))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn filtered_empty_pages_keep_exact_keys_and_replayable_bookmarks() {
    let key = json!({"pk":{"S":"東京"}, "sk":{"N":"12345678901234567890123456789012345678"}});
    let item = json!({"pk":{"S":"東京"},"bytes":{"B":"AP+A"},"null":{"NULL":true},"nested":{"L":[{"BOOL":true},{"NS":["0.1","99"]}]}});
    let result = json!({"Items":[item, {"pk":{"S":"other"}}],"Count":2,"ScannedCount":2,"LastEvaluatedKey":{}});
    let server = Server::start(vec![
        (
            200,
            json!({"Items":[],"Count":0,"ScannedCount":100,"LastEvaluatedKey":key}).to_string(),
        ),
        (200, result.to_string()),
        (200, result.to_string()),
    ]);
    let executor = server.executor();
    let first = fetch(&executor, request("dynamodb.items", &["demo"], None))
        .await
        .unwrap();
    assert!(first.rows.is_empty() && first.next);
    let bookmark = first.continuation.unwrap();
    for _ in 0..2 {
        let page = fetch(
            &executor,
            request("dynamodb.items", &["demo"], Some(bookmark.clone())),
        )
        .await
        .unwrap();
        assert!(!page.next && page.continuation.is_none());
        assert_eq!(cell(&page, 0, "bytes"), Some(item["bytes"].clone()));
        assert_eq!(cell(&page, 0, "nested"), Some(item["nested"].clone()));
        assert_eq!(cell(&page, 0, "null"), Some(json!({"NULL":true})));
        assert_eq!(cell(&page, 1, "null"), None);
    }
    assert!(
        fetch(
            &executor,
            request("dynamodb.items", &["other"], Some(bookmark.clone()))
        )
        .await
        .is_err()
    );
    assert!(
        fetch(
            &server.executor(),
            request("dynamodb.items", &["demo"], Some(bookmark.clone()))
        )
        .await
        .is_err()
    );
    assert!(
        query(&executor, r#"{"operation":"Scan"}"#, Some(bookmark))
            .await
            .is_err()
    );
    let requests = server.finish();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].1["Limit"], 100);
    assert!(!requests[0].1["ConsistentRead"].as_bool().unwrap());
    assert_eq!(requests[1].1["ExclusiveStartKey"], key);
    assert_eq!(requests[1].1, requests[2].1);
}

#[tokio::test]
async fn query_get_item_and_write_rejection_use_native_types() {
    let server = Server::start(vec![
        (200, json!({"Items":[],"Count":0}).to_string()),
        (200, json!({"Item":{"pk":{"B":"AP8="}}}).to_string()),
    ]);
    let executor = server.executor();
    query(&executor,r##"{"operation":"Query","index":"by_status","key_condition_expression":"#p=:p","expression_attribute_names":{"#p":"status"},"expression_attribute_values":{":p":{"S":"open"}},"scan_index_forward":false,"limit":7}"##,None).await.unwrap();
    let page = query(
        &executor,
        r#"{"operation":"GetItem","key":{"pk":{"B":"AP8="}},"consistent_read":true}"#,
        None,
    )
    .await
    .unwrap();
    assert_eq!(cell(&page, 0, "pk"), Some(json!({"B":"AP8="})));
    assert!(
        query(&executor, r#"{"operation":"PutItem","item":{}}"#, None)
            .await
            .is_err()
    );
    let requests = server.finish();
    assert_eq!(requests[0].1["IndexName"], "by_status");
    assert_eq!(requests[0].1["Limit"], 7);
    assert_eq!(requests[0].1["ScanIndexForward"], false);
    assert_eq!(requests[1].1["Key"], json!({"pk":{"B":"AP8="}}));
    assert_eq!(requests[1].1["ConsistentRead"], true);
}

#[tokio::test]
async fn cloud_metadata_routes_only_native_read_operations() {
    let cases = [
        ("dynamodb.ttl", "DescribeTimeToLive", "TableName"),
        (
            "dynamodb.backups_status",
            "DescribeContinuousBackups",
            "TableName",
        ),
        (
            "dynamodb.insights",
            "DescribeContributorInsights",
            "TableName",
        ),
        (
            "dynamodb.kinesis",
            "DescribeKinesisStreamingDestination",
            "TableName",
        ),
        (
            "dynamodb.replica_scaling",
            "DescribeTableReplicaAutoScaling",
            "TableName",
        ),
        ("dynamodb.backups", "ListBackups", ""),
        ("dynamodb.backup", "DescribeBackup", "BackupArn"),
        ("dynamodb.exports", "ListExports", ""),
        ("dynamodb.export", "DescribeExport", "ExportArn"),
        ("dynamodb.imports", "ListImports", ""),
        ("dynamodb.import", "DescribeImport", "ImportArn"),
        ("dynamodb.global_tables", "ListGlobalTables", ""),
        (
            "dynamodb.global_table",
            "DescribeGlobalTable",
            "GlobalTableName",
        ),
        (
            "dynamodb.global_settings",
            "DescribeGlobalTableSettings",
            "GlobalTableName",
        ),
        ("dynamodb.account_insights", "ListContributorInsights", ""),
        ("dynamodb.limits", "DescribeLimits", ""),
        ("dynamodb.endpoints", "DescribeEndpoints", ""),
    ];
    let server = Server::start(
        cases
            .iter()
            .map(|_| {
                (
                    200,
                    json!({"FutureMetadata":{"region":"eu-west-1"}}).to_string(),
                )
            })
            .collect(),
    );
    let executor = server.executor();
    for (id, _, key) in cases {
        let path = if key.is_empty() { vec![] } else { vec!["demo"] };
        let page = fetch(&executor, request(id, &path, None)).await.unwrap();
        if !page.rows.is_empty() {
            assert_eq!(
                cell(&page, 0, "response").unwrap()["FutureMetadata"]["region"],
                "eu-west-1"
            );
        }
    }
    for ((_, operation, key), (headers, body)) in cases.iter().zip(server.finish()) {
        assert!(headers.to_ascii_lowercase().contains(&format!(
            "x-amz-target: dynamodb_20120810.{}",
            operation.to_ascii_lowercase()
        )));
        if !key.is_empty() {
            assert_eq!(body[key], "demo");
        }
    }
}

#[tokio::test]
async fn insight_inventory_opens_table_and_index_details_and_stream_policies_are_readable() {
    let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/demo/stream/2026-09-14T00:00:00.000";
    let policy =
        json!({"Policy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}","RevisionId":"001"});
    let index = json!({"TableName":"demo","IndexName":"by_status","ContributorInsightsStatus":"FAILED","FailureException":{"ExceptionName":"AccessDeniedException","ExceptionDescription":"native failure detail"}});
    let server = Server::start(vec![
        (200,json!({"ContributorInsightsSummaries":[{"TableName":"demo"},{"TableName":"demo","IndexName":"by_status"}]}).to_string()),
        (200,index.to_string()),
        (200,policy.to_string()),
    ]);
    let e = server.executor();
    let inventory = fetch(&e, request("dynamodb.account_insights", &[], None))
        .await
        .unwrap();
    assert_eq!(
        inventory.rows[0].target,
        Some(Resource::new("dynamodb.insights", vec!["demo".into()]))
    );
    let target = inventory.rows[1].target.clone().unwrap();
    assert_eq!(
        target,
        Resource::new(
            "dynamodb.index_insights",
            vec!["demo".into(), "by_status".into()]
        )
    );
    let detail = fetch(
        &e,
        PageRequest {
            resource: target,
            continuation: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(cell(&detail, 0, "response"), Some(index));
    let menu = fetch(&e, request("dynamodb.stream", &[arn], None))
        .await
        .unwrap();
    let target = menu
        .rows
        .iter()
        .find_map(|row| {
            row.target
                .as_ref()
                .filter(|t| t.id == "dynamodb.stream_policy")
        })
        .unwrap()
        .clone();
    let detail = fetch(
        &e,
        PageRequest {
            resource: target,
            continuation: None,
        },
    )
    .await
    .unwrap();
    assert_eq!(cell(&detail, 0, "response"), Some(policy));
    let calls = server.finish();
    assert_eq!(calls.len(), 3);
    assert!(
        calls[1]
            .0
            .contains("DynamoDB_20120810.DescribeContributorInsights")
    );
    assert_eq!(calls[1].1["TableName"], "demo");
    assert_eq!(calls[1].1["IndexName"], "by_status");
    assert!(calls[2].0.contains("DynamoDB_20120810.GetResourcePolicy"));
    assert_eq!(calls[2].1["ResourceArn"], arn);
}

#[tokio::test]
async fn tag_tokens_use_the_native_table_arn_and_policies_keep_json() {
    let table = json!({"Table":{"TableArn":"arn:aws:dynamodb:us-east-1:123456789012:table/demo"}});
    let policy =
        json!({"Policy":"{\"Version\":\"2012-10-17\",\"Statement\":[]}","RevisionId":"revision-1"});
    let server = Server::start(vec![
        (200, table.to_string()),
        (
            200,
            json!({"Tags":[{"Key":"env","Value":"demo"}],"NextToken":"next-tag"}).to_string(),
        ),
        (200, table.to_string()),
        (200, json!({"Tags":[]}).to_string()),
        (200, table.to_string()),
        (200, policy.to_string()),
    ]);
    let executor = server.executor();
    let first = fetch(&executor, request("dynamodb.tags", &["demo"], None))
        .await
        .unwrap();
    assert!(first.next);
    assert!(
        fetch(
            &executor,
            request("dynamodb.tags", &["demo"], first.continuation)
        )
        .await
        .unwrap()
        .rows
        .is_empty()
    );
    let result = fetch(&executor, request("dynamodb.policy", &["demo"], None))
        .await
        .unwrap();
    assert_eq!(cell(&result, 0, "response"), Some(policy));
    let requests = server.finish();
    assert_eq!(requests[1].1["ResourceArn"], table["Table"]["TableArn"]);
    assert_eq!(requests[3].1["NextToken"], "next-tag");
    assert_eq!(requests[5].1["ResourceArn"], table["Table"]["TableArn"]);
}

#[tokio::test]
async fn native_errors_redact_configured_secrets_and_allow_recovery() {
    let server=Server::start(vec![(400,json!({"__type":"com.amazonaws.dynamodb.v20120810#AccessDeniedException","message":"denied table demo: fixture-secret fixture-session-token"}).to_string()),(200,json!({"TableNames":[]}).to_string())]);
    let executor = server.executor();
    let error = fetch(&executor, request("dynamodb.tables", &[], None))
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("HTTP 400")
            && error.contains("AccessDeniedException")
            && error.contains("denied table demo"),
        "{error}"
    );
    assert!(!error.contains("fixture-secret") && !error.contains("fixture-session-token"));
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    assert!(
        fetch(&executor, request("dynamodb.tables", &[], None))
            .await
            .unwrap()
            .rows
            .is_empty()
    );
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Connected);
    assert_eq!(server.finish().len(), 2);
}

#[tokio::test]
async fn oversized_http_response_fails_without_a_partial_page() {
    let server = Server::start(vec![(
        200,
        json!({"TableNames":[],"Unknown":"x".repeat(8*1024*1024)}).to_string(),
    )]);
    let executor = server.executor();
    let error = fetch(&executor, request("dynamodb.tables", &[], None))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("response exceeds 8 MiB"), "{error}");
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test]
async fn native_transient_retries_remain_enabled() {
    let server = Server::start(vec![
        (200, json!({"TableNames":[]}).to_string()),
        (
            500,
            json!({"__type":"InternalServerError","message":"transient fixture failure"})
                .to_string(),
        ),
        (200, json!({"TableNames":[]}).to_string()),
    ]);
    let executor = server.executor();
    // Initialize the client separately so this request measures native retry behavior.
    fetch(&executor, request("dynamodb.tables", &[], None))
        .await
        .unwrap();
    let page = fetch(&executor, request("dynamodb.tables", &[], None))
        .await
        .unwrap();
    assert!(page.rows.is_empty());
    assert_eq!(server.finish().len(), 3);
}

#[tokio::test]
async fn cancellation_discards_the_inflight_client_then_recovers() {
    let body = json!({"TableNames":[]}).to_string();
    let server = Server::delayed(
        vec![(200, body.clone()), (200, body.clone()), (200, body)],
        Duration::from_millis(150),
    );
    let executor = server.executor();
    // Isolate in-flight cancellation from the separately tested native trust-loading queue.
    fetch(&executor, request("dynamodb.tables", &[], None))
        .await
        .unwrap();
    server.requests.try_recv().unwrap();
    let (cancel, context) = RequestContext::new(Duration::from_secs(5));
    let operation = executor.fetch_page(request("dynamodb.tables", &[], None), context);
    let cancel_after_request = async {
        tokio::time::timeout(Duration::from_secs(3), async {
            while server.requests.try_recv().is_err() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        cancel.send(()).unwrap();
    };
    let (result, ()) = tokio::join!(operation, cancel_after_request);
    assert!(result.unwrap_err().to_string().contains("cancelled"));
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    fetch(&executor, request("dynamodb.tables", &[], None))
        .await
        .unwrap();
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Connected);
    assert_eq!(server.finish().len(), 1);
}

#[tokio::test]
async fn deadline_stops_a_slow_response_without_returning_rows() {
    let body = json!({"TableNames":["demo"]}).to_string();
    let server = Server::delayed(
        vec![(200, body.clone()), (200, body)],
        Duration::from_millis(500),
    );
    let executor = server.executor();
    fetch(&executor, request("dynamodb.tables", &[], None))
        .await
        .unwrap();
    let (_cancel, context) = RequestContext::new(Duration::from_millis(200));
    let error = executor
        .fetch_page(request("dynamodb.tables", &[], None), context)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"));
    assert_eq!(*executor.status().borrow(), ConnectionStatus::Disconnected);
    assert_eq!(server.finish().len(), 2);
}
