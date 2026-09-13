use super::*;

const ARN: &str =
    "arn:aws:dynamodb:us-east-1:123456789012:table/demo/stream/2026-09-14T00:00:00.000";
const SHARD: &str = "shardId-00000000000000000000-test";

fn replies(values: Vec<Json>) -> Vec<(u16, String)> {
    values.into_iter().map(|v| (200, v.to_string())).collect()
}
fn record(sequence: &str) -> Json {
    json!({"eventName":"MODIFY","eventID":format!("event-{sequence}"),"awsRegion":"us-east-1",
        "dynamodb":{"SequenceNumber":sequence,"StreamViewType":"NEW_AND_OLD_IMAGES",
        "Keys":{"pk":{"S":"demo"}},"OldImage":{"binary":{"B":"AP8="}},
        "NewImage":{"precise":{"N":"12345678901234567890123456789012345678"}}},
        "userIdentity":{"Type":"Service","PrincipalId":"dynamodb.amazonaws.com"},"futureField":{"retained":true}})
}
fn records_request(continuation: Option<String>) -> PageRequest {
    request("dynamodb.records", &[ARN, SHARD], continuation)
}

#[tokio::test]
async fn inventory_shards_and_descriptions_use_only_native_streams_reads() {
    let server = Server::start(replies(vec![
        json!({"Streams":[{"StreamArn":ARN,"TableName":"demo","StreamLabel":"label"}],"LastEvaluatedStreamArn":ARN}),
        json!({"Streams":[]}),
        json!({"StreamDescription":{"StreamArn":ARN,"StreamStatus":"ENABLED","Shards":[{"ShardId":SHARD,"ParentShardId":"parent","SequenceNumberRange":{"StartingSequenceNumber":"10"}}],"LastEvaluatedShardId":SHARD}}),
        json!({"StreamDescription":{"StreamArn":ARN,"Shards":[],"FutureMetadata":"retained"}}),
    ]));
    let e = server.executor();
    let page = fetch(&e, request("dynamodb.streams", &[], None))
        .await
        .unwrap();
    assert_eq!(
        page.rows[0].target,
        Some(Resource::new("dynamodb.stream", vec![ARN.into()]))
    );
    let menu = fetch(&e, request("dynamodb.stream", &[ARN], None))
        .await
        .unwrap();
    assert_eq!(menu.rows.len(), 2);
    fetch(&e, request("dynamodb.streams", &[], page.continuation))
        .await
        .unwrap();
    let shards = fetch(&e, request("dynamodb.shards", &[ARN], None))
        .await
        .unwrap();
    assert_eq!(shards.rows[0].target, Some(records_request(None).resource));
    assert_eq!(
        cell(&shards, 0, "description").unwrap()["ParentShardId"],
        "parent"
    );
    let info = fetch(&e, request("dynamodb.stream_info", &[ARN], None))
        .await
        .unwrap();
    assert_eq!(
        cell(&info, 0, "description").unwrap()["FutureMetadata"],
        "retained"
    );
    let calls = server.finish();
    assert!(calls[0].0.contains("DynamoDBStreams_20120810.ListStreams"));
    assert_eq!(calls[1].1["ExclusiveStartStreamArn"], ARN);
    assert!(
        calls[2]
            .0
            .contains("DynamoDBStreams_20120810.DescribeStream")
    );
    assert_eq!(calls[2].1["StreamArn"], ARN);
    assert_eq!(calls[2].1["Limit"], 100);
}

#[tokio::test]
async fn sequence_bookmarks_renew_expired_iterators_and_replay_closed_pages() {
    let expired = (
        400,
        json!({"__type":"ExpiredIteratorException","message":"native expired iterator"})
            .to_string(),
    );
    let mut responses = replies(vec![
        json!({"ShardIterator":"initial"}),
        json!({"Records":[record("10"), record("20")],"NextShardIterator":"old"}),
    ]);
    responses.push(expired.clone());
    responses.extend(replies(vec![
        json!({"ShardIterator":"renewed"}),
        json!({"Records":[record("30")]}),
    ]));
    responses.push(expired);
    responses.extend(replies(vec![
        json!({"ShardIterator":"replayed"}),
        json!({"Records":[record("30")]}),
    ]));
    let server = Server::start(responses);
    let e = server.executor();
    let first = fetch(&e, records_request(None)).await.unwrap();
    assert_eq!(cell(&first, 0, "record").unwrap(), record("10"));
    let bookmark = first.continuation.unwrap();
    let last = fetch(&e, records_request(Some(bookmark.clone())))
        .await
        .unwrap();
    assert_eq!(cell(&last, 0, "record").unwrap(), record("30"));
    assert!(!last.next);
    assert!(
        fetch(&e, records_request(last.continuation))
            .await
            .unwrap_err()
            .to_string()
            .contains("shard is closed")
    );
    let replay = fetch(&e, records_request(Some(bookmark.clone())))
        .await
        .unwrap();
    assert_eq!(cell(&replay, 0, "record").unwrap(), record("30"));
    let mut wrong = records_request(Some(bookmark));
    wrong.resource.path[1] = "different-shard".into();
    assert!(fetch(&e, wrong).await.is_err());
    let calls = server.finish();
    assert_eq!(calls[0].1["ShardIteratorType"], "TRIM_HORIZON");
    assert_eq!(calls[3].1["ShardIteratorType"], "AFTER_SEQUENCE_NUMBER");
    assert_eq!(calls[3].1["SequenceNumber"], "20");
    assert_eq!(calls[6].1["SequenceNumber"], "20");
    assert!(calls[1].0.contains("DynamoDBStreams_20120810.GetRecords"));
    assert_eq!(calls[1].1["Limit"], 100);
}

#[tokio::test]
async fn empty_follow_pages_advance_iterators_and_expiry_never_resets_latest() {
    let mut responses = replies(vec![
        json!({"ShardIterator":"latest"}),
        json!({"Records":[],"NextShardIterator":"empty-next"}),
        json!({"Records":[],"NextShardIterator":"empty-later"}),
    ]);
    responses.push((
        400,
        json!({"__type":"ExpiredIteratorException","message":"native expiration fixture-secret"})
            .to_string(),
    ));
    let server = Server::start(responses);
    let e = server.executor();
    async fn follow(e: &DynamoDbExecutor, bookmark: Option<String>) -> anyhow::Result<Page> {
        let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
        e.follow_page(records_request(bookmark), context).await
    }
    let first = follow(&e, None).await.unwrap();
    assert!(first.rows.is_empty() && first.continuation.is_some());
    assert!(
        fetch(&e, records_request(first.continuation.clone()))
            .await
            .is_err()
    );
    let second = follow(&e, first.continuation).await.unwrap();
    let error = follow(&e, second.continuation)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("ExpiredIteratorException") && error.contains("native expiration"),
        "{error}"
    );
    assert!(!error.contains("fixture-secret"));
    let calls = server.finish();
    assert_eq!(calls.len(), 4);
    assert_eq!(calls[0].1["ShardIteratorType"], "LATEST");
    assert_eq!(calls[2].1["ShardIterator"], "empty-next");
    assert_eq!(calls[3].1["ShardIterator"], "empty-later");
}

#[tokio::test]
async fn display_byte_limit_restarts_after_last_retained_sequence_without_skipping() {
    let mut large = record("10");
    large["dynamodb"]["NewImage"] = json!({"text":{"S":"x".repeat(360000)}});
    let mut second = large.clone();
    second["dynamodb"]["SequenceNumber"] = json!("20");
    let mut third = large.clone();
    third["dynamodb"]["SequenceNumber"] = json!("30");
    let server = Server::start(replies(vec![
        json!({"ShardIterator":"initial"}),
        json!({"Records":[large,second,third.clone()],"NextShardIterator":"would-skip-third"}),
        json!({"ShardIterator":"after-retained"}),
        json!({"Records":[third]}),
    ]));
    let e = server.executor();
    let page = fetch(&e, records_request(None)).await.unwrap();
    assert_eq!(page.rows.len(), 2);
    assert!(page.bytes() <= onetui_core::PAGE_BYTES);
    let next = fetch(&e, records_request(page.continuation)).await.unwrap();
    assert_eq!(
        cell(&next, 0, "record").unwrap()["dynamodb"]["SequenceNumber"],
        "30"
    );
    let calls = server.finish();
    assert_eq!(calls[2].1["SequenceNumber"], "20");
    assert_eq!(calls[2].1["ShardIteratorType"], "AFTER_SEQUENCE_NUMBER");
}

#[tokio::test]
async fn local_endpoint_never_falls_through_to_public_streams() {
    let e = DynamoDbProvider.configure(&toml::from_str("region='us-east-1'\nendpoint_url='http://127.0.0.1:9'\naccess_key_id_env='KEY'\nsecret_access_key_env='SECRET'").unwrap(), &|_| Some("fixture-only".into())).unwrap();
    let error = fetch(&e, request("dynamodb.streams", &[], None))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("streams_endpoint_url"));
    assert_eq!(*e.status().borrow(), ConnectionStatus::Configured);
}

#[tokio::test]
async fn sdk_model_failures_preserve_original_records_with_a_warning() {
    let mut raw = record("10");
    raw["dynamodb"]["NewImage"]["empty_map"] = json!({});
    let server = Server::start(replies(vec![
        json!({"ShardIterator":"first"}),
        json!({"Records":[raw.clone()]}),
    ]));
    let e = server.executor();
    let page = fetch(&e, records_request(None)).await.unwrap();
    assert_eq!(cell(&page, 0, "record").unwrap(), raw);
    assert!(
        page.notice
            .contains("Union did not contain a valid variant"),
        "{}",
        page.notice
    );
    assert!(page.notice.contains("showing original JSON"));
    assert!(!page.notice.contains("SdkBody"));
    assert_eq!(server.finish().len(), 2);
}

#[tokio::test]
async fn stream_http_limit_and_invalid_record_identity_do_not_advance_bookmarks() {
    let oversized = format!(
        "{{\"Records\":[],\"padding\":\"{}\"}}",
        "x".repeat(8 * 1024 * 1024)
    );
    let server = Server::start(vec![
        (200, json!({"ShardIterator":"first"}).to_string()),
        (200, oversized),
    ]);
    let e = server.executor();
    let error = fetch(&e, records_request(None))
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("response exceeds 8 MiB"), "{error}");
    assert_eq!(server.finish().len(), 2);

    let server = Server::start(replies(vec![
        json!({"ShardIterator":"first"}),
        json!({"Records":[{"eventName":"INSERT","dynamodb":{}}]}),
    ]));
    let e = server.executor();
    assert!(
        fetch(&e, records_request(None))
            .await
            .unwrap_err()
            .to_string()
            .contains("sequence")
    );
    assert_eq!(server.finish().len(), 2);
}
