//! Connector experiments against the fixed disposable local Qdrant fixture.
use onetui_core::provider::{Executor, PageRequest, Provider, RequestContext, ShutdownContext};
use onetui_core::{Page, Resource};
use qdrant_client::Qdrant;
use qdrant_client::qdrant::{
    CreateCollectionBuilder, DeletePointsBuilder, Distance, GetPointsBuilder,
    MultiVectorComparator, MultiVectorConfigBuilder, NamedVectors, PointStruct,
    ScrollPointsBuilder, SparseVectorConfig, SparseVectorParams, UpsertPointsBuilder, Vector,
    VectorParamsBuilder, VectorParamsMap, VectorsConfig, points_client::PointsClient,
    vector_output, vectors_config,
};
use std::io::Write;
use std::process::Command;
use std::time::Duration;

const QDRANT: &str = "http://127.0.0.1:16334";

#[tokio::test]
#[ignore = "requires the disposable two-node Qdrant fixture"]
async fn topology_reports_native_peers_local_remote_shards_and_tls_rejection() {
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(
            &toml::from_str(&format!(
                "url='{QDRANT}'\nrest_url='http://127.0.0.1:16333'\napi_key_env='KEY'"
            ))
            .unwrap(),
            &|_| Some("fixture-reader-only".into()),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let peers = fetch(&executor, Resource::new("qdrant.peers", vec![]), None)
                .await
                .unwrap();
            if peers.rows.len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let cluster = fetch(&executor, Resource::new("qdrant.cluster", vec![]), None)
        .await
        .unwrap();
    assert_eq!(
        cluster.rows[0].cells[0].as_ref().unwrap().text(),
        Some("enabled")
    );
    let client = Qdrant::from_url(QDRANT)
        .api_key("fixture-admin-only")
        .skip_compatibility_check()
        .build()
        .unwrap();
    let name = format!("topology_fixture_{}", std::process::id());
    client
        .create_collection(
            CreateCollectionBuilder::new(&name)
                .vectors_config(VectorParamsBuilder::new(2, Distance::Cosine))
                .shard_number(2)
                .replication_factor(2),
        )
        .await
        .unwrap();
    let result = async {
        let shards = fetch(
            &executor,
            Resource::new("qdrant.shards", vec![name.clone()]),
            None,
        )
        .await?;
        assert_eq!(shards.rows.len(), 4);
        assert!(
            shards
                .rows
                .iter()
                .any(|r| r.cells[2].as_ref().unwrap().text() == Some("local"))
        );
        assert!(
            shards
                .rows
                .iter()
                .any(|r| r.cells[2].as_ref().unwrap().text() == Some("remote"))
        );
        let details = fetch(
            &executor,
            Resource::new("qdrant.collection_cluster", vec![name.clone()]),
            None,
        )
        .await?;
        assert!(
            details.rows[0].cells[0]
                .as_ref()
                .unwrap()
                .text()
                .unwrap()
                .contains("shard_transfers")
        );
        fetch(
            &executor,
            Resource::new("qdrant.transfers", vec![name.clone()]),
            None,
        )
        .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    client.delete_collection(&name).await.unwrap();
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    result.unwrap();

    let mut tls = onetui_qdrant::QdrantProvider
        .configure(
            &toml::from_str(&format!(
                "url='{QDRANT}'\nrest_url='https://localhost:16336'\napi_key_env='KEY'"
            ))
            .unwrap(),
            &|_| Some("fixture-reader-only".into()),
        )
        .unwrap();
    let error = fetch(&tls, Resource::new("qdrant.cluster", vec![]), None)
        .await
        .unwrap_err()
        .to_string();
    assert!(!error.contains("fixture-reader-only"));
    assert!(
        error.to_lowercase().contains("certificate") || error.to_lowercase().contains("cert"),
        "{error}"
    );
    tls.shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
}

async fn filtered_scroll(
    executor: &onetui_qdrant::QdrantExecutor,
    text: &str,
    continuation: Option<String>,
) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    executor
        .query_page(
            onetui_core::provider::QueryRequest {
                page: PageRequest {
                    resource: Resource::new("qdrant.query", vec!["demo_products".into()]),
                    continuation,
                },
                text: text.into(),
            },
            context,
        )
        .await
}

#[tokio::test]
#[ignore = "requires make dev-seed in the local Qdrant fixture; read-only"]
async fn filtered_scroll_pages_replay_and_reject_changed_query() {
    let mut executor = browser();
    let text = r#"{"filter":{"must":[{"key":"active","match":{"value":true}},{"key":"stock","range":{"gte":1}}]},"limit":25}"#;
    let first = filtered_scroll(&executor, text, None).await.unwrap();
    assert_eq!(first.rows.len(), 25);
    let token = first.continuation.unwrap();
    let second = filtered_scroll(&executor, text, Some(token.clone()))
        .await
        .unwrap();
    let replay = filtered_scroll(&executor, text, Some(token.clone()))
        .await
        .unwrap();
    assert_eq!(second.rows[0].cells, replay.rows[0].cells);
    assert_ne!(second.rows[0].cells, first.rows[0].cells);
    assert!(
        filtered_scroll(&executor, "{}", Some(token))
            .await
            .unwrap_err()
            .to_string()
            .contains("another query")
    );
    let client = Qdrant::from_url(QDRANT)
        .api_key("fixture-reader-only")
        .skip_compatibility_check()
        .build()
        .unwrap();
    let ids = second
        .rows
        .iter()
        .map(|row| {
            row.target.as_ref().unwrap().path[1]
                .parse::<u64>()
                .unwrap()
                .into()
        })
        .collect::<Vec<_>>();
    let points = client
        .get_points(GetPointsBuilder::new("demo_products", ids).with_payload(true))
        .await
        .unwrap();
    for point in points.result {
        let payload = serde_json::to_value(point.payload).unwrap();
        assert_eq!(payload["active"], true);
        assert!(payload["stock"].as_f64().unwrap() >= 1.0);
    }
    let none = filtered_scroll(
        &executor,
        r#"{"filter":{"must":[{"key":"sku","match":{"value":"absent-test-value"}}]}}"#,
        None,
    )
    .await
    .unwrap();
    assert!(none.rows.is_empty() && !none.next);
    let (cancel, context) = RequestContext::new(Duration::from_secs(5));
    cancel.send(()).unwrap();
    let error = executor
        .query_page(
            onetui_core::provider::QueryRequest {
                page: PageRequest {
                    resource: Resource::new("qdrant.query", vec!["demo_products".into()]),
                    continuation: None,
                },
                text: "{}".into(),
            },
            context,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cancelled"));
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires make dev-seed in the local Qdrant fixture"]
async fn demo_data_browses_all_points_and_vector_shapes() {
    let mut executor = browser();
    for (name, count) in [
        ("demo_products", 1500),
        ("demo_documents", 1200),
        ("demo_vectors", 350),
        ("demo_payload_cases", 12),
        ("demo_empty", 0),
    ] {
        let resource = Resource::new("qdrant.points", vec![name.into()]);
        let mut token = None;
        let mut seen = std::collections::HashSet::new();
        let mut bookmarks = Vec::new();
        loop {
            let page = fetch(&executor, resource.clone(), token.clone())
                .await
                .unwrap();
            bookmarks.push((
                token.clone(),
                page.rows
                    .iter()
                    .map(|row| row.cells[0].clone())
                    .collect::<Vec<_>>(),
            ));
            assert!(page.rows.len() <= 100);
            for row in page.rows {
                assert!(seen.insert(row.cells[0].clone()), "duplicate in {name}");
            }
            if !page.next {
                break;
            }
            token = Some(page.continuation.expect("next page needs a token"));
        }
        assert_eq!(seen.len(), count, "{name}");
        for (position, ids) in bookmarks.into_iter().rev() {
            let replay = fetch(&executor, resource.clone(), position).await.unwrap();
            assert_eq!(
                replay
                    .rows
                    .iter()
                    .map(|row| row.cells[0].clone())
                    .collect::<Vec<_>>(),
                ids
            );
        }
    }
    let client = Qdrant::from_url(QDRANT)
        .api_key("fixture-reader-only")
        .skip_compatibility_check()
        .build()
        .unwrap();
    let products = client
        .get_points(GetPointsBuilder::new("demo_products", [1_u64.into()]).with_payload(true))
        .await
        .unwrap();
    assert_eq!(products.result[0].payload.len(), 22);
    let vectors = client
        .get_points(GetPointsBuilder::new("demo_vectors", [1_u64.into()]).with_vectors(true))
        .await
        .unwrap();
    let vectors = vectors.result[0].vectors.as_ref().unwrap();
    assert!(
        matches!(vectors.get_vector_by_name("dense"), Some(vector_output::Vector::Dense(v)) if v.data.len() == 8)
    );
    assert!(
        matches!(vectors.get_vector_by_name("sparse"), Some(vector_output::Vector::Sparse(v)) if v.indices.len() == 2)
    );
    assert!(
        matches!(vectors.get_vector_by_name("multi"), Some(vector_output::Vector::MultiDense(v)) if v.vectors.len() == 3)
    );
    for id in ["1", "9", "10", "12"] {
        let payload = fetch(
            &executor,
            Resource::new(
                "qdrant.payload",
                vec!["demo_payload_cases".into(), id.into()],
            ),
            None,
        )
        .await
        .unwrap();
        assert!(!payload.rows.is_empty());
    }
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
}

#[cfg(unix)]
#[path = "support/terminal.rs"]
mod terminal;

fn browser() -> onetui_qdrant::QdrantExecutor {
    onetui_qdrant::QdrantProvider
        .configure(
            &toml::from_str(&format!("url='{QDRANT}'\napi_key_env='FIXTURE_KEY'")).unwrap(),
            &|_| Some("fixture-reader-only".into()),
        )
        .unwrap()
}

async fn fetch(
    executor: &onetui_qdrant::QdrantExecutor,
    resource: Resource,
    continuation: Option<String>,
) -> anyhow::Result<Page> {
    let (_cancel, context) = RequestContext::new(Duration::from_secs(5));
    let resource_id = resource.id;
    let started = std::time::Instant::now();
    let result = executor
        .fetch_page(
            PageRequest {
                resource,
                continuation,
            },
            context,
        )
        .await;
    eprintln!(
        "qdrant {resource_id} fetch/decode/format: {:?}",
        started.elapsed()
    );
    result
}

#[tokio::test]
#[ignore = "creates only its own collection in the disposable Qdrant fixture"]
async fn production_browser_pages_metadata_payload_and_removed_points() {
    let client = Qdrant::from_url(QDRANT)
        .api_key("fixture-admin-only")
        .skip_compatibility_check()
        .build()
        .unwrap();
    let name = format!("onetui_browser_{}", std::process::id());
    client
        .create_collection(
            CreateCollectionBuilder::new(&name)
                .vectors_config(VectorParamsBuilder::new(3, Distance::Dot)),
        )
        .await
        .unwrap();
    let mut executor = browser();
    let result = async {
        let list = fetch(&executor, Resource::new("qdrant.collections", vec![]), None).await?;
        let collection = list
            .rows
            .iter()
            .find(|r| r.cells[0].as_ref().and_then(onetui_core::Value::text) == Some(&name))
            .unwrap()
            .target
            .clone()
            .unwrap();
        let menu = fetch(&executor, collection, None).await?;
        let points = menu.rows[0].target.clone().unwrap();
        assert!(
            fetch(&executor, points.clone(), None)
                .await?
                .rows
                .is_empty()
        );
        let metadata = fetch(&executor, menu.rows[1].target.clone().unwrap(), None).await?;
        assert!(metadata.rows.iter().any(
            |r| r.cells[0].as_ref().and_then(onetui_core::Value::text)
                == Some("points_count (approximate)")
                && r.cells[1].as_ref().and_then(onetui_core::Value::text) == Some("0")
        ));
        let mut records: Vec<_> = (1_u64..=105)
            .map(|id| {
                PointStruct::new(
                    id,
                    vec![1.0, 2.0, 3.0],
                    [("title", format!("point-{id}").into())],
                )
            })
            .collect();
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        records.push(PointStruct::new(
            uuid,
            vec![1.0, 2.0, 3.0],
            [(
                "nested",
                serde_json::json!({"null": null, "array": [1, "София\u{001b}[31m"]}).into(),
            )],
        ));
        records.push(PointStruct::new(
            u64::MAX,
            vec![1.0, 2.0, 3.0],
            qdrant_client::Payload::default(),
        ));
        client
            .upsert_points(UpsertPointsBuilder::new(&name, records).wait(true))
            .await?;
        let first = fetch(&executor, points.clone(), None).await?;
        assert_eq!(first.rows.len(), 100);
        assert_eq!(
            first.rows[0].cells[0]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("1")
        );
        assert!(first.rows.iter().all(|r| r.cells.len() == 2));
        let token = first.continuation.clone();
        let last = fetch(&executor, points.clone(), token.clone()).await?;
        assert_eq!(last.rows.len(), 7);
        assert_eq!(
            last.rows[0].cells[0]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("101")
        );
        assert!(!last.next);
        assert!(last.continuation.is_none());
        let uuid_row = last
            .rows
            .iter()
            .find(|r| r.cells[0].as_ref().and_then(onetui_core::Value::text) == Some(uuid))
            .unwrap();
        let menu = fetch(&executor, uuid_row.target.clone().unwrap(), None).await?;
        let payload = fetch(&executor, menu.rows[0].target.clone().unwrap(), None).await?;
        assert!(matches!(
            &payload.rows[0].cells[0],
            Some(onetui_core::Value::Json(_))
        ));
        assert!(
            payload.rows[0].cells[0]
                .as_ref()
                .unwrap()
                .text()
                .unwrap()
                .contains("София")
        );
        assert!(
            !payload.rows[0].cells[0]
                .as_ref()
                .unwrap()
                .text()
                .unwrap()
                .contains('\x1b')
        );
        let vectors = fetch(&executor, menu.rows[1].target.clone().unwrap(), None).await?;
        assert_eq!(
            vectors.rows[0].cells[1]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("dense")
        );
        assert_eq!(
            vectors.rows[0].cells[3]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("[1.0,2.0,3.0]")
        );
        let empty_payload = fetch(
            &executor,
            Resource::new("qdrant.payload", vec![name.clone(), u64::MAX.to_string()]),
            None,
        )
        .await?;
        assert_eq!(
            empty_payload.rows[0].cells[0]
                .as_ref()
                .and_then(onetui_core::Value::text),
            Some("{}")
        );
        assert!(
            fetch(&browser(), points.clone(), token.clone())
                .await
                .is_err()
        );
        assert!(
            fetch(
                &executor,
                Resource::new("qdrant.points", vec!["other".into()]),
                token
            )
            .await
            .is_err()
        );
        let refreshed = fetch(&executor, points.clone(), None).await?;
        assert_eq!(refreshed.rows[0].cells[0], first.rows[0].cells[0]);
        #[cfg(unix)]
        terminal::journey(&name);
        client
            .delete_points(
                DeletePointsBuilder::new(&name)
                    .points(vec![qdrant_client::qdrant::PointId::from(uuid)])
                    .wait(true),
            )
            .await?;
        let error = fetch(&executor, menu.rows[0].target.clone().unwrap(), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("disappeared"));
        assert!(fetch(&executor, points, None).await.is_ok());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
    client.delete_collection(&name).await.unwrap();
    result.unwrap();
}

#[tokio::test]
#[ignore = "creates a uniquely named collection ONLY in the disposable Qdrant fixture"]
async fn qdrant_scroll_and_lazy_details() {
    let client = Qdrant::from_url(QDRANT)
        .api_key("fixture-admin-only")
        .skip_compatibility_check()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let name = format!("onetui_fixture_{}", std::process::id());
    client
        .create_collection(
            CreateCollectionBuilder::new(&name)
                .vectors_config(VectorParamsBuilder::new(3, Distance::Cosine)),
        )
        .await
        .unwrap();
    let result = async {
        client
            .upsert_points(
                UpsertPointsBuilder::new(
                    &name,
                    vec![
                        PointStruct::new(1_u64, vec![1.0, 0.0, 0.0], [("title", "first".into())]),
                        PointStruct::new(2_u64, vec![0.0, 1.0, 0.0], [("title", "second".into())]),
                        PointStruct::new(
                            "550e8400-e29b-41d4-a716-446655440000",
                            vec![0.0, 0.0, 1.0],
                            [("title", "uuid".into())],
                        ),
                    ],
                )
                .wait(true),
            )
            .await?;
        let page = client
            .scroll(
                ScrollPointsBuilder::new(&name)
                    .limit(2)
                    .with_payload(false)
                    .with_vectors(false),
            )
            .await?;
        assert_eq!(page.result.len(), 2);
        assert!(
            page.result
                .iter()
                .all(|p| p.payload.is_empty() && p.vectors.is_none())
        );
        let next = client
            .scroll(
                ScrollPointsBuilder::new(&name)
                    .limit(2)
                    .offset(page.next_page_offset.unwrap())
                    .with_payload(false)
                    .with_vectors(false),
            )
            .await?;
        assert_eq!(next.result.len(), 1);
        assert!(next.next_page_offset.is_none());
        let id = next.result[0].id.clone().unwrap();
        assert!(matches!(
            id.point_id_options,
            Some(qdrant_client::qdrant::point_id::PointIdOptions::Uuid(_))
        ));
        let detail = client
            .get_points(
                GetPointsBuilder::new(&name, vec![id])
                    .with_payload(true)
                    .with_vectors(true),
            )
            .await?;
        assert_eq!(detail.result.len(), 1);
        assert!(!detail.result[0].payload.is_empty());
        assert!(detail.result[0].vectors.is_some());
        let numeric = client
            .get_points(
                GetPointsBuilder::new(&name, vec![1_u64.into()])
                    .with_payload(true)
                    .with_vectors(false),
            )
            .await?;
        assert_eq!(numeric.result.len(), 1);
        assert!(!numeric.result[0].payload.is_empty());
        assert!(numeric.result[0].vectors.is_none());
        client
            .delete_points(
                DeletePointsBuilder::new(&name)
                    .points(vec![qdrant_client::qdrant::PointId::from(1_u64)])
                    .wait(true),
            )
            .await?;
        let missing = client
            .get_points(GetPointsBuilder::new(&name, vec![1_u64.into()]))
            .await?;
        assert!(missing.result.is_empty());
        Ok::<_, qdrant_client::QdrantError>(())
    }
    .await;
    client.delete_collection(&name).await.unwrap();
    result.unwrap();
}

#[tokio::test]
#[ignore = "creates only its own collection in the disposable Qdrant fixture"]
async fn qdrant_large_payload_and_vector_variants() {
    let client = Qdrant::from_url(QDRANT)
        .api_key("fixture-admin-only")
        .skip_compatibility_check()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let name = format!("onetui_variants_{}", std::process::id());
    let params = std::collections::HashMap::from([
        (
            "dense".to_owned(),
            VectorParamsBuilder::new(3, Distance::Dot).build(),
        ),
        (
            "multi".to_owned(),
            VectorParamsBuilder::new(3, Distance::Dot)
                .multivector_config(MultiVectorConfigBuilder::new(MultiVectorComparator::MaxSim))
                .build(),
        ),
    ]);
    client
        .create_collection(
            CreateCollectionBuilder::new(&name)
                .vectors_config(VectorsConfig {
                    config: Some(vectors_config::Config::ParamsMap(VectorParamsMap {
                        map: params,
                    })),
                })
                .sparse_vectors_config(SparseVectorConfig {
                    map: std::collections::HashMap::from([(
                        "sparse".to_owned(),
                        SparseVectorParams::default(),
                    )]),
                }),
        )
        .await
        .unwrap();
    let result = async {
        let empty = client.scroll(ScrollPointsBuilder::new(&name).limit(2)).await?;
        assert!(empty.result.is_empty());
        assert!(empty.next_page_offset.is_none());
        let vectors = NamedVectors::default()
            .add_vector("dense", Vector::new_dense(vec![1.0, 2.0, 3.0]))
            .add_vector("sparse", Vector::new_sparse(vec![3, 1000], vec![0.5, 1.5]))
            .add_vector("multi", Vector::new_multi(vec![vec![1.0, 0.0, 0.0], vec![0.0, 1.0, 0.0]]));
        client.upsert_points(UpsertPointsBuilder::new(&name, vec![
            PointStruct::new(1_u64, vectors, [("large", "x".repeat(2 * 1024 * 1024).into())]),
        ]).wait(true)).await?;
        let page = client.scroll(ScrollPointsBuilder::new(&name).limit(2).with_payload(false).with_vectors(false)).await?;
        assert_eq!(page.result.len(), 1);
        assert!(page.result[0].payload.is_empty());
        assert!(page.result[0].vectors.is_none());

        let detail = client.get_points(GetPointsBuilder::new(&name, vec![1_u64.into()])
            .with_payload(false).with_vectors(true)).await?;
        let vectors = detail.result[0].vectors.as_ref().unwrap();
        assert!(matches!(vectors.get_vector_by_name("dense"), Some(vector_output::Vector::Dense(v)) if v.data == [1.0, 2.0, 3.0]));
        assert!(matches!(vectors.get_vector_by_name("sparse"), Some(vector_output::Vector::Sparse(v)) if v.indices == [3, 1000] && v.values == [0.5, 1.5]));
        assert!(matches!(vectors.get_vector_by_name("multi"), Some(vector_output::Vector::MultiDense(v)) if v.vectors.len() == 2 && v.vectors.iter().all(|v| v.data.len() == 3)));

        let channel = tonic::transport::Endpoint::from_static(QDRANT)
            .connect_timeout(Duration::from_secs(5)).timeout(Duration::from_secs(5)).connect().await.unwrap();
        let mut bounded = PointsClient::new(channel).max_decoding_message_size(1024 * 1024);
        let request: qdrant_client::qdrant::GetPoints = GetPointsBuilder::new(&name, vec![1_u64.into()])
            .with_payload(true).with_vectors(false).build();
        let mut request = tonic::Request::new(request);
        request.metadata_mut().insert("api-key", "fixture-reader-only".parse().unwrap());
        let error = bounded.get(request).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::OutOfRange);
        // A rejected detail request must not make an ID-only retry fail.
        let retry = client.scroll(ScrollPointsBuilder::new(&name).limit(2).with_payload(false).with_vectors(false)).await?;
        assert_eq!(retry.result[0].id, page.result[0].id);
        let mut executor = browser();
        let points = Resource::new("qdrant.points", vec![name.clone()]);
        let first = fetch(&executor, points.clone(), None).await?;
        let menu = fetch(&executor, first.rows[0].target.clone().unwrap(), None).await?;
        let error = fetch(&executor, menu.rows[0].target.clone().unwrap(), None).await.unwrap_err();
        assert!(error.to_string().contains("OutOfRange (11)"));
        assert!(error.to_string().contains("1048576"));
        let vectors = fetch(&executor, menu.rows[1].target.clone().unwrap(), None).await?;
        assert_eq!(vectors.rows.iter().map(|r| r.cells[0].as_ref().and_then(onetui_core::Value::text).unwrap()).collect::<Vec<_>>(), ["dense", "multi", "sparse"]);
        assert_eq!(vectors.rows[0].cells[3].as_ref().and_then(onetui_core::Value::text), Some("[1.0,2.0,3.0]"));
        assert_eq!(vectors.rows[1].cells[2].as_ref().and_then(onetui_core::Value::text), Some("2 x 3"));
        assert_eq!(vectors.rows[2].cells[3].as_ref().and_then(onetui_core::Value::text), Some("{\"indices\":[3,1000],\"values\":[0.5,1.5]}"));
        assert_eq!(fetch(&executor, points, None).await?.rows[0].cells[0], first.rows[0].cells[0]);
        executor.shutdown(ShutdownContext::new(Duration::from_secs(1))).await?;
        Ok::<_, anyhow::Error>(())
    }.await;
    client.delete_collection(&name).await.unwrap();
    result.unwrap();
}
fn binary() -> Command {
    let path = std::env::var_os("ONETUI_TEST_BIN")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/onetui")
        });
    assert!(
        path.is_file(),
        "Build the CLI first with make build, or set ONETUI_TEST_BIN"
    );
    Command::new(path)
}
fn fixture_ca(service: &str, path: &str) -> tempfile::NamedTempFile {
    let cert = Command::new("docker")
        .args([
            "compose",
            "-f",
            "hack/compose.yaml",
            "exec",
            "-T",
            service,
            "cat",
            path,
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .output()
        .unwrap();
    assert!(cert.status.success());
    let mut ca = tempfile::NamedTempFile::new().unwrap();
    ca.write_all(&cert.stdout).unwrap();
    ca
}

#[test]
#[ignore = "requires the disposable Qdrant fixture"]
fn qdrant_cli_check_and_auth_failure() {
    let check = |key: &str| {
        binary()
            .args([
                "--check",
                "--config",
                "hack/connections.toml",
                "--connection",
                "local_qdrant",
            ])
            .env("ONETUI_QDRANT_API_KEY", key)
            .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
            .output()
            .unwrap()
    };
    assert!(check("fixture-reader-only").status.success());
    let output = check("fake-wrong-secret");
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("fake-wrong-secret"));
}
#[test]
#[ignore = "requires the disposable Docker Compose Qdrant TLS fixture"]
fn qdrant_https_verifies_trust_hostname_and_authentication() {
    let ca = fixture_ca("qdrant-tls", "/qdrant/tls/ca.crt");
    let unrelated_dir = tempfile::tempdir().unwrap();
    let unrelated_ca = unrelated_dir.path().join("ca.pem");
    let key = unrelated_dir.path().join("key.pem");
    let output = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=onetui-unrelated-test-ca",
            "-keyout",
        ])
        .arg(key)
        .arg("-out")
        .arg(&unrelated_ca)
        .output()
        .unwrap();
    assert!(output.status.success(), "test CA generation failed");
    let empty_cert_dir = tempfile::tempdir().unwrap();
    for (endpoint, trusted_ca, key, expected) in [
        (
            "https://localhost:16335",
            ca.path(),
            "fixture-reader-only",
            None,
        ),
        (
            "https://127.0.0.1:16335",
            ca.path(),
            "fixture-reader-only",
            Some("certificate not valid for name"),
        ),
        (
            "https://localhost:16335",
            unrelated_ca.as_path(),
            "fixture-reader-only",
            Some("UnknownIssuer"),
        ),
        (
            "https://localhost:16335",
            ca.path(),
            "fake-wrong-secret",
            Some("Unauthenticated"),
        ),
        (
            "http://localhost:16335",
            ca.path(),
            "fixture-reader-only",
            Some("Qdrant"),
        ),
        (
            "https://localhost:16334",
            ca.path(),
            "fixture-reader-only",
            Some("Qdrant connection: transport error:"),
        ),
        (
            "https://localhost:16335",
            ca.path(),
            "fixture-reader-only",
            None,
        ),
    ] {
        let mut config = tempfile::NamedTempFile::new().unwrap();
        writeln!(config, "[connections.tls]\nkind='qdrant'\nurl='{endpoint}'\napi_key_env='ONETUI_QDRANT_API_KEY'").unwrap();
        let output = binary()
            .args(["--check", "--connection", "tls", "--config"])
            .arg(config.path())
            .args(["--timeout", "2"])
            // rustls-native-certs reads these in the child only; the OS trust store is untouched.
            .env("SSL_CERT_FILE", trusted_ca)
            .env("SSL_CERT_DIR", empty_cert_dir.path())
            .env("ONETUI_QDRANT_API_KEY", key)
            .output()
            .unwrap();
        let error = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.success(), expected.is_none(), "{error}");
        if let Some(expected) = expected {
            assert!(error.contains(expected), "{error}");
            assert!(output.stdout.is_empty());
        } else {
            assert!(String::from_utf8_lossy(&output.stdout).contains("OK tls (qdrant)"));
        }
        assert!(!error.contains("fake-wrong-secret"));
        assert!(!error.contains("fixture-reader-only"));
        assert!(!error.contains('\u{1b}'));
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "topology_tls_child", "--nocapture"])
            .env(
                "ONETUI_TEST_QDRANT_REST_URL",
                endpoint.replace("16335", "16336").replace("16334", "16333"),
            )
            .env(
                "ONETUI_TEST_QDRANT_REST_OK",
                if expected.is_none() { "yes" } else { "no" },
            )
            .env("SSL_CERT_FILE", trusted_ca)
            .env("SSL_CERT_DIR", empty_cert_dir.path())
            .env("ONETUI_QDRANT_API_KEY", key)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn topology_tls_child() {
    let Ok(url) = std::env::var("ONETUI_TEST_QDRANT_REST_URL") else {
        return;
    };
    let key = std::env::var("ONETUI_QDRANT_API_KEY").unwrap();
    let mut executor = onetui_qdrant::QdrantProvider
        .configure(
            &toml::from_str(&format!(
                "url='{QDRANT}'\nrest_url='{url}'\napi_key_env='KEY'"
            ))
            .unwrap(),
            &|_| Some(key.clone()),
        )
        .unwrap();
    let result = fetch(&executor, Resource::new("qdrant.cluster", vec![]), None).await;
    if std::env::var("ONETUI_TEST_QDRANT_REST_OK").unwrap() == "yes" {
        let page = result.unwrap();
        assert_eq!(
            page.rows[0].cells[0].as_ref().unwrap().text(),
            Some("disabled")
        );
    } else {
        let error = result.unwrap_err().to_string();
        assert!(!error.contains(&key));
        assert!(!error.contains('\x1b'));
    }
    executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await
        .unwrap();
}
