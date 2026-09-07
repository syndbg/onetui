//! Connector experiments against the fixed disposable local Qdrant fixture.
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
        Ok::<_, qdrant_client::QdrantError>(())
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
            Some("TLS certificate trust"),
        ),
        (
            "https://localhost:16335",
            unrelated_ca.as_path(),
            "fixture-reader-only",
            Some("TLS certificate trust"),
        ),
        (
            "https://localhost:16335",
            ca.path(),
            "fake-wrong-secret",
            Some("authentication failed"),
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
            Some("TLS certificate trust"),
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
    }
}
