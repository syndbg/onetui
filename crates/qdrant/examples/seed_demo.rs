//! Seed only the fixed, disposable local Qdrant fixture. Existing collections are preserved.
use anyhow::{Context, Result, ensure};
use qdrant_client::qdrant::{
    CountPointsBuilder, CreateCollectionBuilder, Distance, MultiVectorComparator,
    MultiVectorConfigBuilder, NamedVectors, PointStruct, SparseVectorConfig, SparseVectorParams,
    UpsertPointsBuilder, Vector, VectorParamsBuilder, VectorParamsMap, VectorsConfig,
    vectors_config,
};
use qdrant_client::{Payload, Qdrant};
use serde_json::json;
use std::{collections::HashMap, time::Duration};

#[derive(Clone, Copy, Debug)]
enum Dataset {
    Products,
    Documents,
    Vectors,
    PayloadCases,
    Empty,
}

impl Dataset {
    const ALL: [Self; 5] = [
        Self::Products,
        Self::Documents,
        Self::Vectors,
        Self::PayloadCases,
        Self::Empty,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Products => "demo_products",
            Self::Documents => "demo_documents",
            Self::Vectors => "demo_vectors",
            Self::PayloadCases => "demo_payload_cases",
            Self::Empty => "demo_empty",
        }
    }

    fn count(self) -> u64 {
        match self {
            Self::Products => 1500,
            Self::Documents => 1200,
            Self::Vectors => 350,
            Self::PayloadCases => 12,
            Self::Empty => 0,
        }
    }

    fn collection(self) -> CreateCollectionBuilder {
        let builder = CreateCollectionBuilder::new(self.name());
        match self {
            Self::Documents => builder.vectors_config(VectorsConfig {
                config: Some(vectors_config::Config::ParamsMap(VectorParamsMap {
                    map: HashMap::from([
                        (
                            "title".into(),
                            VectorParamsBuilder::new(8, Distance::Cosine).build(),
                        ),
                        (
                            "content".into(),
                            VectorParamsBuilder::new(16, Distance::Cosine).build(),
                        ),
                    ]),
                })),
            }),
            Self::Vectors => builder
                .vectors_config(VectorsConfig {
                    config: Some(vectors_config::Config::ParamsMap(VectorParamsMap {
                        map: HashMap::from([
                            (
                                "dense".into(),
                                VectorParamsBuilder::new(8, Distance::Dot).build(),
                            ),
                            (
                                "multi".into(),
                                VectorParamsBuilder::new(8, Distance::Dot)
                                    .multivector_config(MultiVectorConfigBuilder::new(
                                        MultiVectorComparator::MaxSim,
                                    ))
                                    .build(),
                            ),
                        ]),
                    })),
                })
                .sparse_vectors_config(SparseVectorConfig {
                    map: HashMap::from([("sparse".into(), SparseVectorParams::default())]),
                }),
            _ => builder.vectors_config(VectorParamsBuilder::new(8, Distance::Cosine)),
        }
    }

    fn point(self, n: u64) -> PointStruct {
        let uuid = format!("00000000-0000-4000-8000-{n:012x}");
        let payload: Payload = json!({
            "title": format!("Synthetic item {n}"), "sku": format!("DEMO-{n:06}"),
            "category": (["books", "electronics", "home", "outdoors"][(n % 4) as usize]),
            "price": (n % 10000) as f64 / 100.0, "stock": n % 250,
            "active": n % 3 != 0, "rating": (n % 50) as f64 / 10.0,
            "tags": ["demo", "synthetic", "browse"], "scores": [1, 2, 3],
            "available": [true, false], "optional": null, "empty_text": "",
            "empty_list": [], "empty_object": {}, "uuid": uuid,
            "created_at": "2025-06-01T12:30:00Z", "location": {"lat": 42.6977, "lon": 23.3219},
            "seller": {"id": n % 20, "name": "Demo shop", "address": {"city": "София", "country": "BG"}},
            "variants": [{"size": "S", "stock": 10}, {"size": "L", "stock": 0}],
            "description": "Synthetic data: София / 東京 / São Paulo 🌊",
            "weights": [0.25, 1.5, -2.0], "mixed": [1, "two", false, null],
        }).try_into().expect("object payload");
        match self {
            Self::Documents => PointStruct::new(
                uuid.as_str(),
                NamedVectors::default()
                    .add_vector("title", Vector::new_dense(dense(n, 8)))
                    .add_vector("content", Vector::new_dense(dense(n, 16))),
                payload,
            ),
            Self::Vectors => PointStruct::new(
                n,
                NamedVectors::default()
                    .add_vector("dense", Vector::new_dense(dense(n, 8)))
                    .add_vector(
                        "sparse",
                        Vector::new_sparse(vec![1, 100 + n as u32], vec![0.5, 1.5]),
                    )
                    .add_vector(
                        "multi",
                        Vector::new_multi(vec![dense(n, 8), dense(n + 1, 8), dense(n + 2, 8)]),
                    ),
                payload,
            ),
            Self::PayloadCases => {
                let payload = match n {
                    1 => json!({}),
                    2 => json!({"value": null}),
                    3 => json!({"value": ""}),
                    4 => json!({"value": "NULL"}),
                    5 => json!({"value": []}),
                    6 => json!({"value": {}}),
                    7 => json!({"value": [1, "two", false, null]}),
                    8 => json!({"value": {"nested": {"array": [{"id": 1}, {"id": 2}]}}}),
                    9 => json!({"value": "София 東京 🌊\nsecond line\ttab"}),
                    10 => json!({"value": "Escaped control test: \u{001b}[31mred\u{001b}[0m"}),
                    11 => json!({"value": i64::MAX, "negative": i64::MIN, "fraction": -0.125}),
                    _ => json!({"value": "Long payload: София 🌊. ".repeat(600)}),
                };
                PointStruct::new(n, dense(n, 8), Payload::try_from(payload).unwrap())
            }
            _ => PointStruct::new(n, dense(n, 8), payload),
        }
    }
}

fn dense(n: u64, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|i| (((n + i as u64) % 19) as f32 + 1.0) / 20.0)
        .collect()
}

async fn seed(client: &Qdrant, dataset: Dataset) -> Result<()> {
    let name = dataset.name();
    if client.collection_exists(name).await? {
        let count = client
            .count(CountPointsBuilder::new(name).exact(true))
            .await?
            .result
            .context("missing count")?
            .count;
        ensure!(
            count == dataset.count(),
            "{name} already exists with {count} points (expected {}); left untouched. Inspect it before explicitly recreating fixtures.",
            dataset.count()
        );
        println!("Preserved {name}: {count} points");
        return Ok(());
    }
    client.create_collection(dataset.collection()).await?;
    let result: Result<()> = async {
        for start in (1..=dataset.count()).step_by(100) {
            let points = (start..=(start + 99).min(dataset.count()))
                .map(|n| dataset.point(n))
                .collect::<Vec<_>>();
            client
                .upsert_points(UpsertPointsBuilder::new(name, points).wait(true))
                .await?;
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        // Only this invocation's new collection is ours to remove after a failed seed.
        client
            .delete_collection(name)
            .await
            .context("seed failed and partial collection cleanup failed")?;
    }
    result?;
    println!("Seeded {name}: {} points", dataset.count());
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let client = Qdrant::from_url("http://127.0.0.1:16334")
        .api_key("fixture-admin-only")
        .skip_compatibility_check()
        .timeout(Duration::from_secs(10))
        .build()?;
    for dataset in Dataset::ALL {
        seed(&client, dataset).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datasets_are_bounded_and_cover_payload_and_vector_shapes() {
        assert_eq!(Dataset::ALL.iter().map(|d| d.count()).sum::<u64>(), 3062);
        assert_eq!(dense(1, 8).len(), 8);
        assert_eq!(Dataset::Products.point(1).payload.len(), 22);
        assert!(Dataset::PayloadCases.point(1).payload.is_empty());
        for dataset in Dataset::ALL {
            for n in 1..=dataset.count() {
                let point = dataset.point(n);
                assert!(point.id.is_some());
                assert!(point.vectors.is_some());
                assert!(serde_json::to_vec(&point.payload).unwrap().len() < 65536);
            }
        }
    }
}
