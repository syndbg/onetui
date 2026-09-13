use super::*;

#[tokio::test]
#[ignore = "creates/deletes one disposable KV bucket; reader never creates consumers or acknowledges"]
async fn kv_keys_values_history_watch_and_bookmarks() {
    let admin = admin().await;
    let js = async_nats::jetstream::new(admin.clone());
    let bucket = format!("TESTKV_{}", std::process::id());
    let kv = js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: bucket.clone(),
            history: 8,
            storage: async_nats::jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .unwrap();
    let name = bucket.clone();
    let tested = tokio::spawn(async move {
        for i in 0..125 {
            kv.put(format!("key{i:03}"), format!("value{i}").into())
                .await
                .unwrap();
        }
        kv.put("history", vec![0, 255, 128].into()).await.unwrap();
        kv.put("history", "new".into()).await.unwrap();
        kv.delete("history").await.unwrap();
        let mut executor = executor();
        let first = read(&executor, "nats.kv_keys", &[&name], None, false)
            .await
            .unwrap();
        assert_eq!(first.rows.len(), 100);
        let bookmark = first.continuation.clone();
        let mut names = std::collections::BTreeSet::new();
        let mut page = first;
        loop {
            for row in page.rows {
                assert!(names.insert(row.target.unwrap().path[1].clone()));
            }
            match page.continuation {
                Some(token) => {
                    page = read(&executor, "nats.kv_keys", &[&name], Some(token), false)
                        .await
                        .unwrap()
                }
                None => break,
            }
        }
        assert_eq!(names.len(), 126);
        let replay = read(&executor, "nats.kv_keys", &[&name], bookmark, false)
            .await
            .unwrap();
        assert_eq!(replay.rows.len(), 26);
        let history = read(
            &executor,
            "nats.kv_history",
            &[&name, "history"],
            None,
            false,
        )
        .await
        .unwrap();
        assert_eq!(history.rows.len(), 3);
        assert_eq!(
            history.rows[0].cells[3],
            Some(Value::Bytes(vec![0, 255, 128]))
        );
        assert!(
            String::from_utf8_lossy(history.rows[2].cells[4].as_ref().unwrap().bytes())
                .contains("KV-Operation: DEL")
        );
        let latest = read(&executor, "nats.kv_value", &[&name, "history"], None, false)
            .await
            .unwrap();
        assert_eq!(latest.rows[0].cells, history.rows[2].cells);
        let tail = read(
            &executor,
            "nats.kv_history",
            &[&name, "history"],
            None,
            true,
        )
        .await
        .unwrap();
        kv.put("unrelated", "not a match".into()).await.unwrap();
        kv.put("history", "revived".into()).await.unwrap();
        let new = read(
            &executor,
            "nats.kv_history",
            &[&name, "history"],
            tail.continuation,
            true,
        )
        .await
        .unwrap();
        assert_eq!(new.rows.len(), 1);
        assert_eq!(
            new.rows[0].cells[3],
            Some(Value::Bytes(b"revived".to_vec()))
        );
        kv.purge("history").await.unwrap();
        let purged = read(
            &executor,
            "nats.kv_history",
            &[&name, "history"],
            None,
            false,
        )
        .await
        .unwrap();
        assert_eq!(purged.rows.len(), 1);
        assert!(
            String::from_utf8_lossy(purged.rows[0].cells[4].as_ref().unwrap().bytes())
                .contains("KV-Operation: PURGE")
        );
        close(&mut executor).await;
    })
    .await;
    let info = api(&admin, &format!("STREAM.INFO.KV_{bucket}"), json!({})).await;
    assert_eq!(info["state"]["consumer_count"], 0);
    js.delete_key_value(&bucket).await.unwrap();
    tested.unwrap();
}

#[tokio::test]
#[ignore = "creates/deletes one disposable object bucket; content inspection does not create consumers"]
async fn object_metadata_lazy_chunks_empty_deleted_and_replaced_versions() {
    let admin = admin().await;
    let js = async_nats::jetstream::new(admin.clone());
    let bucket = format!("TESTOBJ_{}", std::process::id());
    let objects = js
        .create_object_store(async_nats::jetstream::object_store::Config {
            bucket: bucket.clone(),
            storage: async_nats::jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .unwrap();
    let name = bucket.clone();
    let tested = tokio::spawn(async move {
        let object = "София / 東京.bin";
        let bytes = (0..1_200_000).map(|i| (i % 256) as u8).collect::<Vec<_>>();
        objects.put(object, &mut bytes.as_slice()).await.unwrap();
        objects.put("empty", &mut [].as_slice()).await.unwrap();
        let mut executor = executor();
        let names = read(&executor, "nats.objects", &[&name], None, false)
            .await
            .unwrap();
        assert_eq!(names.rows.len(), 2);
        assert!(
            names
                .rows
                .iter()
                .any(|r| r.target.as_ref().unwrap().path[1] == object)
        );
        let info = read(&executor, "nats.object_info", &[&name, object], None, false)
            .await
            .unwrap();
        let info: Json =
            serde_json::from_slice(info.rows[0].cells[0].as_ref().unwrap().bytes()).unwrap();
        assert_eq!(info["size"], bytes.len());
        let first = read(
            &executor,
            "nats.object_chunks",
            &[&name, object],
            None,
            false,
        )
        .await
        .unwrap();
        assert!(first.next && first.bytes() <= onetui_core::PAGE_BYTES);
        let bookmark = first.continuation.clone();
        let mut page = first;
        let mut actual = Vec::new();
        loop {
            for row in page.rows {
                actual.extend_from_slice(row.cells[3].as_ref().unwrap().bytes());
            }
            match page.continuation {
                Some(token) => {
                    page = read(
                        &executor,
                        "nats.object_chunks",
                        &[&name, object],
                        Some(token),
                        false,
                    )
                    .await
                    .unwrap()
                }
                None => break,
            }
        }
        assert_eq!(actual, bytes);
        let empty = read(
            &executor,
            "nats.object_chunks",
            &[&name, "empty"],
            None,
            false,
        )
        .await
        .unwrap();
        assert!(empty.rows.is_empty() && !empty.next);
        objects
            .put(object, &mut b"replacement".as_slice())
            .await
            .unwrap();
        assert!(
            read(
                &executor,
                "nats.object_chunks",
                &[&name, object],
                bookmark,
                false
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("changed")
        );
        objects.delete(object).await.unwrap();
        let info = read(&executor, "nats.object_info", &[&name, object], None, false)
            .await
            .unwrap();
        let info: Json =
            serde_json::from_slice(info.rows[0].cells[0].as_ref().unwrap().bytes()).unwrap();
        assert_eq!(info["deleted"], true);
        assert!(
            read(
                &executor,
                "nats.object_chunks",
                &[&name, object],
                None,
                false
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("deleted")
        );
        close(&mut executor).await;
    })
    .await;
    let info = api(&admin, &format!("STREAM.INFO.OBJ_{bucket}"), json!({})).await;
    assert_eq!(info["state"]["consumer_count"], 0);
    js.delete_object_store(&bucket).await.unwrap();
    tested.unwrap();
}

#[tokio::test]
#[ignore = "creates/deletes a disposable stream; read-only consumer inspection preserves delivery and ACK state"]
async fn consumer_configuration_and_pending_counts_are_read_only() {
    let admin = admin().await;
    let stream = format!("CONSUMERS_{}", std::process::id());
    api(
        &admin,
        &format!("STREAM.CREATE.{stream}"),
        json!({"name": stream, "subjects": [stream], "storage": "memory"}),
    )
    .await;
    api(&admin, &format!("CONSUMER.DURABLE.CREATE.{stream}.application"), json!({"stream_name": stream, "config": {"durable_name": "application", "ack_policy": "explicit", "deliver_policy": "all"}})).await;
    async_nats::jetstream::new(admin.clone())
        .publish(stream.clone(), "pending".into())
        .await
        .unwrap()
        .await
        .unwrap();
    let before = api(
        &admin,
        &format!("CONSUMER.INFO.{stream}.application"),
        json!({}),
    )
    .await;
    let name = stream.clone();
    let tested = tokio::spawn(async move {
        let mut executor = executor();
        let consumers = read(&executor, "nats.consumers", &[&name], None, false)
            .await
            .unwrap();
        assert_eq!(consumers.rows.len(), 1);
        assert_eq!(consumers.rows[0].cells[1], Some("1".into()));
        let info = read(
            &executor,
            "nats.consumer_info",
            &[&name, "application"],
            None,
            false,
        )
        .await
        .unwrap();
        let info: Json =
            serde_json::from_slice(info.rows[0].cells[0].as_ref().unwrap().bytes()).unwrap();
        assert_eq!(info["config"]["ack_policy"], "explicit");
        close(&mut executor).await;
    })
    .await;
    let after = api(
        &admin,
        &format!("CONSUMER.INFO.{stream}.application"),
        json!({}),
    )
    .await;
    api(&admin, &format!("STREAM.DELETE.{stream}"), json!({})).await;
    tested.unwrap();
    assert_eq!(before["delivered"], after["delivered"]);
    assert_eq!(before["ack_floor"], after["ack_floor"]);
    assert_eq!(before["num_pending"], after["num_pending"]);
}
