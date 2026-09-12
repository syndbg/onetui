//! Seed only the disposable Redpanda broker and registry, never a user endpoint.
#[path = "support/broker.rs"]
mod broker;
#[path = "support/redpanda.rs"]
mod fixture;
#[path = "support/protobuf.rs"]
mod protobuf;

fn prepare() -> anyhow::Result<()> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()?;
    let directory = root.join("target/demo-schemas");
    std::fs::create_dir_all(&directory)?;
    fixture::prepare(&directory)?;
    protobuf::prepare(&directory)?;
    let mut config: toml::Value = toml::from_str(include_str!("../../../hack/connections.toml"))?;
    for binding in config["connections"]["local_redpanda"]["decoders"]
        .as_array_mut()
        .unwrap()
    {
        if let Some(catalog) = binding.get_mut("catalog") {
            catalog["directory"] = directory.to_str().unwrap().into();
        }
    }
    std::fs::write(
        root.join("target/demo-onetui.toml"),
        toml::to_string_pretty(&config)?,
    )?;
    println!(
        "Demo config: {}",
        root.join("target/demo-onetui.toml").display()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let (traffic, count) = match args.as_slice() {
        [] => (false, None),
        [arg] if arg == "--prepare" => {
            prepare()?;
            return Ok(());
        }
        [arg] if arg == "--traffic" => (true, None),
        [arg, flag, count] if arg == "--traffic" && flag == "--count" => {
            let count: u32 = count.parse()?;
            anyhow::ensure!(count > 0, "--count must be positive");
            (true, Some(count))
        }
        _ => anyhow::bail!("Usage: seed_redpanda [--prepare | --traffic [--count N]]"),
    };
    prepare()?;
    fixture::seed("demo_avro", 1000).await?;
    protobuf::seed("demo_protobuf", 1000).await?;
    broker::seed("demo_avro_catalog", 1000, fixture::raw).await?;
    broker::seed("demo_protobuf_catalog", 1000, |n| Ok(protobuf::raw(n))).await?;
    if !traffic {
        return Ok(());
    }
    let avro_ids = fixture::schemas("demo_avro")?;
    let proto_ids = protobuf::schemas("demo_protobuf")?;
    let producer = broker::config().create()?;
    let mut ticks = tokio::time::interval(std::time::Duration::from_secs(15));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut n = 0u32;
    loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => { signal?; return Ok(()); }
            _ = ticks.tick() => {}
        }
        for (topic, raw) in [
            ("demo_avro", fixture::message(n, avro_ids)?),
            ("demo_protobuf", protobuf::message(n, proto_ids)),
            ("demo_avro_catalog", fixture::raw(n)?),
            ("demo_protobuf_catalog", protobuf::raw(n)),
        ] {
            let offset = broker::send(&producer, topic, &raw).await?;
            println!("{topic} partition=0 offset={offset} sequence={n}");
        }
        n = n
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Demo sequence exhausted"))?;
        if count == Some(n) {
            return Ok(());
        }
    }
}
