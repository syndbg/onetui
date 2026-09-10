//! Produce only to the fixed disposable NATS fixture, never a user's configured endpoint.
use anyhow::{Result, bail, ensure};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let count = match args.as_slice() {
        [] => None,
        [flag, count] if flag == "--count" => {
            let count: u64 = count.parse()?;
            ensure!(count > 0, "--count must be a positive integer");
            Some(count)
        }
        _ => bail!("Usage: produce_nats [--count POSITIVE_INTEGER]"),
    };
    let client = async_nats::ConnectOptions::new()
        .user_and_password("fixture-admin".into(), "fixture-admin-only".into())
        .max_reconnects(1)
        .connection_timeout(std::time::Duration::from_secs(1))
        .connect("nats://127.0.0.1:14222")
        .await?;
    let js = async_nats::jetstream::new(client.clone());
    js.get_stream("DEMO_LIVE").await?;
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sequence = 0;
    loop {
        tokio::select! {
            biased;
            signal = tokio::signal::ctrl_c() => { signal?; break; }
            _ = interval.tick() => {}
        }
        let payload = serde_json::to_vec(&serde_json::json!({
            "sequence": sequence, "sent_at_ms": SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis(),
            "message": "Synthetic live event", "source": "onetui fixture producer"
        }))?;
        let ack = js.publish("demo.live", payload.into()).await?.await?;
        println!("DEMO_LIVE subject=demo.live sequence={}", ack.sequence);
        sequence += 1;
        if count == Some(sequence) {
            break;
        }
    }
    client.flush().await?;
    Ok(())
}
