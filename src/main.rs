mod check;
mod config;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::Parser;

#[derive(Parser)]
#[command(
    version,
    about,
    long_about = "BlackPearl database browser. This first slice supports headless connection checks; the TUI is not implemented yet."
)]
struct Args {
    /// Perform a read-only metadata check without opening a terminal UI
    #[arg(long, requires = "connection")]
    check: bool,
    /// Alias from the configuration's connections table
    #[arg(long)]
    connection: Option<String>,
    /// Read only this configuration file (otherwise use the XDG config path)
    #[arg(long)]
    config: Option<PathBuf>,
    /// Overall connection-check deadline, in seconds
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=300))]
    timeout: u64,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Driver/parser error chains can contain credentials or server-supplied text.
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<()> {
    if !args.check {
        bail!("the TUI is not implemented yet; use --check --connection <alias> or --help");
    }
    let alias = args
        .connection
        .as_deref()
        .expect("clap requires connection");
    let path = config::config_path(args.config)?;
    let config = config::Config::load(&path)?;
    let connection = config.resolve(alias, |key| std::env::var(key).ok())?;
    let deadline = Duration::from_secs(args.timeout);
    let result = tokio::select! {
        result = tokio::time::timeout(deadline, check::check(&connection, deadline)) => {
            result.map_err(|_| anyhow::anyhow!("connection check timed out; no connection will be reused"))?
        }
        signal = tokio::signal::ctrl_c() => {
            signal.map_err(|_| anyhow::anyhow!("cannot listen for interruption"))?;
            bail!("connection check cancelled; no connection will be reused");
        }
    }?;
    println!("OK {alias} ({}): {result}", connection.kind());
    Ok(())
}
