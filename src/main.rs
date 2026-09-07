mod schema;

use onetui_core::config::{self, ResolvedConnection};

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Result, bail};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    version,
    about,
    long_about = "OneTUI database browser. Browse PostgreSQL rows and schema/relation/column metadata in the terminal, check PostgreSQL/Qdrant connectivity, or dump the offline capability catalog. Filter and sort the displayed page locally. Qdrant point browsing is not implemented yet."
)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
    /// Perform a read-only metadata check without opening a terminal UI
    #[arg(long, requires = "connection")]
    check: bool,
    /// Alias from the configuration's connections table
    #[arg(long)]
    connection: Option<String>,
    /// Read only this configuration file (otherwise use the XDG config path)
    #[arg(long)]
    config: Option<PathBuf>,
    /// Active check/browsing-request deadline in seconds (not displayed-data expiry)
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..=300))]
    timeout: u64,
}

#[derive(Subcommand)]
enum Command {
    /// Dump implemented capabilities/configuration as JSON without config, secrets, network or TUI
    Schema {
        /// Include only this datasource's capabilities
        #[arg(long, value_parser = ["postgres", "qdrant"])]
        datasource: Option<String>,
    },
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
    if let Some(Command::Schema { datasource }) = args.command {
        anyhow::ensure!(
            !args.check && args.connection.is_none(),
            "schema cannot be combined with --check or --connection"
        );
        println!("{}", schema::dump(datasource.as_deref())?);
        return Ok(());
    }
    if !args.check {
        use std::io::IsTerminal;
        anyhow::ensure!(
            std::io::stdin().is_terminal() && std::io::stdout().is_terminal(),
            "interactive browsing requires a terminal on stdin/stdout; use --check or schema for headless operation"
        );
    }
    let path = config::config_path(args.config)?;
    let config = config::Config::load(&path)?;
    let deadline = Duration::from_secs(args.timeout);
    if !args.check {
        return onetui_tui::run(
            onetui_tui::App::new(config, args.connection.as_deref()),
            deadline,
        )
        .await;
    }
    let alias = args
        .connection
        .as_deref()
        .expect("clap requires connection");
    let connection = config.resolve(alias, |key| std::env::var(key).ok())?;
    let check = async {
        match &connection {
            ResolvedConnection::Postgres { url, ca_file } => {
                onetui_postgres::check(url, ca_file.as_deref(), deadline).await
            }
            ResolvedConnection::Qdrant { url, api_key } => {
                onetui_qdrant::check(url, api_key.as_deref(), deadline).await
            }
        }
    };
    let result = tokio::select! {
        result = tokio::time::timeout(deadline, check) => {
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
