mod providers;
mod schema;

use onetui_core::config;
use onetui_core::provider::{Executor, RequestContext, ShutdownContext, validate_catalog};
use providers::BUILTINS;

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    version,
    about,
    long_about = "OneTUI database browser. Browse PostgreSQL rows and metadata, Qdrant collections and points with on-demand payloads/vectors, or Kafka topics, partitions and read-committed records. Check connectivity without a terminal, or dump the offline capability catalog. Filter and sort the displayed page locally."
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
        #[arg(long)]
        datasource: Option<String>,
    },
}

fn main() -> ExitCode {
    let args = Args::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(_) => {
            eprintln!("error: cannot initialize async runtime");
            return ExitCode::FAILURE;
        }
    };
    let result = runtime.block_on(run(args));
    // Sessions have finished bounded cleanup. OS certificate reads cannot be aborted;
    // do not let an abandoned blocking read prevent the CLI process from exiting.
    runtime.shutdown_background();
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // Providers redact native diagnostics; avoid Debug, which may expose transport metadata.
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<()> {
    validate_catalog(BUILTINS)?;
    if let Some(Command::Schema { datasource }) = args.command {
        anyhow::ensure!(
            !args.check && args.connection.is_none(),
            "schema cannot be combined with --check or --connection"
        );
        println!("{}", schema::dump(BUILTINS, datasource.as_deref())?);
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
    let config = config::Config::load(&path, BUILTINS)?;
    let deadline = Duration::from_secs(args.timeout);
    if !args.check {
        return onetui_tui::run(
            onetui_tui::App::new(config, args.connection.as_deref()),
            deadline,
            BUILTINS,
        )
        .await;
    }
    let alias = args
        .connection
        .as_deref()
        .expect("clap requires connection");
    let kind = config
        .descriptor(alias)
        .ok_or_else(|| anyhow::anyhow!("unknown connection alias"))?
        .kind;
    let mut executor = config.configure(alias, BUILTINS, &|key| std::env::var(key).ok())?;
    let (cancel, context) = RequestContext::new(deadline);
    let checked = {
        let check = executor.check(context);
        tokio::pin!(check);
        tokio::select! {
            result = &mut check => result,
            signal = tokio::signal::ctrl_c() => {
                let _ = cancel.send(());
                let result = check.await;
                match signal { Ok(()) => result, Err(_) => Err(anyhow::anyhow!("cannot listen for interruption")) }
            }
        }
    };
    let closed = executor
        .shutdown(ShutdownContext::new(Duration::from_secs(1)))
        .await;
    let result = match (checked, closed) {
        (Err(primary), Err(cleanup)) => {
            return Err(anyhow::anyhow!("{primary}; cleanup: {cleanup}"));
        }
        (Err(error), _) | (_, Err(error)) => return Err(error),
        (Ok(result), Ok(())) => result,
    };
    println!("OK {alias} ({kind}): {}", result.summary);
    Ok(())
}
