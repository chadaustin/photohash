use clap::Parser;
use photohash::config::AppConfig;

mod cmd;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Parser)]
#[command(name = "photohash", about = "Index your files", version)]
struct Args {
    #[arg(short = 'c', value_name = "SECTION.KEY=VALUE", global = true)]
    config: Vec<String>,

    #[command(subcommand)]
    command: cmd::MainCommand,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Args::parse_from(wild::args_os());
    let config = AppConfig::load(&opt.config)?;
    opt.command.run(&config).await
}
