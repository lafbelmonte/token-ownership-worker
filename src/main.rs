pub mod lib;
use clap::Parser;
use lib::Worker;

/// Token ownership model builder
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// MongoDB Database host
    #[clap(
        short,
        long,
        default_value = "mongodb://root:lostintheabyss@localhost:27017"
    )]
    host: String,

    /// MongoDB Database name
    #[clap(short, long, default_value = "expirement007")]
    name: String,

    /// Ethereum JSON RPC endpoint
    #[clap(
        short,
        long,
        default_value = "https://mainnet.infura.io/v3/58b6195ca6e942b9b3e4d539e352b9e6"
    )]
    rpc: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let worker = Worker::new(args.host, args.name, args.rpc).await.unwrap();

    worker.start().await;
}
