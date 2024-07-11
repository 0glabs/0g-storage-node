use clap::{arg, command, Command};

pub fn cli_app<'a>() -> Command<'a> {
    command!()
        .arg(arg!(-c --config <FILE> "Sets a custom config file"))
        .arg(arg!(--"miner-key" [KEY] "Sets miner private key (Default: None)"))
        .arg(
            arg!(--"blockchain-rpc-endpoint" [URL] "Sets blockchain RPC endpoint (Default: http://127.0.0.1:8545)")
        )
        .arg(arg!(--"db-max-num-chunks" [NUM] "Sets the max number of chunks to store in db (Default: None)"))
        .allow_external_subcommands(true)
}
