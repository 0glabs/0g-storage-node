use clap::{arg, command, Command};

pub fn cli_app<'a>() -> Command<'a> {
    command!()
        .arg(arg!(-c --config <FILE> "Sets a custom config file"))
        .allow_external_subcommands(true)
}
