use anyhow::Result;

use crate::settings::{command::Command, option::Options};

use super::ParsePayload;

pub fn print_help() {
    println!("Usage:      cassandra-stress <command> [options]");
    println!("Help usage: cassandra-stress help <command|option>");
    println!();
    Command::print_generic_help();
    println!();
    Options::print_generic_help();
}

fn print_command_help(command_str: &str) -> Result<()> {
    match Command::parse(command_str) {
        Ok(cmd) => {
            cmd.print_help();
            Ok(())
        }
        Err(_) => Err(anyhow::anyhow!(
            "Invalid command or option provided to command help"
        )),
    }
}

pub fn parse_help_command(payload: &mut ParsePayload) -> Result<()> {
    let params = payload.remove("help").unwrap_or_default();

    let (help_param, remaining_help_params) = params.split_first().unzip();
    let (option_payload, mut remaining_payload) = {
        let mut iter = payload.iter();
        let next = iter.next();
        (next, iter)
    };

    anyhow::ensure!(
        remaining_help_params.is_none_or(|remaining| remaining.is_empty())
            && remaining_payload.next().is_none(),
        "Invalid command/option provided to help"
    );

    match (help_param, option_payload) {
        (Some(_), Some(_)) => anyhow::bail!("Invalid command/option provided to help"),
        (Some(command), None) => print_command_help(command),
        (None, Some((option, _option_params))) => Options::print_help(option),
        (None, None) => {
            print_help();
            Ok(())
        }
    }
}
