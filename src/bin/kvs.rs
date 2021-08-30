extern crate clap;
use std::{env, process::exit};

use clap::{App, Arg};

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(App::new("get").arg(Arg::new("KEY").required(true)))
        .subcommand(
            App::new("set")
                .arg(Arg::new("KEY").required(true))
                .arg(Arg::new("VAL").required(true)),
        )
        .subcommand(App::new("rm").arg(Arg::new("KEY").required(true)))
        .arg(Arg::new("version").short('V'))
        .get_matches();

    match matches.subcommand() {
        Some(("get", _sub_m)) => {
            eprintln!("unimplemented");
            exit(1);
        } // get was used
        Some(("set", _sub_m)) => {
            eprintln!("unimplemented");
            exit(1);
        } // set was used
        Some(("rm", _sub_m)) => {
            eprintln!("unimplemented");
            exit(1);
        } // rm was used
        _ => {
            panic!("unknown err");
        }
    }
}
