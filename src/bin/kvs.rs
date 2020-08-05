use std::process::exit;
use structopt::{clap, StructOpt};

#[derive(StructOpt)]
enum Config {
    #[structopt(about = "Set the value of a string key to a string")]
    Set {
        #[structopt(required = true, help = "A string key")]
        key: String,
        #[structopt(required = true, help = "The string value of the key")]
        value: String,
    },
    #[structopt(about = "Get the string value of a given string key")]
    Get {
        #[structopt(required = true, help = "A string key")]
        key: String,
    },
    #[structopt(about = "Remove a given key")]
    Rm {
        #[structopt(required = true, help = "A string key")]
        key: String,
    },
}

fn main() {
    let config = Config::from_args();

    match config {
        Config::Set { .. } => {
            eprintln!("unimplemented");
            exit(-1);
        }
        Config::Get { .. } => {
            eprintln!("unimplemented");
            exit(-1);
        }
        Config::Rm { .. } => {
            eprintln!("unimplemented");
            exit(-1);
        }
    }
}
