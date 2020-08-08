use std::env::current_dir;
use std::process::exit;
use structopt::StructOpt;

use kvs::Result;

#[derive(StructOpt)]
#[allow(dead_code)]
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

fn main() -> Result<()> {
    let config = Config::from_args();

    match config {
        Config::Set { key, value } => {
            let mut storage = kvs::KvStore::open(current_dir()?)?;
            storage.set(key, value)?;
        }
        Config::Get { key } => {
            let mut storage = kvs::KvStore::open(current_dir()?)?;
            if let Some(value) = storage.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Config::Rm { key } => {
            let mut storage = kvs::KvStore::open(current_dir()?)?;
            match storage.remove(key) {
                Ok(()) => {}
                Err(kvs::Error::KeyNotFound(_)) => {
                    println!("Key not found");
                    exit(1);
                }
                Err(e) => return Err(e),
            }
        }
    }

    Ok(())
}
