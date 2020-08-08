use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Write},
    path::PathBuf,
};

mod command;
mod error;

use crate::command::Command;
pub use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;

pub struct KvStore {
    store: HashMap<String, String>,
    reader: BufReader<File>,
    writer: BufWriter<File>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let path = path.join("1.log");

        let mut kvstore = Self {
            store: HashMap::new(),
            writer: BufWriter::new(OpenOptions::new().create(true).append(true).open(&path)?),
            reader: BufReader::new(OpenOptions::new().read(true).open(path)?),
        };

        kvstore.load()?;

        Ok(kvstore)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        self.store.insert(key, value);
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        Ok(self.store.get(&key).cloned())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let command = Command::Rm { key: key.clone() };
        self.store.remove(&key).ok_or(Error::KeyNotFound(key))?;
        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        Ok(())
    }

    fn load(&mut self) -> Result<()> {
        self.store.clear();
        let reader = serde_json::de::Deserializer::from_reader(&mut self.reader);
        for result in reader.into_iter() {
            let command = result?;
            match command {
                Command::Set { key, value } => {
                    self.store.insert(key, value);
                }
                Command::Rm { key } => {
                    self.store.remove(&key);
                }
            }
        }

        Ok(())
    }
}
