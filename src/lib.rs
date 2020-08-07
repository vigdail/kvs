use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
};

mod command;
mod error;

use crate::command::Command;
pub use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;

pub struct KvStore {
    store: HashMap<String, String>,
    file: File,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path.join("1.txt"))?;

        let kvstore = Self {
            store: HashMap::new(),
            file,
        };
        Ok(kvstore)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        self.store.insert(key, value);
        self.file
            .write_all(&serde_json::to_string(&command)?.as_bytes())?;

        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.store.get(&key).cloned())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let command = Command::Rm { key: key.clone() };
        self.store.remove(&key).ok_or(Error::KeyNotFound(key))?;
        self.file
            .write_all(&serde_json::to_string(&command)?.as_bytes())?;

        Ok(())
    }
}
