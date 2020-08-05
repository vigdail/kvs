use std::{collections::HashMap, path::PathBuf};

mod error;

use crate::error::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Default)]
pub struct KvStore {
    store: HashMap<String, String>,
}

impl KvStore {
    pub fn open(_path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self::default())
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.store.insert(key, value);

        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.store.get(&key).cloned())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.store.remove(&key) {
            Some(_) => Ok(()),
            None => Err(Error::KeyDoesNotExist(key)),
        }
    }
}
