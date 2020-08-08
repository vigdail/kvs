use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::PathBuf,
};

mod command;
mod error;

use crate::command::Command;
pub use crate::error::Error;
use serde_json::Deserializer;

pub type Result<T> = std::result::Result<T, Error>;

pub struct KvStore {
    index: BTreeMap<String, Pos>,
    reader: BufReaderWithPos<File>,
    writer: BufWriterWithPos<File>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let path = path.join("1.log");

        let mut kvstore = Self {
            index: BTreeMap::new(),
            writer: BufWriterWithPos::new(
                OpenOptions::new().create(true).append(true).open(&path)?,
            )?,
            reader: BufReaderWithPos::new(OpenOptions::new().read(true).open(path)?)?,
        };

        kvstore.load()?;

        Ok(kvstore)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let pos = self.writer.pos;

        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        self.index.insert(key, (pos..self.writer.pos).into());

        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(pos) = self.index.get(&key) {
            let reader = &mut self.reader;
            reader.seek(SeekFrom::Start(pos.pos))?;
            let cmd_reader = reader.take(pos.len);

            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(Error::KeyNotFound(key))
            }
        } else {
            Ok(None)
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        let command = Command::Rm { key: key.clone() };

        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        self.index.remove(&key).ok_or(Error::KeyNotFound(key))?;

        Ok(())
    }

    fn load(&mut self) -> Result<()> {
        let mut pos = self.reader.seek(SeekFrom::Start(0))?;
        let mut stream = Deserializer::from_reader(&mut self.reader).into_iter::<Command>();
        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => {
                    self.index.insert(key, (pos..new_pos).into());
                }
                Command::Rm { key } => {
                    self.index.remove(&key);
                }
            }
            pos = new_pos;
        }

        Ok(())
    }
}

#[derive(Debug)]
struct Pos {
    pos: u64,
    len: u64,
}

impl From<Range<u64>> for Pos {
    fn from(range: Range<u64>) -> Self {
        Self {
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufWriterWithPos<T: Write + Seek> {
    inner: BufWriter<T>,
    pos: u64,
}

impl<T: Write + Seek> BufWriterWithPos<T> {
    pub fn new(mut inner: T) -> Result<Self> {
        let pos = inner.seek(SeekFrom::End(0))?;
        Ok(Self {
            inner: BufWriter::new(inner),
            pos,
        })
    }
}

impl<T: Write + Seek> Write for BufWriterWithPos<T> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.pos += len as u64;

        Ok(len)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<T: Write + Seek> Seek for BufWriterWithPos<T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.inner.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufReaderWithPos<T: Read + Seek> {
    inner: BufReader<T>,
    pos: u64,
}

impl<T: Read + Seek> BufReaderWithPos<T> {
    pub fn new(mut inner: T) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            inner: BufReader::new(inner),
            pos,
        })
    }
}

impl<T: Read + Seek> Read for BufReaderWithPos<T> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.inner.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<T: Read + Seek> Seek for BufReaderWithPos<T> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.inner.seek(pos)?;
        Ok(self.pos)
    }
}
