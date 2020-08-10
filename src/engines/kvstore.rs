use std::{
    collections::{BTreeMap, HashMap},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::{Path, PathBuf},
};

pub use crate::error::Error;
use crate::Result;
use crate::{command::Command, KvsEngine};
use serde_json::Deserializer;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

#[derive(Debug)]
pub struct KvStore {
    compaction: u64,
    path: PathBuf,
    current_gen: u64,
    index: BTreeMap<String, Pos>,
    readers: HashMap<u64, BufReaderWithPos<File>>,
    writer: BufWriterWithPos<File>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let gens = Self::load_gens_list(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let mut compaction = 0;

        for &gen in &gens {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, gen))?)?;
            compaction += Self::load(gen, &mut reader, &mut index)?;
            readers.insert(gen, reader);
        }

        let current_gen = gens.last().unwrap_or(&0) + 1;
        let writer = Self::new_log_file(&path, current_gen, &mut readers)?;

        let kvstore = Self {
            index,
            path,
            compaction,
            current_gen,
            writer,
            readers,
        };

        Ok(kvstore)
    }

    fn load(
        gen: u64,
        reader: &mut BufReaderWithPos<File>,
        index: &mut BTreeMap<String, Pos>,
    ) -> Result<u64> {
        let mut pos = reader.seek(SeekFrom::Start(0))?;
        let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
        let mut compaction = 0;
        while let Some(cmd) = stream.next() {
            let new_pos = stream.byte_offset() as u64;
            match cmd? {
                Command::Set { key, .. } => {
                    if let Some(old_cmd) = index.insert(key, (gen, pos..new_pos).into()) {
                        compaction += old_cmd.len;
                    }
                }
                Command::Rm { key } => {
                    if let Some(old_cmd) = index.remove(&key) {
                        compaction += old_cmd.len;
                    }
                    compaction += new_pos - pos;
                }
            }
            pos = new_pos;
        }

        Ok(compaction)
    }

    fn compact(&mut self) -> Result<()> {
        let compaction_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = Self::new_log_file(&self.path, self.current_gen, &mut self.readers)?;

        let mut compaction_writer =
            Self::new_log_file(&self.path, compaction_gen, &mut self.readers)?;

        let mut new_pos = 0;
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .unwrap_or_else(|| panic!("Cannot find log reader: {}", cmd_pos.gen));
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }

            let mut entry_reader = reader.take(cmd_pos.len);
            let len = std::io::copy(&mut entry_reader, &mut compaction_writer)?;
            *cmd_pos = (compaction_gen, new_pos..new_pos + len).into();
            new_pos += len;
        }
        compaction_writer.flush()?;

        let stale_gens: Vec<_> = self
            .readers
            .keys()
            .filter(|&&gen| gen < compaction_gen)
            .cloned()
            .collect();
        for stale_gen in stale_gens {
            self.readers.remove(&stale_gen);
            fs::remove_file(log_path(&self.path, stale_gen))?;
        }
        self.compaction = 0;

        Ok(())
    }

    fn load_gens_list(path: &PathBuf) -> Result<Vec<u64>> {
        let mut list: Vec<u64> = fs::read_dir(path)?
            .flat_map(|res| -> Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_end_matches(".log"))
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();

        list.sort_unstable();
        Ok(list)
    }

    fn new_log_file(
        path: &Path,
        gen: u64,
        readers: &mut HashMap<u64, BufReaderWithPos<File>>,
    ) -> Result<BufWriterWithPos<File>> {
        let path = log_path(path, gen);

        let writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(&path)?,
        )?;
        let reader = BufReaderWithPos::new(File::open(path)?)?;
        readers.insert(gen, reader);

        Ok(writer)
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let command = Command::Set {
            key: key.clone(),
            value,
        };
        let pos = self.writer.pos;

        serde_json::to_writer(&mut self.writer, &command)?;
        self.writer.flush()?;

        if let Some(old_cmd) = self
            .index
            .insert(key, (self.current_gen, pos..self.writer.pos).into())
        {
            self.compaction += old_cmd.len;
        }

        if self.compaction > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.gen)
                .unwrap_or_else(|| panic!("Can't find log file: {}.log", cmd_pos.gen));

            reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            let cmd_reader = reader.take(cmd_pos.len);

            if let Command::Set { value, .. } = serde_json::from_reader(cmd_reader)? {
                Ok(Some(value))
            } else {
                Err(Error::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let command = Command::Rm { key: key.clone() };

        if self.index.contains_key(&key) {
            serde_json::to_writer(&mut self.writer, &command)?;
            self.writer.flush()?;

            if let Some(old_cmd) = self.index.remove(&key) {
                self.compaction += old_cmd.len;
            }
            Ok(())
        } else {
            Err(Error::KeyNotFound(key))
        }
    }
}

fn log_path(path: &Path, gen: u64) -> PathBuf {
    path.join(format!("{}.log", gen))
}

#[derive(Debug)]
struct Pos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for Pos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        Self {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

#[derive(Debug)]
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

#[derive(Debug)]
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
