//! Rebuildable Electric-format files; SQLite remains the only authority.
//! I/O runs on the storage actor. Published inodes are only extended, never truncated.
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;

use crate::model::Stream;

const MAX_FILES: usize = 64;

struct Entry {
    file: tempfile::NamedTempFile,
    incarnation: u64,
    published: usize,
}

pub(crate) struct Cache {
    dir: PathBuf,
    ready: bool,
    entries: BTreeMap<String, Entry>,
}

impl Cache {
    pub(crate) fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            ready: false,
            entries: BTreeMap::new(),
        }
    }

    pub(crate) fn invalidate(&mut self) {
        self.entries.clear();
        self.ready = false;
    }

    pub(crate) fn open(&mut self, key: &str, stream: &Stream) -> io::Result<File> {
        if !self.ready {
            match std::fs::remove_dir_all(&self.dir) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            std::fs::create_dir_all(&self.dir)?;
            self.ready = true;
        }
        // Remove while writing: an error drops a partial generation, never publishes it.
        let cached = self.entries.remove(key).filter(|e| {
            e.incarnation == stream.incarnation
                && e.published <= stream.data.len()
                && e.file
                    .as_file()
                    .metadata()
                    .is_ok_and(|m| m.len() == e.published as u64)
        });
        let mut entry = if let Some(entry) = cached {
            entry
        } else {
            Entry {
                file: tempfile::NamedTempFile::new_in(&self.dir)?,
                incarnation: stream.incarnation,
                published: 0,
            }
        };
        while self.entries.len() >= MAX_FILES
            || self.entries.values().map(|e| e.published).sum::<usize>() + stream.data.len()
                > crate::model::MAX_SHARD_BYTES
        {
            self.entries.pop_first();
        }
        entry.file.write_all(&stream.data[entry.published..])?;
        entry.published = stream.data.len();
        // File::try_clone shares the seek cursor; each response needs a fresh open.
        let reader = File::open(entry.file.path())?;
        self.entries.insert(key.to_owned(), entry);
        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn eviction_bounds_entries_and_bytes_without_truncating_readers() {
        let dir = tempfile::tempdir().unwrap();
        let mut cache = Cache::new(dir.path().join("cache"));
        let mut stream = Stream {
            incarnation: 1,
            config: crate::model::StreamConfig {
                content_type: "application/octet-stream".into(),
                json_framing: None,
                expiry: None,
            },
            data: b"x".to_vec(),
            closed: false,
            deleted: false,
            producers: BTreeMap::new(),
            last_seq: None,
            access_ms: 0,
        };
        let mut reader = cache.open("000", &stream).unwrap();
        for index in 1..=MAX_FILES {
            cache.open(&format!("{index:03}"), &stream).unwrap();
        }
        assert_eq!(cache.entries.len(), MAX_FILES);
        assert!(!cache.entries.contains_key("000"));
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"x");
        cache.invalidate();
        stream.data.resize(crate::model::MAX_STREAM_BYTES, b'y');
        for index in 0..3 {
            cache.open(&index.to_string(), &stream).unwrap();
        }
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(std::fs::read_dir(&cache.dir).unwrap().count(), 2);
    }
}
