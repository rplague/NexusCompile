use std::fs::{self, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::compile::cache::ResultCache;
use crate::compile_protocol::{Hash32, TaskId};

pub struct TaskBlobStore<'a> {
    cache: &'a ResultCache,
}

impl<'a> TaskBlobStore<'a> {
    pub fn new(cache: &'a ResultCache) -> Self {
        Self { cache }
    }

    pub fn task_dir(&self, task_id: &Hash32) -> PathBuf {
        self.cache.task_dir(task_id)
    }

    pub fn put_chunk(&self, task_id: TaskId, offset: u64, data: &[u8]) -> io::Result<()> {
        let dir = self.task_dir(&task_id.0);
        fs::create_dir_all(&dir)?;
        let path = dir.join("bundle");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(offset == 0)
            .write(true)
            .open(&path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)
    }

    pub fn remove_task_dir(&self, task_id: &Hash32) {
        let _ = fs::remove_dir_all(self.task_dir(task_id));
    }
}
