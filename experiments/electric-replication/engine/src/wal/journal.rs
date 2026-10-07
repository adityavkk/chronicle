//! Consensus-specific resume and indexed reads of the ACTUAL Electric WAL.
//! The native reset/checkpoint routines must never run on this journal.
use super::*;
use crate::wal::codec::{decode_at, Decoded, HEADER_LEN};
use std::io::Read;
use std::os::unix::fs::FileExt;

impl Shard {
    /// Called single-threaded before the committer starts. Payload memory is
    /// bounded by one native segment-sized record, not the retained history.
    /// Fail closed on ANY CRC error, even in the last segment: an active-file
    /// corrupt frame cannot in general be distinguished from lost acked data.
    pub fn resume_journal(
        &self,
        mut visit: impl FnMut(RecordLocation, &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if path.extension().and_then(|x| x.to_str()) == Some("wal") {
                let start = path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .parse::<u64>()
                    .map_err(io::Error::other)?;
                files.push((start, path));
            }
        }
        files.sort_by_key(|(start, _)| *start);
        let mut expected = 1;
        let mut end = 0;
        for (i, (start, path)) in files.iter().enumerate() {
            if *start != expected {
                return Err(io::Error::other("journal segment/LSN gap"));
            }
            let mut file = std::fs::File::open(path)?;
            let size = file.metadata()?.len();
            let mut offset = 0;
            while offset < size {
                let mut header = [0; HEADER_LEN];
                let remaining = (size - offset).min(HEADER_LEN as u64) as usize;
                file.read_exact(&mut header[..remaining])?;
                if header == [0; HEADER_LEN] {
                    if i + 1 != files.len() {
                        return Err(io::Error::other("zero hole before later journal segment"));
                    }
                    // No valid record may hide behind a zero header.
                    let mut tail = [0u8; 64 * 1024];
                    loop {
                        let count = file.read(&mut tail)?;
                        if count == 0 {
                            break;
                        }
                        if tail[..count].iter().any(|b| *b != 0) {
                            return Err(io::Error::other("nonzero suffix behind journal hole"));
                        }
                    }
                    break;
                }
                if remaining != HEADER_LEN {
                    return Err(io::Error::other("truncated journal header"));
                }
                let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
                let total = HEADER_LEN
                    .checked_add(len)
                    .ok_or_else(|| io::Error::other("frame overflow"))?;
                if total as u64 > self.segment_size || offset + total as u64 > size {
                    return Err(io::Error::other("truncated/oversize journal record"));
                }
                let mut frame = vec![0; total];
                frame[..HEADER_LEN].copy_from_slice(&header);
                file.read_exact(&mut frame[HEADER_LEN..])?;
                match decode_at(&frame, 0) {
                    Decoded::Record {
                        lsn,
                        kind: RecordKind::Raft,
                        ..
                    } if lsn == expected => {
                        visit(
                            RecordLocation {
                                lsn,
                                segment: *start,
                                offset,
                                len: total,
                            },
                            &frame[HEADER_LEN..],
                        )?;
                        expected += 1;
                    }
                    _ => return Err(io::Error::other("corrupt or non-consensus journal frame")),
                }
                offset += total as u64;
            }
            end = offset;
        }
        let (start, path) = files
            .last()
            .ok_or_else(|| io::Error::other("missing journal segment"))?;
        // Truncate first so stale bytes cannot reappear; seal makes recovered
        // complete frames stable before setting the durable watermark.
        FileSegment::open_existing(path.clone())?.seal_to(end)?;
        let active = Arc::new(FileSegment::create(path.clone(), self.segment_size)?);
        active.fdatasync()?;
        let mut g = self.inner.lock().unwrap();
        g.active = active;
        g.seg_start_lsn = *start;
        g.write_pos = end;
        g.next_lsn = expected;
        g.written_high = expected - 1;
        self.durable_lsn.store(expected - 1, Ordering::Release);
        Ok(())
    }

    pub fn read_record(&self, location: RecordLocation) -> io::Result<Vec<u8>> {
        let file = std::fs::File::open(seg_path(&self.dir, location.segment))?;
        let mut frame = vec![0; location.len];
        file.read_exact_at(&mut frame, location.offset)?;
        match decode_at(&frame, 0) {
            Decoded::Record {
                lsn,
                kind: RecordKind::Raft,
                ..
            } if lsn == location.lsn => Ok(frame.split_off(HEADER_LEN)),
            _ => Err(io::Error::other("indexed WAL read failed CRC/identity")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_zero_padding_is_not_a_torn_record_but_nonzero_is_fatal() {
        for padding in 1..HEADER_LEN {
            let dir = tempfile::tempdir().unwrap();
            let size = (HEADER_LEN + 17 + padding) as u64;
            let shard = Shard::open_with_segment_size(dir.path().into(), size).unwrap();
            shard.resume_journal(|_, _| Ok(())).unwrap();
            let data = [19; 17];
            let location = shard
                .reserve_and_stage_indexed(RecordKind::Raft, 0, 0, &data)
                .unwrap();
            shard.commit_once().unwrap();
            drop(shard);
            let shard = Shard::open_with_segment_size(dir.path().into(), size).unwrap();
            let mut seen = 0;
            shard
                .resume_journal(|_, bytes| {
                    assert_eq!(bytes, data);
                    seen += 1;
                    Ok(())
                })
                .unwrap();
            assert_eq!(seen, 1);
            assert_eq!(shard.read_record(location).unwrap(), data);
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(seg_path(dir.path(), 1))
                .unwrap();
            file.write_all_at(&[1], size - 1).unwrap();
            assert!(shard.resume_journal(|_, _| Ok(())).is_err());
        }
    }
}
