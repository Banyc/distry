use core::convert::Infallible;
use std::{
    io::{self, SeekFrom},
    path::Path,
};

use primitive::ops::len::Len;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

pub type SeqNum = u64;

#[derive(Debug)]
pub struct WriteAheadLog {
    file: tokio::fs::File,
    offsets: Vec<usize>,
    base_seq: SeqNum,
}
#[bon::bon]
impl WriteAheadLog {
    pub async fn overwrite(path: impl AsRef<Path>, base_seq: SeqNum) -> io::Result<Self> {
        let res = Self::load_or_overwrite::<(), _, _>()
            .path(path)
            .truncate(true)
            .base_seq(base_seq)
            .read_entries(|_, _| panic!())
            .call()
            .await;
        match res {
            Ok(this) => Ok(this),
            Err(e) => match e {
                LoadError::File(error) => Err(error),
                LoadError::User(_) => unreachable!(),
            },
        }
    }
    pub async fn load_no_err(
        path: impl AsRef<Path>,
        mut read_entries: impl FnMut(SeqNum, &[u8]),
    ) -> Result<Self, LoadError<Infallible>> {
        let res = Self::load::<()>(path, |seq, buf| {
            read_entries(seq, buf);
            Ok(())
        })
        .await;
        match res {
            Ok(this) => Ok(this),
            Err(e) => match e {
                LoadError::File(error) => Err(LoadError::File(error)),
                LoadError::User(_) => unreachable!(),
            },
        }
    }
    pub async fn load<E>(
        path: impl AsRef<Path>,
        read_entries: impl FnMut(SeqNum, &[u8]) -> Result<(), E>,
    ) -> Result<Self, LoadError<E>> {
        Self::load_or_overwrite()
            .path(path)
            .truncate(false)
            .base_seq(0)
            .read_entries(read_entries)
            .call()
            .await
    }

    #[builder]
    async fn load_or_overwrite<E>(
        path: impl AsRef<Path>,
        truncate: bool,
        base_seq: SeqNum,
        mut read_entries: impl FnMut(SeqNum, &[u8]) -> Result<(), E>,
    ) -> Result<Self, LoadError<E>> {
        let mut file = tokio::fs::File::options()
            .create(true)
            .truncate(truncate)
            .read(true)
            .write(true)
            .open(path)
            .await
            .map_err(LoadError::File)?;
        let mut offset = 0;
        let mut file_header = [0; 8];
        let Ok(n) = file.read_exact(&mut file_header).await else {
            file.set_len(0).await.map_err(LoadError::File)?;
            file.seek(SeekFrom::Start(0))
                .await
                .map_err(LoadError::File)?;
            let file_header = write_file_header(base_seq);
            file.write_all(&file_header)
                .await
                .map_err(LoadError::File)?;
            file.sync_all().await.map_err(LoadError::File)?;
            offset += file_header.len();
            return Ok(Self {
                file,
                offsets: vec![offset],
                base_seq,
            });
        };
        offset += n;
        let file_header = read_file_header(file_header);

        let mut buf = vec![];
        let mut offsets = vec![];
        let mut seq = file_header.next;
        loop {
            offsets.push(offset);
            let mut frame_header = [0; 8];
            let Ok(n) = file.read_exact(&mut frame_header).await else {
                break;
            };
            offset += n;
            let frame_size = read_frame_header(frame_header);
            let Some(frame_size) = usize::try_from(frame_size).ok() else {
                break;
            };
            let additional = frame_size.saturating_sub(buf.len());
            buf.extend((0..additional).map(|_| 0));
            let Ok(n) = file.read_exact(&mut buf[..frame_size]).await else {
                break;
            };
            offset += n;
            read_entries(seq, &buf).map_err(LoadError::User)?;
            seq += 1;
        }
        let file_len = u64::try_from(*offsets.last().unwrap()).unwrap();
        file.set_len(file_len).await.map_err(LoadError::File)?;
        file.seek(SeekFrom::Start(file_len))
            .await
            .map_err(LoadError::File)?;
        Ok(Self {
            file,
            offsets,
            base_seq: file_header.next,
        })
    }

    /// It is corrupted if error returned.
    ///
    /// Reload to resume.
    pub async fn truncate(&mut self, seq: SeqNum) -> io::Result<()> {
        let index = usize::try_from(seq.checked_sub(self.base_seq).unwrap()).unwrap();
        let offset = self.offsets[index];
        let offset = u64::try_from(offset).unwrap();
        self.file.set_len(offset).await?;
        self.file.seek(io::SeekFrom::Start(offset)).await?;
        self.file.sync_data().await?;
        self.offsets.drain(index + 1..);
        Ok(())
    }

    /// It is corrupted if error returned.
    ///
    /// Reload to resume.
    pub async fn push(&mut self, buf: &[u8]) -> io::Result<SeqNum> {
        let seq = self.next_seq();
        let mut offset = *self.offsets.last().unwrap();
        let frame_header = write_frame_header(u64::try_from(buf.len()).unwrap());
        self.file.write_all(&frame_header[..]).await?;
        offset += frame_header.len();
        self.file.write_all(buf).await?;
        self.file.sync_data().await?;
        offset += buf.len();
        self.offsets.push(offset);
        Ok(seq)
    }

    pub fn next_seq(&self) -> SeqNum {
        self.base_seq + u64::try_from(self.offsets.len() - 1).unwrap()
    }
}
impl Len for WriteAheadLog {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }
}

fn read_file_header(buf: [u8; 8]) -> FileHeader {
    let next = u64::from_be_bytes(buf);
    FileHeader { next }
}
fn write_file_header(seq: SeqNum) -> [u8; 8] {
    seq.to_be_bytes()
}

fn read_frame_header(buf: [u8; 8]) -> u64 {
    u64::from_be_bytes(buf)
}
fn write_frame_header(frame_size: u64) -> [u8; 8] {
    frame_size.to_be_bytes()
}

#[derive(Debug)]
pub enum LoadError<E> {
    File(io::Error),
    User(E),
}

struct FileHeader {
    pub next: SeqNum,
}

#[cfg(test)]
mod tests {
    use primitive::ops::len::LenExt;

    use super::*;

    #[tokio::test]
    async fn test_wal() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let mut wal = WriteAheadLog::load::<()>(temp_file.path(), |_seq, _buf| {
            panic!();
        })
        .await
        .unwrap();
        assert_eq!(wal.next_seq(), 0);
        wal.truncate(0).await.unwrap();
        assert_eq!(wal.len(), 0);

        let mut wal = WriteAheadLog::load_no_err(temp_file.path(), |_seq, _buf| {
            panic!();
        })
        .await
        .unwrap();

        let first = b"hello world";
        let first_seq = wal.push(first).await.unwrap();
        assert_eq!(first_seq, 0);
        let mut wal = WriteAheadLog::load_no_err(temp_file.path(), |seq, buf| {
            assert_eq!(seq, first_seq);
            assert_eq!(first, buf);
        })
        .await
        .unwrap();
        assert_eq!(wal.next_seq(), 1);
        wal.truncate(0).await.unwrap();
        assert_eq!(wal.next_seq(), 0);

        let n = 1 << 4;
        for i in 0..n {
            assert_eq!(wal.push(i.to_string().as_bytes()).await.unwrap(), i);
        }
        let n = n / 2;
        wal.truncate(n).await.unwrap();
        let mut i = 0;
        let wal = WriteAheadLog::load_no_err(temp_file.path(), |seq, buf| {
            assert_eq!(seq, i);
            assert_eq!(buf, i.to_string().as_bytes());
            i += 1;
        })
        .await
        .unwrap();
        assert!(i == n);
        assert_eq!(wal.len(), n.try_into().unwrap());

        let wal = WriteAheadLog::overwrite(temp_file.path(), u64::MAX)
            .await
            .unwrap();
        assert!(wal.is_empty());
        assert_eq!(wal.next_seq(), u64::MAX);
    }
}
