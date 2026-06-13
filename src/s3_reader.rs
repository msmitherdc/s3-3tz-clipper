use aws_sdk_s3::Client;
use std::io::{Error, ErrorKind, SeekFrom};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};
use futures::future::BoxFuture;

pub struct S3SeekableReader {
    client: Client,
    bucket: String,
    key: String,
    pos: u64,
    size: u64,
    pending_read: Option<BoxFuture<'static, std::io::Result<Vec<u8>>>>,
}

impl S3SeekableReader {
    pub async fn new(client: Client, bucket: String, key: String) -> Result<Self, aws_sdk_s3::Error> {
        let head = client.head_object().bucket(&bucket).key(&key).send().await?;
        let size = head.content_length().unwrap_or(0) as u64;

        Ok(Self { client, bucket, key, pos: 0, size, pending_read: None })
    }
}

impl AsyncSeek for S3SeekableReader {
    fn start_seek(mut self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        match position {
            SeekFrom::Start(p) => self.pos = p,
            SeekFrom::End(p) => self.pos = (self.size as i64 + p) as u64,
            SeekFrom::Current(p) => self.pos = (self.pos as i64 + p) as u64,
        }
        
        // Invalidate any pending read since we shifted focus
        self.pending_read = None;
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.pos))
    }
}

impl AsyncRead for S3SeekableReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.pos >= self.size { 
            return Poll::Ready(Ok(())); // EOF
        }

        let remaining = buf.remaining() as u64;
        if remaining == 0 {
            return Poll::Ready(Ok(())); // Nothing requested
        }

        if self.pending_read.is_none() {
            let client = self.client.clone();
            let bucket = self.bucket.clone();
            let key = self.key.clone();
            let start = self.pos;
            
            // Underflow & Overflow Safe Bound Calculation
            let end = if start + remaining >= self.size {
                self.size - 1
            } else {
                start + remaining - 1
            };
            
            let range = format!("bytes={}-{}", start, end);

            let fut = Box::pin(async move {
                let resp = client.get_object().bucket(bucket).key(key).range(range).send().await
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                let data = resp.body.collect().await
                    .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
                Ok(data.into_bytes().to_vec())
            });
            self.pending_read = Some(fut);
        }

        if let Some(fut) = self.pending_read.as_mut() {
            match fut.as_mut().poll(cx) {
                Poll::Ready(Ok(data)) => {
                    buf.put_slice(&data);
                    self.pos += data.len() as u64;
                    self.pending_read = None;
                    return Poll::Ready(Ok(()));
                }
                Poll::Ready(Err(e)) => {
                    self.pending_read = None;
                    return Poll::Ready(Err(e));
                }
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Pending
    }
}
