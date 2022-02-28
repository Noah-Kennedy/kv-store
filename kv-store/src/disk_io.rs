use bytes::Bytes;
use tokio::io;
use tokio::sync::{mpsc, oneshot};

use crate::DATA_DIR;

pub async fn run_io_thread(
    mut receiver: mpsc::Receiver<(String, oneshot::Sender<io::Result<Bytes>>)>,
) {
    while let Some((key, responder)) = receiver.recv().await {
        glommio::spawn_local(async move {
            let res = read_key_file(&key).await;
            let _ = responder.send(res);
        })
        .detach();
    }
}

pub async fn read_key_file(key: &str) -> io::Result<Bytes> {
    let mut path = DATA_DIR.to_owned();

    path.push_str(key);

    let file = glommio::io::DmaFile::open(&path).await?;

    let len = file.file_size().await? as usize;

    let data = file.read_at(0, len).await?.to_vec();

    Ok(Bytes::from(data))
}
