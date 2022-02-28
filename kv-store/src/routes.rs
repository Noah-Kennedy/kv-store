use actix_web::web;
use bytes::{Bytes, BytesMut};
use rand::Rng;
use tokio::sync::oneshot;

use crate::{State, StreamExt, DATA_DIR};

const BASE64_CONFIG: base64::Config = base64::Config::new(base64::CharacterSet::UrlSafe, false);

#[actix_web::post("/")]
pub async fn append(mut body: web::Payload) -> actix_web::Result<String> {
    let id: u128 = rand::thread_rng().gen();

    let mut path = DATA_DIR.to_owned();

    base64::encode_config_buf(id.to_be_bytes(), BASE64_CONFIG, &mut path);

    let mut collected = BytesMut::new();

    while let Some(chunk) = body.next().await {
        collected.extend_from_slice(&chunk?);
    }

    tokio::fs::write(&path, &collected).await?;

    Ok(path)
}

#[actix_web::get("/{key}")]
pub(super) async fn read(
    path: web::Path<String>,
    state: web::Data<State>,
) -> actix_web::Result<Bytes> {
    let key = path.into_inner();

    let (tx, rx) = oneshot::channel();

    let _ = state.rx_sender.send((key, tx)).await;

    let data = Bytes::from(rx.await.unwrap()?);

    Ok(data)
}

#[actix_web::get("/")]
pub async fn list() -> actix_web::Result<web::Json<Vec<String>>> {
    let mut files = tokio::fs::read_dir(DATA_DIR).await?;

    let mut list = Vec::new();

    while let Some(entry) = files.next_entry().await? {
        list.push(entry.file_name().into_string().unwrap());
    }

    Ok(web::Json(list))
}
