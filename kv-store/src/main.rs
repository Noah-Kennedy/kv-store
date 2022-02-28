use std::io;

use actix_web::{web, App, HttpServer};
use bytes::Bytes;
use futures::StreamExt;
use glommio::Placement;
use tokio::sync::{mpsc, oneshot};

use crate::disk_io::run_io_thread;
use crate::routes::{append, list, read};

mod disk_io;
mod routes;

struct State {
    rx_sender: mpsc::Sender<(String, oneshot::Sender<io::Result<Bytes>>)>,
}

const DATA_DIR: &str = "/data/kv-store/";

#[actix_web::main]
async fn main() {
    let (rx_sender, rx_receiver) = mpsc::channel(64);

    let _handle = glommio::LocalExecutorBuilder::new(Placement::Fixed(0))
        .spawn(|| run_io_thread(rx_receiver))
        .unwrap();

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(State {
                rx_sender: rx_sender.clone(),
            }))
            .service(append)
            .service(read)
            .service(list)
    })
    .bind("127.0.0.1:8080")
    .unwrap()
    .run()
    .await
    .unwrap()
}
