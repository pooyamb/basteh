use std::future::Future;
use std::pin::Pin;

use actix_storage::{Format, Storage};
use actix_web::{web, App, HttpServer};

fn recr_fibo(input: usize, storage: Storage) -> Pin<Box<dyn Future<Output = usize> + 'static>> {
    if input == 0 {
        Box::pin(async { 0 })
    } else if input == 1 {
        Box::pin(async { 1 })
    } else {
        Box::pin(async move {
            if let Ok(Some(res)) = storage.get(&input.to_string()).await {
                return res;
            } else {
                let res = recr_fibo(input - 1, storage.clone()).await
                    + recr_fibo(input - 2, storage.clone()).await;
                storage.set(&input.to_string(), &res).await.unwrap();
                res
            }
        })
    }
}

#[actix_web::get("/{input}")]
async fn fibo(input: web::Path<usize>, storage: Storage) -> String {
    if *input > 93 {
        format!("Maximum supported input is 93")
    } else {
        recr_fibo(*input, storage.scope("fibo")).await.to_string()
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // let store = actix_storage_dashmap::DashMapStore::new();
    // OR
    let store = actix_storage_hashmap::HashMapStore::new();
    // OR
    // let store = actix_storage_redis::RedisBackend::connect_default().await.unwrap();
    // OR
    // let store = actix_storage_sled::SledStore::from_db(
    //     actix_storage_sled::SledConfig::default()
    //         .temporary(true)
    //         .open()?,
    // );

    let storage = Storage::build().store(store).format(Format::Json).finish();

    let server = HttpServer::new(move || App::new().app_data(storage.clone()).service(fibo));
    server.bind("localhost:5000")?.run().await
}
