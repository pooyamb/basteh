use std::future::Future;
use std::pin::Pin;

use actix_web::{web, App, HttpServer};
use basteh::Storage;

fn recr_fibo(input: i64, storage: Storage) -> Pin<Box<dyn Future<Output = i64> + 'static>> {
    if input == 0 {
        Box::pin(async { 0 })
    } else if input == 1 {
        Box::pin(async { 1 })
    } else {
        Box::pin(async move {
            if let Ok(Some(res)) = storage.get_number(input.to_le_bytes()).await {
                res
            } else {
                let res = recr_fibo(input - 1, storage.clone()).await
                    + recr_fibo(input - 2, storage.clone()).await;
                storage
                    .set(input.to_le_bytes(), res.to_le_bytes())
                    .await
                    .unwrap();
                res
            }
        })
    }
}

#[actix_web::get("/{input}")]
async fn fibo(input: web::Path<i64>, storage: Storage) -> String {
    if *input > 92 {
        format!("Maximum supported input is 93")
    } else if *input < 0 {
        format!("Number should be positive")
    } else {
        recr_fibo(*input, storage.scope("fibo")).await.to_string()
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let provider = basteh_memory::HashMapBackend::start_default();
    // OR
    // let provider = basteh_redis::RedisBackend::connect_default().await.unwrap();
    // OR
    // let provider = basteh_sled::SledStore::from_db(
    //     basteh_sled::SledConfig::default()
    //         .temporary(true)
    //         .open()?,
    // );

    let storage = Storage::build().store(provider).no_expiry().finish();

    let server = HttpServer::new(move || App::new().app_data(storage.clone()).service(fibo));
    server.bind("localhost:5000")?.run().await
}
