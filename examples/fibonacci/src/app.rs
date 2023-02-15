use std::future::Future;
use std::pin::Pin;

use actix_web::{web, App, HttpServer};
use basteh::Basteh;

fn recr_fibo(input: i64, basteh: Basteh) -> Pin<Box<dyn Future<Output = i64> + 'static>> {
    if input == 0 {
        Box::pin(async { 0 })
    } else if input == 1 {
        Box::pin(async { 1 })
    } else {
        Box::pin(async move {
            if let Ok(Some(res)) = basteh.get(input.to_le_bytes()).await {
                res
            } else {
                let res = recr_fibo(input - 1, basteh.clone()).await
                    + recr_fibo(input - 2, basteh.clone()).await;
                basteh.set(input.to_le_bytes(), res).await.unwrap();
                res
            }
        })
    }
}

#[actix_web::get("/{input}")]
async fn fibo(input: web::Path<i64>, basteh: web::Data<Basteh>) -> String {
    if *input > 92 {
        format!("Maximum supported input is 93")
    } else if *input < 0 {
        format!("Number should be positive")
    } else {
        recr_fibo(*input, basteh.scope("fibo")).await.to_string()
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let provider = basteh_memory::MemoryBackend::start_default();
    // OR
    // let provider = basteh_redis::RedisBackend::connect_default().await.unwrap();
    // OR
    // let provider = basteh_sled::SledBackend::from_db(
    //     basteh_sled::SledConfig::default()
    //         .temporary(true)
    //         .open()?,
    // ).start(1);

    let basteh = Basteh::build().provider(provider).finish();

    // We don't need to wrap basteh inside data, as it's Arced and clonable, but we do it for the sake of
    // easy extraction with web::Data. If you're too worried about double arcing, you can make a new type
    // and implement the extraction logic there.
    let basteh = web::Data::new(basteh);

    let server = HttpServer::new(move || App::new().app_data(basteh.clone()).service(fibo));
    server.bind("localhost:5000")?.run().await
}
