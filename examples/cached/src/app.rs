use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

use actix_web::{web, App, HttpServer};
use basteh::Basteh;

#[derive(Serialize, Deserialize)]
struct Object {
    id: u64,
    data: u64,
}

#[derive(Serialize, Deserialize)]
struct Response {
    object: Object,
    cached_on: SystemTime,
}

async fn get_obj_by_id(id: u64) -> Object {
    // Pretending it's a heavy query by waiting a second
    // actix_web::rt::time::sleep(Duration::from_secs(1)).await;

    Object {
        id,
        data: rand::random(),
    }
}

#[actix_web::get("/{obj_id}")]
async fn get_obj(obj_id: web::Path<u64>, basteh: web::Data<Basteh>) -> web::Json<Response> {
    let resp = if let Ok(Some(resp)) = basteh.get::<Vec<u8>>(obj_id.to_string()).await {
        serde_json::from_slice(&resp).unwrap()
    } else {
        let object = get_obj_by_id(*obj_id).await;
        let resp = Response {
            object,
            cached_on: SystemTime::now(),
        };

        // Cache for 5 seconds
        basteh
            .set_expiring(
                &obj_id.to_string(),
                &serde_json::to_vec(&resp).unwrap(),
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        resp
    };

    web::Json(resp)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let provider = basteh_memory::MemoryBackend::start_default();
    // OR
    // let provider = basteh_redis::RedisBackend::connect_default()
    //     .await
    //     .unwrap();
    // OR
    // let provider = basteh_sled::actor::SledBackend::from_db(
    //     basteh_sled::SledConfig::default()
    //         .temporary(true)
    //         .open()?,
    // )
    // .start(1);

    let basteh = Basteh::build().provider(provider).finish();

    // We don't need to wrap basteh inside data, as it's Arced and clonable, but we do it for the sake of
    // easy extraction with web::Data. If you're too worried about double arcing, you can make a new type
    // and implement the extraction logic there.
    let basteh = web::Data::new(basteh);

    let server = HttpServer::new(move || App::new().app_data(basteh.clone()).service(get_obj));
    server.bind("localhost:5000")?.run().await
}
