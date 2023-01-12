use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};

use actix_storage::Storage;
use actix_web::{web, App, HttpServer};

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
    actix_web::rt::time::sleep(Duration::from_secs(1)).await;

    Object {
        id,
        data: rand::random(),
    }
}

#[actix_web::get("/{obj_id}")]
async fn get_obj(obj_id: web::Path<u64>, storage: Storage) -> web::Json<Response> {
    let resp = if let Ok(Some(resp)) = storage.get(obj_id.to_string()).await {
        serde_json::from_slice(&resp).unwrap()
    } else {
        let object = get_obj_by_id(*obj_id).await;
        let resp = Response {
            object,
            cached_on: SystemTime::now(),
        };

        // Cache for 5 seconds
        storage
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
    let provider = actix_storage_hashmap::HashMapActor::start_default();
    // OR
    // let provider = actix_storage_redis::RedisBackend::connect_default()
    //     .await
    //     .unwrap();
    // OR
    // let provider = actix_storage_sled::actor::SledActor::from_db(
    //     actix_storage_sled::SledConfig::default()
    //         .temporary(true)
    //         .open()?,
    // )
    // .start(4);

    let storage = Storage::build().store(provider).finish();

    let server = HttpServer::new(move || App::new().app_data(storage.clone()).service(get_obj));
    server.bind("localhost:5000")?.run().await
}
