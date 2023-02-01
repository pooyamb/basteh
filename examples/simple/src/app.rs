use std::collections::HashMap;

use actix_web::{web, App, Error, HttpServer};
use basteh::Storage;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Person {
    name: String,
    points: HashMap<String, u16>,
}

#[derive(Serialize)]
struct PersonOut {
    new: bool,
    previous_point: Option<u16>,
    name: String,
    points: HashMap<String, u16>,
}

/// We get a name, a lesson and a point for the combination. If the name was not defined, we set new to true,
/// if the lesson for the name already had a point, we return that point with output, and we return all the data,
/// for that name at the end.
#[actix_web::get("/{name}/{lesson}/{point}")]
async fn index(
    path: web::Path<(String, String, u16)>,
    storage: web::Data<Storage>,
) -> Result<web::Json<PersonOut>, Error> {
    let new: bool;
    let (name, lesson, point) = path.into_inner();
    let mut previous_point: Option<u16> = None;

    let person = if let Some(Ok(mut person)) = storage
        .get(&name)
        .await
        .unwrap()
        .map(|person| serde_json::from_slice::<Person>(&person))
    {
        new = false;
        if let Some(point) = person.points.insert(lesson, point) {
            previous_point = Some(point);
        }

        person
    } else {
        new = true;
        let mut person = Person {
            name: name.clone(),
            points: HashMap::new(),
        };
        person.points.insert(lesson, point);

        person
    };

    // Setting back the data to storage
    storage
        .set(&name, &serde_json::to_vec(&person).unwrap())
        .await
        .unwrap();

    let out = PersonOut {
        name: person.name,
        points: person.points,
        new,
        previous_point,
    };

    Ok(web::Json(out))
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

    let storage = Storage::build().store(provider).no_expiry().finish();
    let storage = web::Data::new(storage);

    let server = HttpServer::new(move || App::new().app_data(storage.clone()).service(index));
    server.bind("localhost:5000")?.run().await
}
