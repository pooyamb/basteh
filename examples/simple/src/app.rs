use std::collections::HashMap;

use actix_web::{web, App, Error, HttpServer};
use basteh::Basteh;
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
    basteh: web::Data<Basteh>,
) -> Result<web::Json<PersonOut>, Error> {
    let new: bool;
    let (name, lesson, point) = path.into_inner();
    let mut previous_point: Option<u16> = None;

    let person = if let Some(Ok(mut person)) = basteh
        .get::<String>(&name)
        .await
        .unwrap()
        .map(|person| serde_json::from_str::<Person>(&person))
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

    // Setting back the data to basteh
    basteh
        .set(&name, &serde_json::to_string(&person).unwrap())
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

    let basteh = Basteh::build().provider(provider).finish();

    // We don't need to wrap basteh inside data, as it's Arced and clonable, but we do it for the sake of
    // easy extraction with web::Data. If you're too worried about double arcing, you can make a new type
    // and implement the extraction logic there.
    let basteh = web::Data::new(basteh);

    let server = HttpServer::new(move || App::new().app_data(basteh.clone()).service(index));
    server.bind("localhost:5000")?.run().await
}
