use actix_storage::{Format, Storage};
use actix_storage_sled::{SledConfig, SledStore};
use actix_web::{web, App, Error, HttpServer};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Person {
    name: String,
    age: u16,
}

#[actix_web::get("/{name}/{age}")]
async fn index(
    web::Path((name, age)): web::Path<(String, u16)>,
    storage: web::Data<Storage>,
) -> Result<String, Error> {
    if let Some(person) = storage.get::<_, Person>(&name).await? {
        Ok(format!(
            "I already said hello to you {} and I know you're {}",
            person.name, person.age
        ))
    } else {
        let person = Person {
            name: name.clone(),
            age,
        };
        storage.set(&name, &person).await?;
        Ok(format!("Hello {}", name))
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let storage = Storage::build()
        .store(SledStore::from_db(
            SledConfig::default().temporary(true).open()?,
        ))
        .format(Format::Bincode)
        .finish();

    let server = HttpServer::new(move || App::new().data(storage.clone()).service(index));
    server.bind("localhost:5000")?.run().await
}
