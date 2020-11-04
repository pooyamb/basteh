use std::time::Duration;

use crate::{dev::*, *};

pub async fn test_store<S: 'static + Store>(store: S) {
    let store = Storage::build().store(store).finish();
    assert!(store.set_bytes("key1", "val").await.is_ok());
    assert!(store.get_bytes("key1").await.unwrap() == Some("val".as_bytes().into()));
    assert!(store.contains_key("key1").await.unwrap());
    assert!(store.delete("key1").await.is_ok());
    assert!(store.get_bytes("key1").await.unwrap() == None);
    assert!(!store.contains_key("key1").await.unwrap());
}

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementor has between
// getting a command and executing it
pub async fn test_expiry<S: 'static + Store, E: 'static + Expiry>(
    store: S,
    expiry: E,
    delay_secs: u64,
) {
    let store = Storage::build().store(store).expiry(expiry).finish();
    let delay = Duration::from_secs(delay_secs);
    let delayx2 = Duration::from_secs(delay_secs * 2);

    // Expiry for invalid key should return None
    assert!(store.expiry("key2").await.unwrap().is_none());

    // Testing set and get before expiry
    assert!(store.set_bytes("key2", "val").await.is_ok());
    assert!(store.expire("key2", delay).await.is_ok());
    assert!(store.get_bytes("key2").await.unwrap() == Some("val".as_bytes().into()));

    // The exact number of seconds returned depends on the implementation
    let exp = store.expiry("key2").await.unwrap().unwrap();
    assert!(exp.as_secs() > 0);
    assert!(exp.as_secs() <= delay_secs);

    // Testing extending the expiry time
    // The exact number of seconds returned depends on the implementation
    store.extend("key2", delay).await.unwrap();
    let exp = store.expiry("key2").await.unwrap().unwrap();
    assert!(exp.as_secs() >= delay_secs);
    assert!(exp.as_secs() <= delay_secs * 2);

    // Test persist
    assert!(store.set_bytes("key_persist", "val").await.is_ok());
    assert!(store.expire("key_persist", delayx2).await.is_ok());
    assert!(store.persist("key_persist").await.is_ok());

    // Test if second call to expire overwrites the first expiry
    assert!(store.set_bytes("key_2*expire", "val").await.is_ok());
    assert!(store.expire("key_2*expire", delay).await.is_ok());
    assert!(store
        .expire("key_2*expire", Duration::from_secs(delay_secs * 5))
        .await
        .is_ok());

    // Test if second call to expire doesn't break the expiry
    assert!(store
        .set_bytes("key_2*expire_expiring", "val")
        .await
        .is_ok());
    assert!(store.expire("key_2*expire_expiring", delay).await.is_ok());
    assert!(store.expire("key_2*expire_expiring", delay).await.is_ok());

    // Test if second call to set, removes expiry from key
    assert!(store.set_bytes("key_2*set", "val").await.is_ok());
    assert!(store.expire("key_2*set", delay).await.is_ok());
    assert!(store.set_bytes("key_2*set", "val").await.is_ok());
    assert!(store
        .expire("key_2*set", Duration::from_secs(20))
        .await
        .is_ok());

    actix::clock::delay_for(delayx2).await;

    // Check if extended item has been expired
    assert!(store.get_bytes("key2").await.unwrap() == None);

    // Check if persistent key is still there
    assert!(store.get_bytes("key_persist").await.unwrap() == Some("val".as_bytes().into()));

    // Check if calling expire twice did the overwrite
    assert!(store.get_bytes("key_2*expire").await.unwrap() == Some("val".as_bytes().into()));

    // Check if overwriten expiry still works
    assert!(store.get_bytes("key_2*expire_2sec").await.unwrap() == None);

    // Check if calling set twice cleaerd the expire
    assert!(store.get_bytes("key_2*set").await.unwrap() == Some("val".as_bytes().into()));
}

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementor has between
// getting a command and executing it
pub async fn test_expiry_store<S: 'static + ExpiryStore>(store: S, delay_secs: u64) {
    let store = Storage::build().expiry_store(store).finish();
    let delay = Duration::from_secs(delay_secs);

    // Test set and get expiring
    assert!(store.set_expiring_bytes("key3", "val", delay).await.is_ok());
    let (v, e) = store.get_expiring_bytes("key3").await.unwrap().unwrap();
    assert!(v == "val".as_bytes());
    assert!(e.unwrap().as_secs() > 0);
    assert!(e.unwrap().as_secs() <= delay_secs);

    // Test if second call to set expiring, overwrites expiry for key
    assert!(store
        .set_expiring_bytes("key3_2set", "val", delay)
        .await
        .is_ok());
    assert!(store
        .set_expiring_bytes("key3_2set", "val", Duration::from_secs(delay_secs * 2))
        .await
        .is_ok());
    let exp = store.expiry("key3_2set").await.unwrap().unwrap();
    assert!(exp.as_secs() > delay_secs);
    assert!(exp.as_secs() <= delay_secs * 2);

    actix::clock::delay_for(delay).await;

    // Check if first item expired as expected
    assert!(store.get_expiring_bytes("key3").await.unwrap() == None);

    // Check if the second call to set overwrites expiry
    assert!(store.get_bytes("key3_2set").await.unwrap().is_some());
}

#[cfg(all(
    feature = "serde-json",
    feature = "serde-cbor",
    feature = "serde-ron",
    feature = "serde-yaml",
    feature = "serde-bincode",
    feature = "serde-xml"
))]
fn get_formats() -> Vec<Format> {
    vec![
        Format::Json,
        Format::Cbor,
        Format::Ron,
        Format::Yaml,
        Format::Bincode,
        Format::Xml,
    ]
}

#[cfg(all(
    feature = "serde-json",
    feature = "serde-cbor",
    feature = "serde-ron",
    feature = "serde-yaml",
    feature = "serde-bincode",
    feature = "serde-xml"
))]
pub async fn test_format<S: 'static + Store>(store: S, format: Format) {
    let storage = Storage::build()
        .store(store)
        .format(format.clone())
        .finish();
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Eq, PartialEq)]
    struct Human {
        name: String,
        height: u32,
        says_hello: bool,
    }

    fn get_mamad() -> Human {
        Human {
            name: "Mamad".to_string(),
            height: 160,
            says_hello: false,
        }
    }

    let value = get_mamad();
    let key = format!("key_{:?}", format);
    storage.set(key.clone(), &value).await.unwrap();
    let v: Option<Human> = storage.get(key).await.unwrap();
    assert!(v.is_some());
    assert!(v.unwrap() == value);
}

#[cfg(all(
    feature = "serde-json",
    feature = "serde-cbor",
    feature = "serde-ron",
    feature = "serde-yaml",
    feature = "serde-bincode",
    feature = "serde-xml"
))]
pub async fn test_all_formats<S: 'static + Store + Clone>(store: S) {
    let formats = get_formats();
    for format in formats {
        test_format(store.clone(), format).await;
    }
}
