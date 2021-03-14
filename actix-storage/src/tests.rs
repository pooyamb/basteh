use std::{future::Future, pin::Pin, time::Duration};

use crate::{dev::*, *};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////    Store tests     ////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_store<S: 'static + Store>(store: S) {
    let store = Storage::build().store(store).finish();

    assert!(store.set_bytes("store_key", "val").await.is_ok());

    let get_res = store.get_bytes("store_key").await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some("val".as_bytes().into()));

    let contains_res = store.contains_key("store_key").await;
    assert!(contains_res.is_ok());
    assert_eq!(contains_res.unwrap(), true);

    assert!(store.delete("store_key").await.is_ok());

    let get_res = store.get_bytes("store_key").await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), None);

    let contains_res = store.contains_key("store_key").await;
    assert!(contains_res.is_ok());
    assert_eq!(contains_res.unwrap(), false);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////    Expiry tests     ///////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Testing the expiry basics by setting a key and expiry, then waiting for it to expire
pub async fn test_expiry_basics(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expiring_key";
    let value = "val";

    // Expiry for invalid key should return None
    assert!(storage.expiry(key).await.unwrap().is_none());

    // Testing set and get before expiry
    assert!(storage.set_bytes(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());
    assert_eq!(
        storage.get_bytes(key).await.unwrap(),
        Some(value.as_bytes().into())
    );

    // The exact duration depends on the implementation
    let exp = storage.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() > 0);
    assert!(exp.as_secs() <= delay_secs);

    // Adding some error to the delay, for the implementors sake
    actix::clock::delay_for(Duration::from_secs(delay_secs + 1)).await;

    // Check if extended item has been expired
    assert_eq!(storage.get_bytes(key).await.unwrap(), None);
}

/// Testing extending functionality by setting an expiry and extending it later,
/// The key shouldn't be expired before the sum of default expiry and extended time
pub async fn test_expiry_extend(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "extended_expiring_key";
    let value = "val";

    // Testing set and get before expiry
    assert!(storage.set_bytes(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());

    // Testing extending the expiry time
    // The exact duration depends on the implementation
    storage.extend(key, delay).await.unwrap();

    let exp = storage.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() >= delay_secs);
    assert!(exp.as_secs() <= delay_secs * 2);

    // Adding some error to the delay, for the implementors sake
    actix::clock::delay_for(Duration::from_secs(delay_secs + 1)).await;

    // Check if extended item still exist
    assert_eq!(storage.get_bytes(key).await.unwrap(), Some(value.into()));

    // Adding some error to the delay, for the implementors sake
    actix::clock::delay_for(Duration::from_secs(delay_secs + 1)).await;

    // Check if extended item has been expired
    assert_eq!(storage.get_bytes(key).await.unwrap(), None);
}

/// Testing persist, by setting an expiry for a key and making it persistant later
pub async fn test_expiry_persist(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "persistant_key";
    let value = "val";

    assert!(storage.set_bytes(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());
    assert!(storage.persist(key).await.is_ok());

    // Adding some error to the delay, for the implementors sake
    actix::clock::delay_for(Duration::from_secs((delay_secs * 2) + 1)).await;

    // Check if persistent key is still there
    assert_eq!(storage.get_bytes(key).await.unwrap(), Some(value.into()));
}

/// Testing if calling set after expire, clears expiration from the key
pub async fn test_expiry_set_clearing(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "set_after_expire_key";
    let value = "val";

    assert!(storage.set_bytes(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());
    assert!(storage.set_bytes(key, value).await.is_ok());

    actix::clock::delay_for(Duration::from_secs((delay_secs) + 1)).await;

    // Check if calling set twice cleaerd the expire
    assert_eq!(
        storage.get_bytes(key).await.unwrap(),
        Some(value.as_bytes().into())
    );
}

/// Testing if second call to expire overrides the first one
/// The key should expire in this test, as we set a shorter period second time
pub async fn test_expiry_override_shorter(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_override_shorter_key";
    let value = "val";

    // Test if second call to expire overwrites the first expiry
    assert!(storage.set_bytes(key, value).await.is_ok());
    assert!(storage
        .expire(key, Duration::from_secs(delay_secs * 5))
        .await
        .is_ok());
    assert!(storage.expire(key, delay).await.is_ok());

    actix::clock::delay_for(Duration::from_secs((delay_secs) + 1)).await;

    // Check if calling set twice cleaerd the expire
    assert_eq!(storage.get_bytes(key).await.unwrap(), None);
}

/// Testing if second call to expire overrides the first one
/// The key shouldn't expire in this test, as we set a longer period second time
pub async fn test_expiry_override_longer(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_override_longer_key";
    let value = "val";

    // Test if second call to expire overwrites the first expiry
    assert!(storage.set_bytes(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());
    assert!(storage
        .expire(key, Duration::from_secs(delay_secs * 5))
        .await
        .is_ok());

    actix::clock::delay_for(Duration::from_secs((delay_secs) + 1)).await;

    // Check if calling set twice cleaerd the expire
    assert_eq!(
        storage.get_bytes(key).await.unwrap(),
        Some(value.as_bytes().into())
    );
}

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementor has between
// getting a command and executing it
pub fn test_expiry<F, S, E>(cfg: Pin<Box<F>>, delay_secs: u64)
where
    F: 'static + Future<Output = (S, E)>,
    S: 'static + Store,
    E: 'static + Expiry,
{
    let mut system = actix_rt::System::new("expiry_tests");

    let (store, expiry) = system.block_on(async { cfg.await });
    let storage = Storage::build().store(store).expiry(expiry).finish();

    let mut futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = Vec::new();

    futures.push(Box::pin(test_expiry_basics(storage.clone(), delay_secs)));
    futures.push(Box::pin(test_expiry_extend(storage.clone(), delay_secs)));
    futures.push(Box::pin(test_expiry_persist(storage.clone(), delay_secs)));
    futures.push(Box::pin(test_expiry_set_clearing(
        storage.clone(),
        delay_secs,
    )));
    futures.push(Box::pin(test_expiry_override_shorter(
        storage.clone(),
        delay_secs,
    )));
    futures.push(Box::pin(test_expiry_override_longer(
        storage.clone(),
        delay_secs,
    )));

    system.block_on(async {
        futures::future::join_all(futures).await;
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////    Expiry Store tests     ////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementor has between
// getting a command and executing it
pub async fn test_expiry_store<S: 'static + ExpiryStore>(store: S, delay_secs: u64) {
    let store = Storage::build().expiry_store(store).finish();
    let delay = Duration::from_secs(delay_secs);

    // Test set and get expiring
    assert!(store.set_expiring_bytes("key3", "val", delay).await.is_ok());
    let (v, e) = store.get_expiring_bytes("key3").await.unwrap().unwrap();
    assert_eq!(v, "val".as_bytes());
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

    // Adding some error to the delay, for the implementors sake
    actix::clock::delay_for(Duration::from_secs(delay_secs + 1)).await;

    // Check if first item expired as expected
    assert_eq!(store.get_expiring_bytes("key3").await.unwrap(), None);

    // Check if the second call to set overwrites expiry
    assert!(store.get_bytes("key3_2set").await.unwrap().is_some());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////    Format tests     ///////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(any(
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

    #[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
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
    assert_eq!(v.unwrap(), value);
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
pub async fn test_all_formats<S: 'static + Store + Clone>(store: S) {
    let formats = get_formats();
    for format in formats {
        test_format(store.clone(), format).await;
    }
}
