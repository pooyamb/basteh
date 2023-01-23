use std::{cmp::Ordering, future::Future, pin::Pin, time::Duration};

use crate::{dev::*, *};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////    Store tests     ////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_store<S>(store: S)
where
    S: 'static + Store,
{
    let storage = Storage::build().store(store).no_expiry().finish();

    let key = "store_key";
    let value = "val";

    assert!(storage.set(key, value).await.is_ok());

    let get_res = storage.get(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(value.as_bytes().into()));

    let contains_res = storage.contains_key(key).await;
    assert!(contains_res.is_ok());
    assert!(contains_res.unwrap());

    assert!(storage.delete(key).await.is_ok());

    let get_res = storage.get(key).await;
    assert!(get_res.is_ok());
    assert!(get_res.unwrap().is_none());

    let contains_res = storage.contains_key(key).await;
    assert!(contains_res.is_ok());
    assert!(!contains_res.unwrap());
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
    assert!(storage.set(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());
    assert_eq!(
        storage.get(key).await.unwrap(),
        Some(value.as_bytes().into())
    );

    // The exact duration depends on the implementation
    let exp = storage.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() > 0);
    assert!(exp.as_secs() <= delay_secs);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if extended item has been expired
    assert_eq!(storage.get(key).await.unwrap(), None);
}

/// Testing extending functionality by setting an expiry and extending it later,
/// The key shouldn't be expired before the sum of default expiry and extended time
pub async fn test_expiry_extend(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "extended_expiring_key";
    let value = "val";

    // Testing set and get before expiry
    assert!(storage.set(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());

    // Testing extending the expiry time
    // The exact duration depends on the implementation
    storage.extend(key, delay).await.unwrap();

    let exp = storage.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() >= delay_secs);
    assert!(exp.as_secs() <= delay_secs * 2);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if extended item still exist
    assert_eq!(
        storage.get(key).await.unwrap(),
        Some(value.as_bytes().into())
    );

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if extended item has been expired
    assert_eq!(storage.get(key).await.unwrap(), None);
}

/// Testing persist, by setting an expiry for a key and making it persistant later
pub async fn test_expiry_persist(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "persistant_key";
    let value = "val";

    assert!(storage.set(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());
    assert!(storage.persist(key).await.is_ok());

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if persistent key is still there
    assert_eq!(
        storage.get(key).await.unwrap(),
        Some(value.as_bytes().into())
    );
}

/// Testing if calling set after expire, clears expiration from the key
pub async fn test_expiry_set_clearing(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "set_after_expire_key";
    let value = "val";

    assert!(storage.set(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());
    assert!(storage.set(key, value).await.is_ok());

    tokio::time::sleep(Duration::from_secs((delay_secs) + 1)).await;

    // Check if calling set twice cleaerd the expire
    assert_eq!(
        storage.get(key).await.unwrap(),
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
    assert!(storage.set(key, value).await.is_ok());
    assert!(storage.expire(key, delay * 5).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());

    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if calling set twice cleaerd the expire
    assert_eq!(storage.get(key).await.unwrap(), None);
}

/// Testing if second call to expire overrides the first one
/// The key shouldn't expire in this test, as we set a longer period second time
pub async fn test_expiry_override_longer(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_override_longer_key";
    let value = "val";

    // Test if second call to expire overwrites the first expiry
    assert!(storage.set(key, value).await.is_ok());
    assert!(storage.expire(key, delay).await.is_ok());
    assert!(storage.expire(key, delay * 5).await.is_ok());

    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if calling set twice cleaerd the expire
    assert_eq!(
        storage.get(key).await.unwrap(),
        Some(value.as_bytes().into())
    );
}

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementer has between
// getting a command and executing it
pub async fn test_expiry<S, E>(store: S, expiry: E, delay_secs: u64)
where
    S: 'static + Store,
    E: 'static + Expiry,
{
    let storage = Storage::build().store(store).expiry(expiry).finish();

    let futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![
        Box::pin(test_expiry_basics(storage.clone(), delay_secs)),
        Box::pin(test_expiry_extend(storage.clone(), delay_secs)),
        Box::pin(test_expiry_persist(storage.clone(), delay_secs)),
        Box::pin(test_expiry_set_clearing(storage.clone(), delay_secs)),
        Box::pin(test_expiry_override_shorter(storage.clone(), delay_secs)),
        Box::pin(test_expiry_override_longer(storage, delay_secs)),
    ];

    futures::future::join_all(futures).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////    Expiry Store tests     ////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_expiry_store_basics(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expiry_store_key";
    let value = "value";

    // Test set and get expiring
    assert!(storage.set_expiring(key, value, delay).await.is_ok());

    let (v, e) = storage.get_expiring(key).await.unwrap().unwrap();
    assert_eq!(v.as_ref(), value.as_bytes());
    assert!(e.unwrap().as_secs() > 0);
    assert!(e.unwrap().as_secs() <= delay_secs);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if first item expired as expected
    assert_eq!(storage.get_expiring(key).await.unwrap(), None);
}

/// Testing if second call to expire overrides the first one
/// The key should expire in this test, as we set a shorter period second time
pub async fn test_expiry_store_override_shorter(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_store_override_shorter_key";
    let value = "value";

    // Test if second call to set expiring, overwrites expiry for key
    assert!(storage.set_expiring(key, value, delay).await.is_ok());
    assert!(storage.set_expiring(key, value, delay * 2).await.is_ok());
    let exp = storage.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() > delay_secs);
    assert!(exp.as_secs() <= delay_secs * 2);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if the second call to set overwrites expiry
    assert_eq!(
        storage.get(key).await.unwrap(),
        Some(value.as_bytes().into())
    );
}

/// Testing if second call to expire overrides the first one
/// The key shouldn't expire in this test, as we set a longer period second time
pub async fn test_expiry_store_override_longer(storage: Storage, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_store_override_longer_key";
    let value = "value";

    // Test if second call to set expiring, overwrites expiry for key
    assert!(storage.set_expiring(key, value, delay * 2).await.is_ok());
    assert!(storage.set_expiring(key, value, delay).await.is_ok());
    let exp = storage.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() > 0);
    assert!(exp.as_secs() <= delay_secs);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if the second call to set overwrites expiry
    assert_eq!(storage.get(key).await.unwrap(), None);
}

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementer has between
// getting a command and executing it
pub async fn test_expiry_store<S>(store: S, delay_secs: u64)
where
    S: 'static + ExpiryStore,
{
    let storage = Storage::build().store(store).finish();

    let futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![
        Box::pin(test_expiry_store_basics(storage.clone(), delay_secs)),
        Box::pin(test_expiry_store_override_shorter(
            storage.clone(),
            delay_secs,
        )),
        Box::pin(test_expiry_store_override_longer(storage, delay_secs)),
    ];

    futures::future::join_all(futures).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////    Numbers and Mutation tests     ////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_store_numbers<S>(store: S)
where
    S: 'static + Store,
{
    let storage = Storage::build().store(store).no_expiry().finish();

    let key = "number_key";
    let value = 1337;

    assert!(storage.set_number(key, value).await.is_ok());

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(value));
}

pub async fn test_mutate_numbers<S>(store: S)
where
    S: 'static + Store,
{
    let storage = Storage::build().store(store).no_expiry().finish();

    let key = "mutate_number_key";
    let value = 1500;

    assert!(storage.set_number(key, value).await.is_ok());

    // Increase by 100
    storage.mutate(key, |m| m.incr(100)).await.ok();

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(1600));

    // Decrease by 200
    storage.mutate(key, |m| m.decr(200)).await.ok();

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(1400));

    // Mutiply by 2
    storage.mutate(key, |m| m.mul(2)).await.ok();

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(2800));

    // Divide by 4
    storage.mutate(key, |m| m.div(4)).await.ok();

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(700));

    // Set to 100
    storage.mutate(key, |m| m.set(100)).await.ok();

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(100));

    // Conditional if
    storage
        .mutate(key, |m| m.if_(Ordering::Equal, 100, |m| m.set(200)))
        .await
        .ok();

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(200));

    // Conditional if else
    storage
        .mutate(key, |m| {
            m.if_else(Ordering::Greater, 200, |m| m.decr(100), |m| m.decr(50))
        })
        .await
        .ok();

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(150));

    // Multi level conditionals
    let mutation = |m: Mutation| {
        m.if_(Ordering::Greater, 100, |m| {
            m.if_(Ordering::Less, 200, |m| {
                m.if_else(Ordering::Greater, 150, |m| m.set(125), |m| m.set(175))
            })
        })
    };
    storage.mutate(key, mutation).await.ok();

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(175));

    // Multi level conditionals
    storage.mutate(key, mutation).await.ok();

    let get_res = storage.get_number(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(125));
}
