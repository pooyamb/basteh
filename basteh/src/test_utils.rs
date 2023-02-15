use std::{cmp::Ordering, collections::HashSet, time::Duration};

use crate::{dev::*, *};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////    Basteh tests     ////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_store<P>(store: P)
where
    P: 'static + Provider,
{
    let store = Basteh::build().provider(store).finish();

    tokio::join!(
        test_store_methods(store.clone()),
        test_store_bytes(store.clone()),
        test_store_numbers(store.clone()),
        test_store_keys(store.clone())
    );
}

pub async fn test_store_methods(store: Basteh) {
    let key = "store_key";
    let value = "val";

    assert!(store.set(key, value).await.is_ok());

    let get_res = store.get::<String>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(value.to_owned()));

    let contains_res = store.contains_key(key).await;
    assert!(contains_res.is_ok());
    assert!(contains_res.unwrap());

    assert!(store.delete(key).await.is_ok());

    let get_res = store.get::<String>(key).await;
    assert!(get_res.is_ok());
    assert!(get_res.unwrap().is_none());

    let contains_res = store.contains_key(key).await;
    assert!(contains_res.is_ok());
    assert!(!contains_res.unwrap());
}

pub async fn test_store_numbers(store: Basteh) {
    let key = "number_key";
    let value = 1337;

    assert!(store.set(key, value).await.is_ok());

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(value));
}

pub async fn test_store_bytes(store: Basteh) {
    let key = "bytes_key";
    let value = b"Some Value";

    assert!(store.set(key, value.as_ref()).await.is_ok());

    let get_res = store.get::<Vec<u8>>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(value.to_vec()));
}

pub async fn test_store_keys(store: Basteh) {
    let store = store.scope("TEST_SCOPE");
    let value = "val";

    let mut keys = HashSet::new();
    keys.insert(String::from("key1"));
    keys.insert(String::from("key2"));
    keys.insert(String::from("key3"));
    keys.insert(String::from("key4"));
    keys.insert(String::from("key5"));
    keys.insert(String::from("key6"));

    for key in keys.iter() {
        assert!(store.set(&key, value).await.is_ok());
    }
    let retrieved_keys = store
        .keys()
        .await
        .unwrap()
        .map(|v| String::from_utf8(v.to_vec()).unwrap())
        .collect::<HashSet<_>>();

    assert_eq!(retrieved_keys, keys);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////    Expiration tests     /////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Testing the expiry basics by setting a key and expiry, then waiting for it to expire
pub async fn test_expiry_basics(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expiring_key";
    let value = "val";

    // Provider for invalid key should return None
    assert!(store.expiry(key).await.unwrap().is_none());

    // Testing set and get before expiry
    assert!(store.set(key, value).await.is_ok());
    assert!(store.expire(key, delay).await.is_ok());
    assert_eq!(
        store.get::<String>(key).await.unwrap(),
        Some(value.to_owned())
    );

    // The exact duration depends on the implementation
    let exp = store.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() > 0);
    assert!(exp.as_secs() <= delay_secs);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if extended item has been expired
    assert_eq!(store.get::<String>(key).await.unwrap(), None);
}

/// Testing extending functionality by setting an expiry and extending it later,
/// The key shouldn't be expired before the sum of default expiry and extended time
pub async fn test_expiry_extend(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "extended_expiring_key";
    let value = "val";

    // Testing set and get before expiry
    assert!(store.set(key, value).await.is_ok());
    assert!(store.expire(key, delay).await.is_ok());

    // Testing extending the expiry time
    // The exact duration depends on the implementation
    store.extend(key, delay).await.unwrap();

    let exp = store.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() >= delay_secs);
    assert!(exp.as_secs() <= delay_secs * 2);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if extended item still exist
    assert_eq!(
        store.get::<String>(key).await.unwrap(),
        Some(value.to_owned())
    );

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if extended item has been expired
    assert_eq!(store.get::<String>(key).await.unwrap(), None);
}

/// Testing persist, by setting an expiry for a key and making it persistant later
pub async fn test_expiry_persist(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "persistant_key";
    let value = "val";

    assert!(store.set(key, value).await.is_ok());
    assert!(store.expire(key, delay).await.is_ok());
    assert!(store.persist(key).await.is_ok());

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if persistent key is still there
    assert_eq!(
        store.get::<String>(key).await.unwrap(),
        Some(value.to_owned())
    );
}

/// Testing persist, by setting an expiry for a key and making it persistant later
pub async fn test_mutate_sould_not_change_expiry(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "mutated_key";
    let value = 1200;

    assert!(store.set(key, value).await.is_ok());
    assert!(store.expire(key, delay).await.is_ok());

    assert!(store.mutate(key, |m| m.incr(1300)).await.is_ok());

    assert_eq!(store.get::<i64>(key).await.unwrap(), Some(2500));

    // Check if persistent expiry have changed
    assert!(store.expiry(key).await.unwrap().is_some());
}

/// Testing if calling set after expire, clears expiration from the key
pub async fn test_expiry_set_clearing(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "set_after_expire_key";
    let value = "val";

    assert!(store.set(key, value).await.is_ok());
    assert!(store.expire(key, delay).await.is_ok());
    assert!(store.set(key, value).await.is_ok());

    tokio::time::sleep(Duration::from_secs((delay_secs) + 1)).await;

    // Check if calling set twice cleaerd the expire
    assert_eq!(
        store.get::<String>(key).await.unwrap(),
        Some(value.to_owned())
    );
}

/// Testing if second call to expire overrides the first one
/// The key should expire in this test, as we set a shorter period second time
pub async fn test_expiry_override_shorter(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_override_shorter_key";
    let value = "val";

    // Test if second call to expire overwrites the first expiry
    assert!(store.set(key, value).await.is_ok());
    assert!(store.expire(key, delay * 5).await.is_ok());
    assert!(store.expire(key, delay).await.is_ok());

    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if calling set twice cleaerd the expire
    assert_eq!(store.get::<String>(key).await.unwrap(), None);
}

/// Testing if second call to expire overrides the first one
/// The key shouldn't expire in this test, as we set a longer period second time
pub async fn test_expiry_override_longer(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_override_longer_key";
    let value = "val";

    // Test if second call to expire overwrites the first expiry
    assert!(store.set(key, value).await.is_ok());
    assert!(store.expire(key, delay).await.is_ok());
    assert!(store.expire(key, delay * 5).await.is_ok());

    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if calling set twice cleaerd the expire
    assert_eq!(
        store.get::<String>(key).await.unwrap(),
        Some(value.to_owned())
    );
}

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementer has between
// getting a command and executing it
pub async fn test_expiry<P>(provider: P, delay_secs: u64)
where
    P: 'static + Provider,
{
    let store = Basteh::build().provider(provider).finish();

    tokio::join!(
        test_expiry_basics(store.clone(), delay_secs),
        test_mutate_sould_not_change_expiry(store.clone(), delay_secs,),
        test_expiry_extend(store.clone(), delay_secs),
        test_expiry_persist(store.clone(), delay_secs),
        test_expiry_set_clearing(store.clone(), delay_secs),
        test_expiry_override_shorter(store.clone(), delay_secs),
        test_expiry_override_longer(store, delay_secs)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////    Basteh-Expiration tests     //////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_expiry_store_basics(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expiry_store_key";
    let value = "value";

    // Test set and get expiring
    assert!(store.set_expiring(key, value, delay).await.is_ok());

    let (v, e) = store.get_expiring::<String>(key).await.unwrap().unwrap();
    assert_eq!(&v, &value);
    assert!(e.unwrap().as_secs() > 0);
    assert!(e.unwrap().as_secs() <= delay_secs);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if first item expired as expected
    assert_eq!(store.get_expiring::<String>(key).await.unwrap(), None);
}

/// Testing if second call to expire overrides the first one
/// The key should expire in this test, as we set a shorter period second time
pub async fn test_expiry_store_override_shorter(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_store_override_shorter_key";
    let value = "value";

    // Test if second call to set expiring, overwrites expiry for key
    assert!(store.set_expiring(key, value, delay).await.is_ok());
    assert!(store.set_expiring(key, value, delay * 2).await.is_ok());
    let exp = store.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() > delay_secs);
    assert!(exp.as_secs() <= delay_secs * 2);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if the second call to set overwrites expiry
    assert_eq!(
        store.get::<String>(key).await.unwrap(),
        Some(value.to_owned())
    );
}

/// Testing if second call to expire overrides the first one
/// The key shouldn't expire in this test, as we set a longer period second time
pub async fn test_expiry_store_override_longer(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_store_override_longer_key";
    let value = "value";

    // Test if second call to set expiring, overwrites expiry for key
    assert!(store.set_expiring(key, value, delay * 2).await.is_ok());
    assert!(store.set_expiring(key, value, delay).await.is_ok());
    let exp = store.expiry(key).await.unwrap().unwrap();
    assert!(exp.as_secs() > 0);
    assert!(exp.as_secs() <= delay_secs);

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Check if the second call to set overwrites expiry
    assert_eq!(store.get::<String>(key).await.unwrap(), None);
}

/// Testing if mutation after expiry works as expected
pub async fn test_expiry_store_mutate_after_expiry(store: Basteh, delay_secs: u64) {
    let delay = Duration::from_secs(delay_secs);
    let key = "expire_store_mutate_after_expiry_key";
    let value = 1000;

    // Set a number and set expiry for it
    assert!(store.set(key, value).await.is_ok());
    assert!(store.expire(key, delay).await.is_ok());

    // Adding some error to the delay, for the implementers sake
    tokio::time::sleep(Duration::from_secs(delay_secs + 1)).await;

    // Mutate the number
    store.mutate(key, |m| m.incr(100)).await.unwrap();
    assert_eq!(store.get::<i64>(key).await.unwrap(), Some(100))
}

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementer has between
// getting a command and executing it
pub async fn test_expiry_store<P>(provider: P, delay_secs: u64)
where
    P: 'static + Provider,
{
    let store = Basteh::build().provider(provider).finish();

    tokio::join!(
        test_expiry_store_basics(store.clone(), delay_secs),
        test_expiry_store_override_shorter(store.clone(), delay_secs),
        test_expiry_store_override_longer(store.clone(), delay_secs),
        test_expiry_store_mutate_after_expiry(store, delay_secs),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////    Mutation tests     //////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_mutate_numbers(store: Basteh) {
    let key = "mutate_number_key";

    // Increase by 1600, key doesn't exist so it should be considered 0
    store.mutate(key, |m| m.incr(1600)).await.ok();

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(1600));

    // Decrease by 200
    store.mutate(key, |m| m.decr(200)).await.ok();

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(1400));

    // Mutiply by 2
    store.mutate(key, |m| m.mul(2)).await.ok();

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(2800));

    // Divide by 4
    store.mutate(key, |m| m.div(4)).await.ok();

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(700));

    // Set to 100
    store.mutate(key, |m| m.set(100)).await.ok();

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(100));

    // Conditional if
    store
        .mutate(key, |m| m.if_(Ordering::Equal, 100, |m| m.set(200)))
        .await
        .ok();

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(200));

    // Conditional if else
    store
        .mutate(key, |m| {
            m.if_else(Ordering::Greater, 200, |m| m.decr(100), |m| m.decr(50))
        })
        .await
        .ok();

    let get_res = store.get::<i64>(key).await;
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
    store.mutate(key, mutation).await.ok();

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(175));

    // Multi level conditionals
    store.mutate(key, mutation).await.ok();

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(125));
}

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementer has between
// getting a command and executing it
pub async fn test_mutations<P>(provider: P)
where
    P: 'static + Provider,
{
    let store = Basteh::build().provider(provider).finish();

    tokio::join!(test_mutate_numbers(store.clone()));
}
