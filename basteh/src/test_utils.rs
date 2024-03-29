use std::{cmp::Ordering, collections::HashSet, time::Duration};

use bytes::Bytes;

use crate::{dev::*, *};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////    Basteh tests     ////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    let removed_value = store.remove::<String>(key).await;
    assert!(removed_value.is_ok());
    assert_eq!(removed_value.unwrap(), Some(value.to_owned()));

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

    assert!(store.set(key, Bytes::from_static(value)).await.is_ok());

    let get_res = store.get::<Bytes>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap().map(|v| v.to_vec()), Some(value.to_vec()));
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

pub async fn test_store_list(store: Basteh) {
    store
        .set(
            "list_key",
            [100_i64, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
        )
        .await
        .unwrap();

    let get_vec = store.get_range::<i64>("list_key", -5, -5).await.unwrap();
    assert_eq!(get_vec, vec![600]);

    let get_vec = store.get_range::<i64>("list_key", 5, -5).await.unwrap();
    assert_eq!(get_vec, vec![600]);

    let get_vec = store.get_range::<i64>("list_key", -5, 5).await.unwrap();
    assert_eq!(get_vec, vec![600]);

    let get_vec = store.get_range::<i64>("list_key", 5, 5).await.unwrap();
    assert_eq!(get_vec, vec![600]);

    let get_vec = store.get_range::<i64>("list_key", 0, -5).await.unwrap();
    assert_eq!(get_vec, vec![100, 200, 300, 400, 500, 600]);

    let get_vec = store.get_range::<i64>("list_key", -5, -1).await.unwrap();
    assert_eq!(get_vec, vec![600, 700, 800, 900, 1000]);

    store.set("list_key", vec!["Hello", "World"]).await.unwrap();

    let get_vec = store.get_range::<String>("list_key", 0, -1).await.unwrap();
    assert_eq!(get_vec, vec!["Hello".to_string(), "World".to_string()]);

    let get_vec = store.get_range::<String>("list_key", -5, -1).await.unwrap();
    assert_eq!(get_vec, vec!["Hello".to_string(), "World".to_string()]);

    let get_vec = store.get_range::<String>("list_key", 1, -1).await.unwrap();
    assert_eq!(get_vec, vec!["World".to_string()]);
}

pub async fn test_store<P>(store: P)
where
    P: 'static + Provider,
{
    let store = Basteh::build().provider(store).finish();

    tokio::join!(
        test_store_methods(store.clone()),
        test_store_bytes(store.clone()),
        test_store_numbers(store.clone()),
        test_store_keys(store.clone()),
        test_store_list(store.clone())
    );
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
    let key = "mutate_key";

    // Increase by 1600, key doesn't exist so it should be considered 0
    let mut_res = store.mutate(key, |m| m.incr(1600)).await;
    assert_eq!(mut_res.unwrap(), 1600);

    let get_res = store.get(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(1600));

    // Decrease by 200
    let mut_res = store.mutate(key, |m| m.decr(200)).await;
    assert_eq!(mut_res.unwrap(), 1400);

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(1400));

    // Mutiply by 2
    let mut_res = store.mutate(key, |m| m.mul(2)).await;
    assert_eq!(mut_res.unwrap(), 2800);

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(2800));

    // Divide by 4
    let mut_res = store.mutate(key, |m| m.div(4)).await;
    assert_eq!(mut_res.unwrap(), 700);

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(700));

    // Set to 100
    let mut_res = store.mutate(key, |m| m.set(100)).await;
    assert_eq!(mut_res.unwrap(), 100);

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(100));

    // Conditional if
    let mut_res = store
        .mutate(key, |m| m.if_(Ordering::Equal, 100, |m| m.set(200)))
        .await;
    assert_eq!(mut_res.unwrap(), 200);

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(200));

    // Conditional if else
    let mut_res = store
        .mutate(key, |m| {
            m.if_else(Ordering::Greater, 200, |m| m.decr(100), |m| m.decr(50))
        })
        .await;
    assert_eq!(mut_res.unwrap(), 150);

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
    let mut_res = store.mutate(key, mutation).await;
    assert_eq!(mut_res.unwrap(), 175);

    let get_res = store.get::<i64>(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(175));

    // Multi level conditionals
    let mut_res = store.mutate(key, mutation).await;
    assert_eq!(mut_res.unwrap(), 125);

    let get_res = store.get(key).await;
    assert!(get_res.is_ok());
    assert_eq!(get_res.unwrap(), Some(125));
}

async fn test_mutate_edge_cases(store: Basteh) {
    let key = "mutate_edge_key";

    // No mutations, no value, should be set to 0
    let mut_res = store.mutate(key, |m| m).await;
    assert_eq!(mut_res.unwrap(), 0);

    // Check if it is actually set to 0
    let get_res = store.get(key).await;
    assert_eq!(get_res.unwrap(), Some(0));

    store.set(key, 100).await.ok();

    // No mutations, with value, should keep the value
    let mut_res = store.mutate(key, |m| m).await;
    assert_eq!(mut_res.unwrap(), 100);

    // Check if it is actually kept the value
    let get_res = store.get(key).await;
    assert_eq!(get_res.unwrap(), Some(100));

    store.set(key, "Hi").await.ok();

    // No mutations, with string value
    let mut_res = store.mutate(key, |m| m).await;
    assert!(mut_res.is_err());

    // Check if it is actually kept the value
    let get_res = store.get::<String>(key).await;
    assert_eq!(get_res.unwrap(), Some("Hi".to_string()));

    // No mutations, with string value
    let mut_res = store.mutate(key, |m| m.incr(100)).await;
    assert!(mut_res.is_err());

    // Check if it is actually kept the value
    let get_res = store.get::<String>(key).await;
    assert_eq!(get_res.unwrap(), Some("Hi".to_string()));
}

async fn test_mutate_list(store: Basteh) {
    store.push("mutate_list", "value").await.unwrap();

    let pop_value = store.pop("mutate_list").await.unwrap();
    assert_eq!(pop_value, Some("value".to_string()));

    store
        .push_mutiple(
            "mutate_list",
            vec!["value1", "value2", "value3", "value4"].into_iter(),
        )
        .await
        .unwrap();

    store.push("mutate_list", 100).await.unwrap();
    store
        .push("mutate_list", Bytes::from_static(b"\0100\0"))
        .await
        .unwrap();

    let pop_value = store.pop("mutate_list").await.unwrap();
    assert_eq!(pop_value, Some(Bytes::from_static(b"\0100\0")));

    let pop_value = store.pop("mutate_list").await.unwrap();
    assert_eq!(pop_value, Some(100));

    let pop_value = store.pop("mutate_list").await.unwrap();
    assert_eq!(pop_value, Some("value4".to_string()));

    let pop_value = store.pop("mutate_list").await.unwrap();
    assert_eq!(pop_value, Some("value3".to_string()));

    store.push("mutate_list", "value5").await.unwrap();

    let pop_value = store.pop("mutate_list").await.unwrap();
    assert_eq!(pop_value, Some("value5".to_string()));

    let pop_value = store.pop("mutate_list").await.unwrap();
    assert_eq!(pop_value, Some("value2".to_string()));

    let pop_value = store.pop("mutate_list").await.unwrap();
    assert_eq!(pop_value, Some("value1".to_string()));

    let pop_value = store.pop::<String>("mutate_list").await.unwrap();
    assert_eq!(pop_value, None);
}

// delay_secs is the duration of time we set for expiry and we wait to see
// the result, it should depend on how much delay an implementer has between
// getting a command and executing it
pub async fn test_mutations<P>(provider: P)
where
    P: 'static + Provider,
{
    let store = Basteh::build().provider(provider).finish();

    tokio::join!(
        test_mutate_numbers(store.clone()),
        test_mutate_edge_cases(store.clone()),
        test_mutate_list(store.clone()),
    );
}
