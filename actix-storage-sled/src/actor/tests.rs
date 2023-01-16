use super::*;
use actix_storage::test_utils::*;
use inner::open_tree;
use utils::{encode, get_current_timestamp};
use zerocopy::{U16, U64};

async fn open_database() -> sled::Db {
    let mut tries = 0;
    loop {
        tries += 1;
        if tries > 5 {
            break;
        };
        let db = SledConfig::default().temporary(true).open();
        if let Ok(db) = db {
            return db;
        } else {
            // Wait for sometime and try again.
            actix::clock::sleep(Duration::from_millis(500)).await;
        }
    }
    panic!("Sled can not open the database files")
}

#[test]
fn test_sled_store() {
    test_store(Box::pin(async {
        SledActor::from_db(open_database().await).start(1)
    }));
}

#[test]
fn test_sled_store_numbers() {
    test_store_numbers(Box::pin(async {
        SledActor::from_db(open_database().await).start(1)
    }));
}

#[test]
fn test_sled_expiry() {
    test_expiry(
        Box::pin(async {
            let store = SledActor::from_db(open_database().await).start(1);
            (store.clone(), store)
        }),
        4,
    );
}

#[test]
fn test_sled_expiry_store() {
    test_expiry_store(
        Box::pin(async { SledActor::from_db(open_database().await).start(1) }),
        4,
    );
}

#[actix::test]
async fn test_sled_perform_deletion() {
    let scope: Arc<[u8]> = "prefix".as_bytes().into();
    let key: Arc<[u8]> = "key".as_bytes().into();
    let value = "val".as_bytes().into();
    let db = open_database().await;
    let dur = Duration::from_secs(1);
    let store = SledActor::from_db(db.clone())
        .perform_deletion(true)
        .start(1);
    store
        .send(StoreRequest::Set(scope.clone(), key.clone(), value))
        .await
        .unwrap();
    store
        .send(ExpiryRequest::Set(scope.clone(), key.clone(), dur))
        .await
        .unwrap();
    assert!(open_tree(&db, &scope)
        .unwrap()
        .contains_key(key.clone())
        .unwrap());
    actix::clock::sleep(dur * 2).await;
    assert!(!open_tree(&db, &scope).unwrap().contains_key(key).unwrap());
}

#[actix::test]
async fn test_sled_scan_on_start() {
    let db = open_database().await;

    let dur = Duration::from_secs(2);
    let value = encode("value".as_bytes(), ExpiryFlags::new_expiring(1, dur));
    let value2 = encode(
        "value2".as_bytes(),
        ExpiryFlags {
            persist: U16::ZERO,
            nonce: U64::new(1),
            expires_at: U64::new(get_current_timestamp() - 1),
        },
    );

    db.insert("key", value).unwrap();
    db.insert("key2", value2).unwrap();
    let actor = SledActor::from_db(db.clone())
        .scan_db_on_start(true)
        .perform_deletion(true)
        .start(1);

    // Waiting for the actor to start up, there should be a better way
    actix::clock::sleep(Duration::from_millis(500)).await;
    assert!(db.contains_key("key").unwrap());
    assert!(!db.contains_key("key2").unwrap());
    actix::clock::sleep(Duration::from_millis(2000)).await;
    assert!(!db.contains_key("key").unwrap());

    // Making sure actor stays alive
    drop(actor)
}
