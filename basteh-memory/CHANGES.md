## Version 0.4 Alpha.3
- Updated to basteh 0.4.0-alpha.3
- Doesn't Arc the values anymore

## Version 0.4 Alpha.2
- Updated to basteh 0.4.0-alpha.2

## Version 0.4 Alpha.1 (Not released)
- Updated to basteh 0.4.0-alpha.1
- Removed the actix dependency
- Removed the actor version completely
- Removed the basic store implementation
- Added a new full store, using Arc<Mutex> and tokio's delayqueue

## Version 0.3
- Updated to actix 4 and tokio 1

## Version 0.2
- Updated to actix-storage 0.2

## Version 0.2-alpha.2
- Updated to actix-storage 0.2-alpha.2

## Version 0.2-alpha.1
- Updated to actix-storage 0.2 with support for scopes
- Changed the underlying map to HashMap<scope, Hashmap> to support scopes
- Basic implementer's from_hashmap now requires the new map structure
