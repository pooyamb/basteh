## Version 0.4 Alpha.2
- Updated to basteh 0.4.0-alpha.2

## Version 0.4 Alpha.1 (Not released)
- Updated to basteh 0.4.0-alpha.1
- Removed the actix dependency
- Removed the actor version completely
- Removed the basic store implementation
- Added a new full store, using crossbeam channels and tokio's spawn_blocking

## Version 0.3 (Not released because of a dependency problem)
- Updated to actix 4.0 and tokio 1.0

## Version 0.2 (Not released because of a dependency problem)
- Updated to basteh 0.2

## Version 0.2-alpha.3
- Restructured the code
- Bug fixes

## Version 0.2-alpha.2
- Updated to actix-storage 0.2-alpha.2
- There is now a new feature flag `v01-compat` for backward compatiblity with 0.1 global scopes

## Version 0.2-alpha.1
- Updated to actix-storage 0.2 with support for scopes based on sled's trees
- **Backward incompatibility:** Global scope is now a subtree of the main sled's db, so you'll need to migrate your data from sled's global tree to actix-storage's global tree on `STORAGE_GLOBAL_SCOPE` exported as a constant from `actix_storage::GLOBAL_SCOPE`
