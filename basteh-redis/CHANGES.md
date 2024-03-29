## Version 0.4 Alpha.5

- Updated to basteh 0.4.0-alpha.5

## Version 0.4 Alpha.4

- Updated to basteh 0.4.0-alpha.4
- Pin the exact version number(cargo doesn't respect prelease version rules)

## Version 0.4 Alpha.3

- Updated to basteh 0.4.0-alpha.3
- Improve lua script generation

## Version 0.4 Alpha.2

- Updated to basteh 0.4.0-alpha.2

## Version 0.4 Alpha.1 (Not released)

- Updated to basteh 0.4.0-alpha.1
- Added support for numbers and mutations

## Version 0.3

- Update to actix 4 and tokio 1

## Version 0.2

- Updated to actix-storage 0.2

## Version 0.2-alpha.2

- Updated to actix-storage 0.2-alpha.2
- There is now a new feature flag `v01-compat` for backward compatiblity with 0.1 global scopes

## Version 0.2-alpha.1

- Updated to actix-storage 0.2 with support for scopes based on key prefixes
- **Backward incompatibility:** Global scope is now a a key prefix defined in actix-storage exported from `actix_storage::GLOBAL_SCOPE` instead of no-prefix, so you'll need to migrate your previous data to have that key prefix in their names
