## Version 0.4 Alpha.5
- Renamed Storage to Basteh
- Combined all the provider traits(Store/Expiry/ExpiryStore) into a single trait named Provider
- Removed the glue code and no_expiry method
- Renamed `store` method to `provider` in builder

## Version 0.4 Alpha.4
- Fixed set_expiring method signature

## Version 0.4 Alpha.3
- Changed get/set methods to accept bytes/strings/numbers
- Removed set_number and get_number methods
- Added test for mutation after expiration

## Version 0.4 Alpha.2
- Changed Arc scope/key/values to refrences, backends can use Arc as they wish
- Scope is now required to be utf-8 string
- Fix doc/repo links in crate

## Version 0.4 Alpha.1
- Renamed the package to `basteh` since we don't rely on actix anymore and actix-storage is missleading
- Removed `actix` actors support
- Removed formats and de/serialization support
- - De/Serialization should ideally depends on the backend or decided by the user per case.
- Removed `get`, `set`, `get_expiring` and `set_expiring` methods, and replace them by their `_byte` equivalent.
- Added numeric value get/set support with `_number`
- Added mutation support for numeric values

## Version 0.3
- Updated to actix 4 and tokio 1

## Version 0.2
- Nothing

## Version 0.2-alpha.2
- Improved tests
- Bugfixes
- Backward compatiblity flags

## Version 0.2-alpha.1
- Added Scope to storage operations - Scopes may or may not be implemented as key prefixes
- Implemented extractor(FromRequest) for Storage itself, as it is already Arc'ed

Note: For a more detailed changelog, read subcrates' CHANGES.md files
