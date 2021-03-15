## Version 0.2-alpha.2
- Updated to actix-storage 0.2-alpha.2
- There is now a new feature flag `v01-compat` for backward compatiblity with 0.1 global scopes

## Version 0.2-alpha.1
- Updated to actix-storage 0.2 with support for scopes based on key prefixes
- **Backward incompatibility:** Global scope is now a a key prefix defined in actix-storage exported from `actix-storage::GLOBAL_SCOPE` instead of no-prefix, so you'll need to migrate your previous data to have that key prefix in their names
