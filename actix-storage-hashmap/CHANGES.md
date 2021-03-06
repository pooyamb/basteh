## Version 0.2-alpha.1
- Updated to actix-storage 0.2 with support for scopes
- Changed the underlying map to HashMap<scope, Hashmap> to support scopes
- Basic implementor's from_hashmap now requires the new map structure
