## Version 0.2-alpha.1
- Updated to actix-storage 0.2 with support for scopes
- Changed the underlying map to DashMap<scope, Dashmap> to support scopes
- Basic implementor's from_dashmap now requires the new map structure
