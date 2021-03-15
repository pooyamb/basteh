## Version 0.2-alpha.2
- Updated to actix-storage 0.2-alpha.2
- Bug fixes:
- - Expiry extend overrided the expiry time instead of extending it
- - Reseting expiry didn't work if the new expiry time was shorter that the existing
- - Nonce could overflow in expiry flags

## Version 0.2-alpha.1
- Updated to actix-storage 0.2 with support for scopes
- Changed the underlying map to DashMap<scope, Dashmap> to support scopes
- Basic implementor's from_dashmap now requires the new map structure
