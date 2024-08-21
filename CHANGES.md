# `laaws-metadataextractor-common` Release Notes

## Changes since 2.6.0

* Remove  Travis CI
* Move to OpenAPI 3
* Move to Java 17
* Move to springdoc
* Added triggerWDogOnExit calls
* Ensure database connections are closed
* UI rewording: Journal Configuration -> AU Configuration, Volume/Volume Name/Journal Volume -> AU, Node Url -> URL, Box -> Peer, and similar. (see lockss-daemon caed1637f7b4a9e9c6ade26c0186752719ff674f)
* Give XxxDbManagerSql classes access to owning XxxDbManager
* Spring 6.x and Spring Boot 3.x related changes

## Changes Since 2.0.5.0

*   Switched to a 3-part version numbering scheme.

## 2.0.5.0

### Features

*   ...

### Fixes

*   ...

## 2.0.4.0

### Features

*   Added automatic metadata indexing
*   Made the PostgreSQL schema name configurable
*   Added configurable prefix for database names
*   Better decoupling of the metadata service from the metadata extractor service
