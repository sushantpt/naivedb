# naiveDB

## Introduction

naiveDB is a simple database implementation with the following characteristics (initially. current implementation is not naive at all with support for indexing and other features):

- Flat storage: Data is written in JSON format
- Linear retrieval: Fetching requires scanning through the whole data set or until data is found
- No indexing
- No query planner, just raw execution
- No concurrency mechanism
- No async/await support
---
## Commands and queries

| Initials                   | Description                     | Usage                                                                | Example                                                    |
|----------------------------|---------------------------------|----------------------------------------------------------------------|------------------------------------------------------------|
| create                     | Create a new database           | create `<database_name>`                                             | create mydatabase                                          |
| list                       | List all databases              | list                                                                 | list                                                       |
| connect database           | Connect to a database           | connect `<database_name>`                                            | connect mydatabase                                         |
| disconnect database        | Disconnect from a database      | disconnect `<database_name>`                                         | disconnect mydatabase                                      |
| export                     | Export database to JSON         | export `<database_name>`                                             | export mydatabase                                          |
| import                     | Import database from JSON       | import `<path_to_json>`                                              | import ./backup.json                                       |
| drop                       | Delete database                 | drop `<database_name>`                                               | drop mydatabase                                            |
| query create               | Create a new table              | query create table -n `<table_name>`                                 | query create table -n users                                |
| query info                 | View table metadata             | query info -n `<table_name>`                                         | query info -n users                                        |
| query add                  | Insert new record               | query add -n `<table_name>` -data '{json}'                           | query add -n users -data '{"id": 1, "name": "John"}'       |
| query get                  | Retrieve records                | query get -n `<table_name>`                                          | query get -n users                                         |
| query get by key           | Retrieve record by key          | query get by key -n `<table_name>` -key `<key>`                      | query get by key -n users -key 123                         |
| query get range            | Retrieve records in key range   | query get range -n `<table_name>` -start `<start_key>` -end `<end_key>` | query get range -n users -start 1 -end 10               |
| query delete any predicate | Delete record(s)                | query delete -n `<table_name>` where key==value                      | query delete -n users where name==john                     |
| query delete by key        | Delete record by key            | query delete by key -n `<table_name>` -key `<key_value>`             | query delete by key -n users -key 123                      |
| query delete range         | Delete records in a key range   | query delete range -n `<table_name>` -start `<start_key>` -end `<end_key>` | query delete range -n users -start 1 -end 10           |
| query update               | Update records with predicate   | query update -n `<table_name>` -data '{json}' where key==value       | query update -n users -data '{"name": "John"}' where id==1 |
| query tables               | Get list of tables              | query tables                                                         | query tables                                               |
| query drop                 | Drop table                      | query drop -n `<table_name>`                                         | query drop -n users                                        |

---

---

## Versions & notes
> v1.0.0
- Initial release
   1. cli support
   2. json based storage
   3. linear retrieval
   4. no indexing
   5. no query planner
   6. no concurrency mechanism
- Moving forward
  1. better file storage (paging)
  2. improve cli
  3. code cleanup

> v1.1.0
- improvements on v1.0.0
   1. better file storage (paging)
   2. richer cli
   3. code cleanup

> v1.2.0 (skipped to v2.0.0)
- improvements on v1.1.0
  1. robust error handling
  2. code cleanup 
  3. proper logging (even if it fails; async -> fire and forget)

> v1.3.0 (skipped to v2.0.0)
- improvements on v1.2.0
   1. test coverage
   2. code cleanup

> v2.0.0
- sorta major update: indexing support and cover up backlog of v1.2.0 and v1.3.0
1. indexing support
2. better logging
3. better error handling
4. code cleanup

> v2.0.1 🛫
- improve on v2.0.0
1. indexing support (which was not actually implemented in v2.0.0)
2. code cleanup
3. fix race condition

> v2.1.0
- minor update: better error handling and native lib support for .net
1. better error handling
2. native lib support for .net
3. code cleanup
4. benchmarking
detail: v2.0.0 is very io heavy for inserts and updates. this can be improved by having a background service that does writes (wal), implement tombstone and use background service to flush, etc.
---

## todo
* native lib and helper extensions support for c# 
* journaling, wal
* caching 
* concurrency 
* performance (parallelism, caching)
* tui support (gui -> Terminal.Gui)


---
---
---

notes

there is basically 3 layers here:
* physical storage -> actual physical storage which is paged.
* table-level data management -> bpt used for cold start and index mirror
* indexing -> caching which is volatile



