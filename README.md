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

## Project Structure

The project consists of the following components:

- **core**: Core functionality (all logic resides here)
- **server**: Connection server (TCP, HTTP, gRPC, etc.)
- **tests**: All test cases
- **tools**: Migration, benchmarking, profiling, etc.

### rules
- no direct call to core module; use facade 
- no try-catch in core module except logger (handle it in facade)
---

### Dependency Graph
- core
- cli      ---> core
- tests    ---> core
- server   ---> core
- tools    ---> core
------

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

> v2.0.0 🛫
- sorta major update: indexing support and cover up backlog of v1.2.0 and v1.3.0
1. indexing support
2. better logging
3. better error handling
4. code cleanup

> v2.1.0
- minor update: better error handling and native lib support for .net
1. better error handling
2. native lib support for .net
3. code cleanup
4. benchmarking
---

## todo
* native lib and helper extensions support for c# 
* journaling, wal
* caching 
* concurrency 
* performance (parallelism, caching)



