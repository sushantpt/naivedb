# naiveDB v1.0.0

## Introduction

naiveDB is a simple database implementation with the following characteristics:

- Flat storage: Data is written in JSON format
- Linear retrieval: Fetching requires scanning through the whole data set or until data is found
- No indexing
- No query planner, just raw execution
- No concurrency mechanism
- No async/await support

## Project Structure

The project consists of the following components:

- **core**: Core functionality (all logic resides here)
- **server**: Connection server (TCP, HTTP, gRPC, etc.)
- **tests**: All test cases
- **tools**: Migration, benchmarking, profiling, etc.

### Dependency Graph
- core
- cli      ---> core
- tests    ---> core
- server   ---> core
- tools    ---> core

