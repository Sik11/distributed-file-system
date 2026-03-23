# Distributed File System

A distributed file storage system built in Java using TCP sockets, implementing file replication across multiple data stores with a central controller for coordination.

Personal project exploring distributed file storage, replication, and coordination in Java.

## Architecture

```
        ┌──────────┐
   ┌────┤Controller├────┐
   │    └────┬─────┘    │
   │         │          │
┌──┴───┐ ┌───┴──┐ ┌───┴──┐
│Dstore│ │Dstore│ │Dstore│
│  1   │ │  2   │ │  3   │
└──────┘ └──────┘ └──────┘
```

The system consists of three component types:

- **Controller** - Central orchestrator that manages file metadata (the Index), handles client requests, and coordinates file replication across Dstores.
- **Dstore (Data Store)** - Storage nodes that hold actual file data on disk. Multiple Dstores run concurrently, each managing its own file folder.
- **Client** - Connects to the Controller to perform file operations.

## Protocol

Communication uses a custom text-based protocol over TCP:

| Operation | Client &rarr; Controller | Controller &rarr; Client | Controller &rarr; Dstore |
|-----------|-------------------------|-------------------------|-------------------------|
| Store     | `STORE filename size`   | `STORE_TO port1 port2...` | -                     |
| Load      | `LOAD filename`         | `LOAD_FROM port size`    | -                      |
| Remove    | `REMOVE filename`       | `REMOVE_COMPLETE`        | `REMOVE filename`      |
| List      | `LIST`                  | `LIST file1 file2...`    | -                      |

## Key Features

- **File Replication** - Files are replicated across R Dstores (configurable replication factor)
- **Concurrent Operations** - Thread-safe operations using `ConcurrentHashMap`, `CountDownLatch`, and `ReentrantReadWriteLock`
- **Load Balancing** - Store operations select Dstores with the fewest files
- **Rebalancing** - Periodic redistribution of files across Dstores to maintain even distribution
- **Fault Tolerance** - Handles Dstore disconnections, timeouts, and reload attempts across replicas

## Concurrency Model

- The Controller uses a fair `ReentrantReadWriteLock` to protect the Dstore connection set
- Store and Remove operations use `CountDownLatch` to await acknowledgements from Dstores with configurable timeouts
- All connection handling runs on dedicated threads
- The `Index` class uses `ConcurrentHashMap` for thread-safe file metadata tracking with state transitions (e.g., "store in progress" &rarr; "store complete")

## Building and Running

### Prerequisites

- Java 17 or later

### Compile

```bash
mkdir -p out && javac -cp lib/client.jar -d out $(find src -name '*.java' | sort) examples/*.java tests/*.java
```

### Start the Controller

```bash
java -cp lib/client.jar:out dfs.controller.Controller <cport> <replication_factor> <timeout_ms> <rebalance_period_s>
```

Example:

```bash
java -cp lib/client.jar:out dfs.controller.Controller 12345 3 2000 10
```

### Start Dstores (start R or more instances)

```bash
java -cp lib/client.jar:out dfs.dstore.Dstore <port> <controller_port> <timeout_ms> <file_folder>
```

Example (start 3 Dstores):

```bash
java -cp lib/client.jar:out dfs.dstore.Dstore 12346 12345 2000 var/dstores/dstore1
java -cp lib/client.jar:out dfs.dstore.Dstore 12347 12345 2000 var/dstores/dstore2
java -cp lib/client.jar:out dfs.dstore.Dstore 12348 12345 2000 var/dstores/dstore3
```

### Start a Client

```bash
java -cp lib/client.jar:out ClientMain <controller_port> <timeout_ms>
```

Example:

```bash
java -cp lib/client.jar:out ClientMain 12345 2000
```

### Run the Concurrency Smoke Test

Start the controller and at least 3 Dstores first, then run:

```bash
java -cp lib/client.jar:out ConcurrencySmokeTest clients-only 12345
```

## Project Structure

- `src/dfs/controller/` - Controller entry point and controller-side connection handling
- `src/dfs/dstore/` - Dstore entry point and Dstore-side connection handling
- `src/dfs/core/` - Shared protocol, indexing, rebalancing, and base connection classes
- `src/dfs/logging/` - Internal logging infrastructure
- `examples/` - Example client programs for manual testing
- `tests/` - End-to-end verification utilities, including the concurrency smoke test
- `lib/` - External client library dependency
- `config/` - Auxiliary configuration files such as `my_policy.policy`
- `var/` - Recommended runtime location for Dstore folders, downloads, and upload fixtures

## License

MIT License - see [LICENSE](LICENSE)
