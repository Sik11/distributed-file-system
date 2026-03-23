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
javac -cp .:client.jar *.java
```

### Start the Controller

```bash
java Controller <cport> <replication_factor> <timeout_ms> <rebalance_period_s>
```

Example:

```bash
java Controller 12345 3 2000 10
```

### Start Dstores (start R or more instances)

```bash
java Dstore <port> <controller_port> <timeout_ms> <file_folder>
```

Example (start 3 Dstores):

```bash
java Dstore 12346 12345 2000 dstore1
java Dstore 12347 12345 2000 dstore2
java Dstore 12348 12345 2000 dstore3
```

### Start a Client

```bash
java -cp .:client.jar ClientMain <controller_port> <timeout_ms>
```

Example:

```bash
java -cp .:client.jar ClientMain 12345 2000
```

## File Structure

| File | Description |
|------|-------------|
| `Controller.java` | Central controller - accepts connections, orchestrates operations, runs rebalancing |
| `Dstore.java` | Data store node - stores files on disk, responds to Controller commands |
| `Connection.java` | Abstract base class for TCP socket connections with message handling |
| `ControllerClientConnection.java` | Handles client-to-controller communication |
| `ControllerDstoreConnection.java` | Handles controller-to-dstore communication |
| `DstoreClientConnection.java` | Handles client/controller-to-dstore communication |
| `Index.java` | File index with state management and acknowledgement tracking |
| `FileInformation.java` | Data class for file metadata (name, size, state, storing Dstores) |
| `Protocol.java` | Protocol message constants |
| `Rebalance.java` | Standalone rebalance algorithm implementation |
| `Logger.java` | Abstract logging base class |
| `ControllerLogger.java` | Controller-specific logging |
| `DstoreLogger.java` | Dstore-specific logging |
| `ClientMain.java` | Example client for testing (requires `client.jar`) |
| `ClientTest1.java` | Additional test client (requires `client.jar`) |
| `client.jar` | University-provided client library |

## License

MIT License - see [LICENSE](LICENSE)
