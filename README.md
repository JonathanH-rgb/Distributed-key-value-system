# Distributed Key-Value Store

A distributed key-value store built from scratch in Java, using gRPC for inter-node communication and consistent hashing for key partitioning.

## Architecture

```
Client Application
       │
       ▼
 ClusterClient          ← routes requests using the hash ring
       │
  HashRing              ← consistent hashing, determines which node owns a key
       │
  ┌────┴────┐
  ▼         ▼
Node 1    Node 2  ...   ← KVServer instances (gRPC servers)
  │         │
  ▼         ▼
InMemoryStore           ← ConcurrentHashMap-backed storage
```

### Components

**Storage Layer** (`com.kvstore.storage`)
- `StorageEngine` — interface defining `get`, `put`, `delete`
- `InMemoryStore` — thread-safe implementation backed by `ConcurrentHashMap`

**Server Layer** (`com.kvstore.server`)
- `KVServer` — gRPC service implementation. Handles `Get`, `Put`, `Delete` RPCs and delegates to a `StorageEngine`
- `ServerMain` — entry point, starts the gRPC server on a configured port

**Client Layer** (`com.kvstore.client`)
- `KVClient` — thin gRPC client wrapping a single node connection
- `ClusterClient` — multi-node client. Uses `HashRing` to route each operation to the correct node and maintains a pool of `KVClient` instances

**Consistent Hashing** (`com.kvstore.consistenHashing`)
- `HashRing` — places nodes on a ring using MD5 hashing. Each physical node is represented by N virtual nodes to ensure even key distribution. Uses a `TreeMap` for O(log N) key lookup via `ceilingKey`

**Common** (`com.kvstore.common`)
- `Node` — represents a cluster node with host and port
- Custom exceptions: `EmptyRingException`, `NodeAlreadyInRingException`, `NodeNotInRingException`

## Communication Protocol

Nodes communicate using [gRPC](https://grpc.io/) over HTTP/2 with [Protocol Buffers](https://protobuf.dev/) for serialization. The schema is defined in `src/main/proto/kvstore.proto`.

Three operations are supported:

| Operation | Request | Response |
|-----------|---------|----------|
| `Get` | key | value (bytes), found flag |
| `Put` | key, value (bytes) | success flag |
| `Delete` | key | success flag |

## Running

**Requirements:** Java 17+, Maven 3.8+

**Start a node:**
```bash
mvn compile
mvn exec:java -Dexec.mainClass="com.kvstore.server.ServerMain"
```

The server starts on port `8080` by default.

**Run tests:**
```bash
mvn test
```

## Project Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Single-node KV store with gRPC | Done |
| 2 | Consistent hashing & partitioning | In progress |
| 3 | Replication + quorum consistency | Planned |
| 4 | Failure detection & recovery | Planned |
| 5 | Raft consensus for metadata | Planned |
| 6 | Polish & persistence | Planned |
