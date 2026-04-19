# Distributed Key-Value Store

A distributed key-value store built from scratch in Java, using gRPC for inter-node communication, consistent hashing for key partitioning, quorum-based replication, and gossip-based failure detection.

## Architecture

```
Client Application
       │
       ▼
 ClusterClient          ← routes requests using the hash ring, auto-discovers nodes
       │
  HashRing              ← consistent hashing, determines which nodes own a key
       │
  ┌────┼────┐
  ▼    ▼    ▼
Node1 Node2 Node3 ...   ← KVServer instances (gRPC servers)
  │    │    │
  ▼    ▼    ▼
InMemoryStore           ← ConcurrentHashMap-backed storage per node

Servers also gossip with each other:
Node1 ←──gossip──► Node2 ←──gossip──► Node3
```

## Components

**Storage Layer** (`com.kvstore.storage`)
- `StorageEngine` — interface defining `get`, `put`, `delete`
- `InMemoryStore` — thread-safe implementation backed by `ConcurrentHashMap` with versioned values

**Server Layer** (`com.kvstore.server`)
- `KVServer` — gRPC service implementation. Handles `Get`, `Put`, `Delete`, `Gossip`, and `ViewCluster` RPCs
- `ClusterConfig` — loads gossip tuning parameters from `cluster.properties` (fanout factor, timeouts, gossip frequency, max failed attempts)

**Client Layer** (`com.kvstore.client`)
- `KVClient` — thin gRPC client wrapping a single node connection
- `ClusterClient` — multi-node client. Uses `HashRing` to route each operation to the correct nodes, enforces read/write quorum, and auto-discovers cluster topology from seed nodes every 3 seconds
- `GossipClient` — server-to-server client used exclusively for gossip between nodes
- `GossipClientFactory` — interface for creating `GossipClient` instances, injectable for testing

**Consistent Hashing** (`com.kvstore.consistenHashing`)
- `HashRing` — places nodes on a ring using MD5 hashing. Each physical node is represented by N virtual nodes for even key distribution. Uses a `TreeMap` for O(log N) key lookup

**Failure Detection** (built into `KVServer` and `com.kvstore.common`)
- Gossip protocol: each server periodically selects random peers and exchanges cluster state
- `NodeInformation` — tracks each node's status (ALIVE / SUSPECT / DEAD), heartbeat counter, and incarnation number
- Incarnation number: set to `System.currentTimeMillis()` at startup so a restarted node always overrides stale state in the cluster
- A node is marked SUSPECT after one missed gossip, DEAD after `MAX_GOSSIP_ATTEMPTS` consecutive failures
- `ClusterClient` polls seed nodes for cluster view and automatically adds/removes nodes from the hash ring

**Common** (`com.kvstore.common`)
- `Node` — immutable cluster node identifier (host + port), safe to use as map key
- `VersionedValue` — byte array with a version number for replica conflict resolution
- Custom exceptions: `NodeAlreadyInRingException`, `NodeNotInRingException`, `NotEnoughNodesException`, `WriteConsensusException`, `EmptyHardcodedNodesListException`

## Communication Protocol

Nodes communicate using [gRPC](https://grpc.io/) over HTTP/2 with [Protocol Buffers](https://protobuf.dev/) for serialization. Schema defined in `src/main/proto/kvstore.proto`.

| RPC | Used by | Description |
|-----|---------|-------------|
| `Get` | client → node | Read a key |
| `Put` | client → node | Write a key/value with version |
| `Delete` | client → node | Delete a key |
| `Gossip` | node → node | Exchange cluster state |
| `ViewCluster` | client → node | Get this node's view of the cluster |

### Replication & Consistency

- Each key is replicated across `PARTITION_FACTOR=3` nodes determined by the hash ring
- Writes require `WRITE_CONSENSUS_NUMBER=2` nodes to acknowledge
- Reads query `PARTITION_FACTOR` nodes in parallel and return the value with the highest version if at least `READ_CONSENSUS_NUMBER=2` respond

## Configuration

Create `src/main/resources/cluster.properties`:

```properties
FANOUT_FACTOR=3
GOSSIP_TIMEOUT_SECS=5
GOSSIP_LOOP_DELAY_SECS=0
GOSSIP_OTHER_SERVERS_FREQ_SECS=1
MAX_GOSSIP_ATTEMPS=3
```

## Running

**Requirements:** Java 21+, Maven 3.8+

**Run tests:**
```bash
mvn test
```

## Project Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Single-node KV store with gRPC | Done |
| 2 | Consistent hashing, partitioning, quorum reads/writes | Done |
| 3 | Gossip-based failure detection (SUSPECT/DEAD, incarnation numbers, auto-discovery) | Done |
| 4 | Persistence — WAL + snapshots | Planned |
| 5 | Raft consensus — leader election + replicated log | Planned |
| 6 | Read repair | Planned |
| 7 | Data migration on topology changes | Planned |
| 8 | TLS + authentication | Planned |
| 9 | Benchmarking & tuning | Planned |
