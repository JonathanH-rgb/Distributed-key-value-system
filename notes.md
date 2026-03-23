# Distributed Key Value System — Notes

This document is a set of notes where I discuss and reflect about the system.
Also I will document technical decisions made during development.

---

## Why am I doing this project?

I always wanted to know about high performance systems with concurrent operations
and distributed nodes working towards one operation. So that is why I decided to
make this, in theory is a simple application. You provide a key and the system
returns a value, behind the scene it uses different technologies like RPC,
protobuf and distributed nodes.

---

## Technologies Motivation

### 1. RPC

As I mention the main goal of this system is to provide 3 functions: `get(key)`,
`put(key, value)` and `delete(key)` so you can retrieve values in your computer
from the server, that is simple, right? Well it is but we will make it run in
another computer using a remote procedure call (RPC) that is a paradigm where we
invoke functions in our code but it will be execute in another computer so you
have a simple API and don't have to use the classic HTTP protocol.

### 2. Protocol Buffers

It is a google serialization library at byte level, instead of sending data
encoded as text like in json formats the client and server both agree on the data
structure before the data flows. That way we can save a lot of network resources
only sending values with light bytes-wide signals and some meta information per
message.

---

## Structure

```
  Client Application
         │
         ▼
   ClusterClient ←→ HashRing
   (quorum reads/writes)  (consistent hashing,
         │                 determines which nodes
         │                 own a key)
    ┌────┼────┬────────┐
    ▼    ▼    ▼        ▼
  KVClient  KVClient  KVClient  ...
    │         │          │
    ▼         ▼          ▼
  KVServer  KVServer  KVServer
    │         │          │
    ▼         ▼          ▼
  Storage   Storage   Storage
  Engine    Engine    Engine
```

### Storage Engine

For now the storage engine will be a simple ConcurrentHashMap, it will implement
an interface so it's easy to replace to something more robust in the future, the
app only cares about the interface. It's concurrent so it can handle multiple
operations with no race conditions.

### KVClient and KVServer

This classes are the backbone that connects my application with gRPC, both implement some required characteristics by gRPC so the remote calls can be done. The client can invoke some functions defined in the .proto file and the server will execute the code and return the result.

### HashRing

To explain this class first I think a good approach would be to state the problem it solves. We have multiple nodes that have (key, value) pairs, of course each node can't have the whole dataset because at some scale it won't fit in a single computer, we have to scale horizontally. Ok so we will split the data, now we have to determine an algorithm that given a key returns a set of Nodes where that data should be.

We have multiple options, like transform the key to a unique integer and then apply the modulo over the number of nodes, or check all nodes to see which have the key. At the end those solutions have their own problems like doing a multiple node scan is not reliable, maybe the value was in a node that is down but we don't know because it's down, or the modulo is deterministic and tells us precisely where to find the key, but when we want to delete one node now we have to recompute all modulo operations over all the key set to migrate values from the soon to be deleted node. 

