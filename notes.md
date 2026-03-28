# Distributed Key Value System — Notes

This document is a set of notes where I discuss and reflect about the system.
Also I will document technical decisions made during development.

---

## Why am I doing this project?

I always wanted to know about high performance systems with concurrent operations
and distributed nodes working towards one operation. So that is why I decided to
make this. In theory is a simple application, you provide a key, and the system
returns a value. Behind the scene it uses different technologies like RPC,
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

To explain this class first I think a good approach would be to state the problem it solves. We have multiple nodes that have (key, value) pairs, at some point the dataset will be to big to fit in just one computer, we have to scale horizontally. So we will split the data, now we have to determine an algorithm that given a key returns a set of Nodes where that data should be.

We have multiple options, like transform the key to a unique integer and then apply the modulo over the number of nodes, this gives a result that is also evenly distributed on nodes(that is a big plus) but a problem is that if we add/remove a node we will have to migrate a lot of keys. Of course for a system that is suppose to have millions of records this is not a good approach. 

Another approach would be to add values to each node in order, we keep in a variable the last node where we store a value and insert to the next one, this has two big problems, now when the user gives us the key we have to do do in the worst case a check on every node to see which has the value. Also if a node is down and it's the case that it had the value, we have no way to distinguish if the value was on that node or is not in the system.

We can see from the solutions suggested above that a good algorithm would be able to identify the nodes or nodes that are suppose to have the data just with the key, and also make the migrations as cheap as possible. This is a known problem and the data structure used to solve this problem is called a HashRing.

The HashRing is a structure that will save the server indentifiers hashed, remember that this hashed server identifier is just an integer. Then when given a key, first we have to determine which N nodes should manage that key. The HashRing will hash the key, and find in the hashed server the next PARTITION_FACTOR. For example if I have hashed server indentifiers [100,230,2323,5000] and the hash for 'key' is 200 with and I am storing data in two servers(because I want to avoid single point failures) then this HashRing will return me [230, 2323]. Now that I have the hashed server identifiers I just have to get the server directions and now I can perform my operations. 


