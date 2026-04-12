# Distributed Key Value System — Notes

This document is a set of notes where I discuss and reflect about the system.
Also I will document technical decisions made during development.

---

## Why am I doing this project?
I want to understand the basics of Distributed systems, when the scale of data is enough so 
one computer won't be able to process everything and we need to distribute the work. This 
project is a simple Map but will be working on different computers with the most common 
techniques to deal with distribution and location of data.

---

## Technologies Motivation

### 1. RPC


### 2. Protocol Buffers


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


### KVClient and KVServer


### HashRing


