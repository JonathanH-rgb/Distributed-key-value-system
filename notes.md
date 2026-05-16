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
RPC stands for remote procedure call. Is a communication protocol that let us call procedures in other computers as if they were a function. It's an alternative from REST APIs used in service to service communication where two services agree on the structure of the data before communication, making it more efficient at readability cost.

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
An storage engine is responsible to persist data, it is the heart of the application and provides a basic API to GET, PUT and DELETE methods. Of course there are some properties we desire from the storage engine as persistence that means the data will be there after the client gets a confirmation whatever happens, parallelism to handle multiple request at the same time and also efficiency to retrieve the data fast.

To handle persistence we will use a WAL(write ahead log) that is basically a text file where we store the PUT/DELETE request, this way we know the information is now on a text file and even if the servers fails we can reconstruct the map from the file. I used this syntax: 
String message = time + "|" + Operation.PUT + "|" + key + "|" + valueString + "|" + version;
Of course we'll have to handle parallel request and more if it's writing to a file, that is the reason we use synchronized so only one thread at the time can use the method, for example:
public synchronized void writeDelete
The last function we need is to build the Map<key,value> from the file, that is going to iterate over the file and parsing the string into java types. Also we included an time argument to the function so the restore is after that point in time, this will become handy with the next feature.

Having the PUT/DELETE operations in a file as a safety measure is good but it's not the most efficient thing to recover state, imagine the client sends PUT {"TEST":1} -> PUT {"TEST":2} ... ->  PUT {"TEST":1,000}. If we wanted to reconstruct the Map we would be doing 1,000 operations. That is why we also have a Snapshot feature. That is basically going to just copy the Map into a file every N minutes, this way we have safety per operation with the WAL and an efficient way to retrieve the state.


### KVClient and KVServer


### HashRing


