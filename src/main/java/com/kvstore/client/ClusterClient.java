package com.kvstore.client;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyRingException;
import com.kvstore.consistenHashing.HashRingInterface;

public class ClusterClient {

  private HashRingInterface hashRing;
  private ConcurrentHashMap<Node, KVClient> clientPool;

  public ClusterClient(HashRingInterface hashRing) {
    this.hashRing = hashRing;
    populateClientPool();
  }

  private void populateClientPool() {
    // TODO: Error handling here? Could be that the client wasn't able to connect
    hashRing.getCopyOfNodesInRing()
        .stream()
        .forEach(n -> {
          KVClient client = new KVClient(n.gethost(), n.getport());
        });
  }

  public Optional<byte[]> getValue(String key) {
    try {
      Node node = hashRing.determineNodeForKey(key);
      KVClient client = clientPool.get(node);
      return client.get(key);
    } catch (EmptyRingException ex) {
      return Optional.empty();
    }
  }

  // TODO: should this throw? Mmm the ring is only used in this class so I think
  // throwing
  // an exception
  // is fine but should be other like NoNodesException?
  public void putValue(String key, byte[] value) throws EmptyRingException {
    Node node = hashRing.determineNodeForKey(key);
    KVClient client = clientPool.get(node);
    client.put(key, value);
  }

  // TODO: should this throw? Mmm the ring is only used in this class so I think
  // throwing
  // an exception
  // is fine but should be other like NoNodesException?
  public void deleteValue(String key) throws EmptyRingException {
    Node node = hashRing.determineNodeForKey(key);
    KVClient client = clientPool.get(node);
    client.delete(key);
  }

}
