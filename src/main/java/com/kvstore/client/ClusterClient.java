package com.kvstore.client;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyRingException;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;
import com.kvstore.consistenHashing.HashRingInterface;

/**
 * Multi-node client for the distributed KV store.
 * Uses a HashRing to determine which node owns a given key and routes
 * each operation to the correct node via a pool of KVClient instances.
 */
public class ClusterClient {

  private final HashRingInterface hashRing;
  private ConcurrentHashMap<Node, KVClient> clientPool;

  public ClusterClient(final HashRingInterface hashRing) {
    this.hashRing = hashRing;
    this.clientPool = new ConcurrentHashMap<>();
    populateClientPool();
  }

  public ClusterClient(final HashRingInterface hashRing, ConcurrentHashMap<Node, KVClient> clientPool) {
    this.hashRing = hashRing;
    this.clientPool = clientPool;
  }

  private void createClientAndPutInPool(Node node) {
    final KVClient client = new KVClient(node.gethost(), node.getport());
    clientPool.put(node, client);
  }

  private void populateClientPool() {
    hashRing.getCopyOfNodesInRing()
        .stream()
        .forEach(node -> {
          createClientAndPutInPool(node);
        });
  }

  public Optional<byte[]> getValue(final String key) {
    try {
      final Node node = hashRing.determineNodeForKey(key);
      final KVClient client = clientPool.get(node);
      return client.get(key);
    } catch (final EmptyRingException ex) {
      return Optional.empty();
    }
  }

  public void putValue(final String key, final byte[] value) throws EmptyRingException {
    final Node node = hashRing.determineNodeForKey(key);
    final KVClient client = clientPool.get(node);
    client.put(key, value);
  }

  public void deleteValue(final String key) throws EmptyRingException {
    final Node node = hashRing.determineNodeForKey(key);
    final KVClient client = clientPool.get(node);
    client.delete(key);
  }

  public void addNode(final Node node) throws NodeAlreadyInRingException {
    hashRing.addNode(node);
    createClientAndPutInPool(node);
  }

  public void removeNode(final Node node) throws NodeNotInRingException {
    hashRing.removeNode(node);
    KVClient client = clientPool.get(node);
    client.shutdown();
    clientPool.remove(node);
  }

}
