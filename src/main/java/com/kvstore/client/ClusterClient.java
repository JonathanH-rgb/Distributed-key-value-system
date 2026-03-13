package com.kvstore.client;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyRingException;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;
import com.kvstore.consistenHashing.HashRingInterface;

public class ClusterClient {

  private final HashRingInterface hashRing;
  private ConcurrentHashMap<Node, KVClient> clientPool;

  public ClusterClient(final HashRingInterface hashRing) {
    this.hashRing = hashRing;
    populateClientPool();
  }

  private void populateClientPool() {
    // TODO: Error handling here? Could be that the client wasn't able to connect
    hashRing.getCopyOfNodesInRing()
        .stream()
        .forEach(n -> {
          final KVClient client = new KVClient(n.gethost(), n.getport());
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
  }

  public void removeNode(final Node node) throws NodeNotInRingException {
    hashRing.removeNode(node);
  }

}
