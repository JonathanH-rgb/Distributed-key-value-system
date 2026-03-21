package com.kvstore.client;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.kvstore.common.Node;
import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;
import com.kvstore.common.exceptions.NotEnoughNodesException;
import com.kvstore.consistenHashing.HashRingInterface;

/**
 * Multi-node client for the distributed KV store.
 * Uses a HashRing to determine which node owns a given key and routes
 * each operation to the correct node via a pool of KVClient instances.
 */
public class ClusterClient {

  private final HashRingInterface hashRing;
  private ConcurrentHashMap<Node, KVClient> clientPool;
  // TODO: for now hardcoded, maybe change in the future
  private final int PARTITION_FACTOR = 3;
  private final int READ_CONSENSUS_NUMBER = 2;
  private final int WRITE_CONSENSUS_NUMBER = 2;

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

  public Optional<VersionedValue> getValue(final String key) {

    final HashSet<Node> nodes;
    List<Optional<VersionedValue>> results = new ArrayList<>();

    try {
      nodes = hashRing.determineNodesForKey(key,
          PARTITION_FACTOR);
    } catch (final NotEnoughNodesException ex) {
      return Optional.empty();
    }

    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<Optional<VersionedValue>>> futures = nodes.stream()
          .map(node -> executor.submit(() -> clientPool.get(node).get(key)))
          .collect(Collectors.toList());

      for (Future<Optional<VersionedValue>> future : futures) {
        try {
          results.add(future.get(5, TimeUnit.SECONDS));

        } catch (InterruptedException ex) {

          Thread.currentThread().interrupt();
          break;

        } catch (TimeoutException | ExecutionException ex) {

        }
      }

    }

    if (results.size() < READ_CONSENSUS_NUMBER) {
      return Optional.empty();
    }

    int mostRecentIndex = -1;

    for (int i = 0; i < results.size(); i++) {

      Optional<VersionedValue> value = results.get(i);

      if (value.isEmpty()) {
        continue;
      }

      if (mostRecentIndex == -1) {
        mostRecentIndex = i;
      } else if (results.get(mostRecentIndex).get().getVersion() < value.get().getVersion()) {
        mostRecentIndex = i;
      }
    }

    if (mostRecentIndex == -1) {
      return Optional.empty();
    }

    return Optional.of(results.get(mostRecentIndex).get());

  }

  // public void putValue(final String key, final byte[] value, long version)
  // throws EmptyRingException {
  // // Here I have to write to all and wait for w to confirm
  // // final Node node = hashRing.determineNodeForKey(key);
  // // final KVClient client = clientPool.get(node);
  // client.put(key, value, version);
  // }
  //
  // public void deleteValue(final String key) throws EmptyRingException {
  // // Here I have to write to all and wait for w to confirm
  // // final Node node = hashRing.determineNodeForKey(key);
  // final KVClient client = clientPool.get(node);
  // client.delete(key);
  // }

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
