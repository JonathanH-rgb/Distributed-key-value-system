package com.kvstore.client;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.kvstore.common.Node;
import com.kvstore.common.NodeInformation;
import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;
import com.kvstore.common.exceptions.NotEnoughNodesException;
import com.kvstore.common.exceptions.WriteConsensusException;
import com.kvstore.consistenHashing.HashRingInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Multi-node client for the distributed KV store.
 * Uses a HashRing to determine which node owns a given key and routes
 * each operation to the correct node via a pool of KVClient instances.
 */
public class ClusterClient {

  private static final Logger logger = LoggerFactory.getLogger(ClusterClient.class);

  private final HashRingInterface hashRing;
  private ConcurrentHashMap<Node, KVClient> clientPool;
  private Map<Node, KVClient> hardcodedNodesToClientMap;
  private final ExecutorService repairExecutor = Executors.newVirtualThreadPerTaskExecutor();
  // TODO: for now hardcoded, maybe change in the future
  public final int PARTITION_FACTOR = 3;
  public final int READ_CONSENSUS_NUMBER = 2;
  public final int WRITE_CONSENSUS_NUMBER = 2;
  public final int TIMEOUT_LIMIT_SECS_GET = 5;
  public final int TIMEOUT_LIMIT_SECS_DELETE = 5;
  public final int TIMEOUT_LIMIT_SECS_PUT = 5;
  public final int REFRESH_RATE_NODE_INFORMATION_SECS = 3;
  public final int REFRESH_RATE_NODE_INFORMATION_DELAY_SECS = 0;
  public final int THREADS_RUNNING_REFRESH_LOOP = 1;

  private void initHardcodedNodesAndClientPool() {
    // TODO: make this configurable, maybe read from a file
    // Implement
    throw new RuntimeException("Implement me");
  }

  public ClusterClient(final HashRingInterface hashRing) {
    this.hashRing = hashRing;
    initHardcodedNodesAndClientPool();
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(THREADS_RUNNING_REFRESH_LOOP);
    scheduler.scheduleAtFixedRate(this::updateNodeInformation, REFRESH_RATE_NODE_INFORMATION_DELAY_SECS,
        REFRESH_RATE_NODE_INFORMATION_SECS,
        TimeUnit.SECONDS);
  }

  public ClusterClient(final HashRingInterface hashRing, HashMap<Node, KVClient> hardcodedNodes) {
    this.hashRing = hashRing;
    clientPool = new ConcurrentHashMap<>();
    for (Node hardcodedNode : hardcodedNodes.keySet()) {
      clientPool.put(hardcodedNode, hardcodedNodes.get(hardcodedNode));
    }
    this.hardcodedNodesToClientMap = hardcodedNodes;
  }

  private void createClientAndPutInPool(Node node) {
    final KVClient client = new KVClient(node.gethost(), node.getport());
    clientPool.put(node, client);
  }

  public Optional<VersionedValue> getValue(final String key) {
    logger.debug("GET request for key '{}'", key);

    final HashSet<Node> nodes;
    try {
      nodes = hashRing.determineNodesForKey(key, PARTITION_FACTOR);
    } catch (final NotEnoughNodesException ex) {
      return Optional.empty();
    }

    Map<Node, Optional<VersionedValue>> results = queryNodes(key, nodes);

    if (results.size() < READ_CONSENSUS_NUMBER) {
      logger.warn("GET for key '{}' did not meet read consensus: got {} responses, needed {}", key, results.size(),
          READ_CONSENSUS_NUMBER);
      return Optional.empty();
    }

    Optional<Node> optionalNodeWithHighestVersion = pickNodeWithHighestVersion(results);
    if (optionalNodeWithHighestVersion.isEmpty()) {
      return Optional.empty();
    }

    Node nodeWithHigestVersion = optionalNodeWithHighestVersion.get();
    VersionedValue value = results.get(nodeWithHigestVersion).get();

    // Take nodes with diff value that latest and update
    List<Node> nodesWithDifferentValueThanLatest = results.keySet().stream()
        .filter(node -> !value.equals(results.get(node).orElse(null)))
        .toList();

    updateOldNodesWithNewValAsync(value, nodesWithDifferentValueThanLatest, key);
    return Optional.of(value);
  }

  private Map<Node, Optional<VersionedValue>> queryNodes(String key, HashSet<Node> nodes) {
    Map<Node, Optional<VersionedValue>> results = new HashMap<>();
    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      Map<Future<Optional<VersionedValue>>, Node> futureToNode = new HashMap<>();
      nodes.forEach(node -> futureToNode.put(
          executor.submit(() -> clientPool.get(node).get(key)), node));

      for (Map.Entry<Future<Optional<VersionedValue>>, Node> entry : futureToNode.entrySet()) {
        try {
          results.put(entry.getValue(), entry.getKey().get(TIMEOUT_LIMIT_SECS_GET, TimeUnit.SECONDS));
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          break;
        } catch (TimeoutException | ExecutionException ex) {
          logger.error("GET request for key '{}' failed on node {}", key, entry.getValue(), ex);
        }
      }
    }
    return results;
  }

  private Optional<Node> pickNodeWithHighestVersion(Map<Node, Optional<VersionedValue>> results) {
    return results.keySet().stream()
        .filter(key -> results.get(key).isPresent())
        .max((keyA, keyB) -> Long.compare(results.get(keyA).get().getVersion(),
            results.get(keyB).get().getVersion()));
  }

  private void updateOldNodesWithNewValAsync(VersionedValue latestVal, List<Node> nodesToBeUpdated,
      String key) {
    nodesToBeUpdated.forEach(node -> repairExecutor
        .submit(() -> {
          try {
            clientPool.get(node).put(key, latestVal.getBytes(), latestVal.getVersion());
          } catch (Exception ex) {
            logger.warn("Read repair failed for key={} node={}", key, node, ex);
          }
        }));
  }

  public void putValue(final String key, final byte[] value, long version)
      throws NotEnoughNodesException, WriteConsensusException {

    logger.debug("PUT request for key '{}' at version {}", key, version);

    final HashSet<Node> nodes = hashRing.determineNodesForKey(key,
        PARTITION_FACTOR);

    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

      List<Future<?>> futures = nodes.stream()
          .map(node -> executor.submit(() -> clientPool.get(node).put(key, value, version)))
          .collect(Collectors.toList());

      int successCount = 0;
      for (Future<?> future : futures) {
        try {
          future.get(TIMEOUT_LIMIT_SECS_PUT, TimeUnit.SECONDS);
          successCount++;
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          break;
        } catch (TimeoutException | ExecutionException ex) {
          logger.error("PUT request for key '{}' failed on one node", key, ex);
        }
      }

      if (successCount < WRITE_CONSENSUS_NUMBER) {
        logger.warn("PUT for key '{}' did not meet write consensus: {} nodes succeeded, needed {}", key, successCount,
            WRITE_CONSENSUS_NUMBER);
        throw new WriteConsensusException("Only " + successCount + " nodes updated the value, but "
            + WRITE_CONSENSUS_NUMBER + " were expected to update that value");
      }
    }
  }

  public void deleteValue(final String key) throws NotEnoughNodesException, WriteConsensusException {

    logger.debug("DELETE request for key '{}'", key);

    final HashSet<Node> nodes = hashRing.determineNodesForKey(key,
        PARTITION_FACTOR);

    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {

      List<Future<?>> futures = nodes.stream()
          .map(node -> executor.submit(() -> clientPool.get(node).delete(key)))
          .collect(Collectors.toList());

      int successCount = 0;
      for (Future<?> future : futures) {
        try {
          future.get(TIMEOUT_LIMIT_SECS_DELETE, TimeUnit.SECONDS);
          successCount++;
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          break;
        } catch (TimeoutException | ExecutionException ex) {
          logger.error("DELETE request for key '{}' failed on one node", key, ex);
        }
      }

      if (successCount < WRITE_CONSENSUS_NUMBER) {
        logger.warn("DELETE for key '{}' did not meet write consensus: {} nodes succeeded, needed {}", key,
            successCount, WRITE_CONSENSUS_NUMBER);
        throw new WriteConsensusException("Only " + successCount + " nodes deleted the value, but "
            + WRITE_CONSENSUS_NUMBER + " were expected to deleted that value");
      }

    }
  }

  private void addNode(final Node node) throws NodeAlreadyInRingException {
    hashRing.addNode(node);
    createClientAndPutInPool(node);
    logger.info("Node {} added to the hash ring", node);
  }

  private void removeNode(final Node node) throws NodeNotInRingException {
    hashRing.removeNode(node);
    KVClient client = clientPool.get(node);
    client.shutdown();
    clientPool.remove(node);
    logger.info("Node {} removed from the hash ring", node);
  }

  private void updateNodeInformation() {

    HashMap<Node, NodeInformation> clusterNodeInfo = new HashMap<>();
    boolean ableToCallOneHardcodedNodeForStatus = false;

    for (Node hardcodedNodeToTryWith : hardcodedNodesToClientMap.keySet()) {
      KVClient client = hardcodedNodesToClientMap.get(hardcodedNodeToTryWith);
      try {
        clusterNodeInfo = client.viewCluster();
        ableToCallOneHardcodedNodeForStatus = true;
        break;
      } catch (Exception ex) {
        logger.warn("Unable to reach seed node {} for cluster view update", hardcodedNodeToTryWith);
      }
    }

    if (!ableToCallOneHardcodedNodeForStatus) {
      logger.warn("Unable to connect to any seed node; skipping cluster view update");
      return;
    }

    logger.info("Cluster view updated; received {} nodes", clusterNodeInfo.size());

    for (Node node : clusterNodeInfo.keySet()) {
      NodeInformation info = clusterNodeInfo.get(node);
      if (clientPool.containsKey(node) && info.getStatus() == NodeInformation.Status.DEAD) {
        try {
          removeNode(node);
        } catch (NodeNotInRingException ex) {
          // Should never throw this because we check if client.contains it
        }
      } else if (!clientPool.containsKey(node) && info.getStatus() == NodeInformation.Status.ALIVE) {
        try {
          addNode(node);
        } catch (NodeAlreadyInRingException ex) {
          // Should never throw this because we check if client.contains it
        }
      }
    }
  }

  public void shutdown() {
    repairExecutor.shutdown();
    clientPool.values().forEach(client -> client.shutdown());
  }

}
