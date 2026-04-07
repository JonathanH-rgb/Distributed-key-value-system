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
import java.util.ArrayList;
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

  private void initHardcodedNodesAndClients() {
    // TODO: make this configurable, maybe read from a file
    hardcodedNodesToClientMap = new HashMap<>();
    for (int i = 0; i < 1; i++) {
      String host = "localhost";
      int port = 9090 + i;
      Node node = new Node(host, port);
      KVClient client = new KVClient(host, port);
      hardcodedNodesToClientMap.put(node, client);
    }
  }

  public ClusterClient(final HashRingInterface hashRing) {
    this.hashRing = hashRing;
    this.clientPool = new ConcurrentHashMap<>();
    initHardcodedNodesAndClients();
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(THREADS_RUNNING_REFRESH_LOOP);
    scheduler.scheduleAtFixedRate(this::updateNodeInformation, REFRESH_RATE_NODE_INFORMATION_DELAY_SECS,
        REFRESH_RATE_NODE_INFORMATION_SECS,
        TimeUnit.SECONDS);
  }

  public ClusterClient(final HashRingInterface hashRing, ConcurrentHashMap<Node, KVClient> clientPool) {
    this.hashRing = hashRing;
    this.clientPool = clientPool;
  }

  private void createClientAndPutInPool(Node node) {
    final KVClient client = new KVClient(node.gethost(), node.getport());
    clientPool.put(node, client);
  }

  public Optional<VersionedValue> getValue(final String key) {

    logger.debug("GET request for key '{}'", key);

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

          results.add(future.get(TIMEOUT_LIMIT_SECS_GET, TimeUnit.SECONDS));

        } catch (InterruptedException ex) {

          Thread.currentThread().interrupt();
          break;

        } catch (TimeoutException | ExecutionException ex) {
          logger.error("GET request for key '{}' failed on one node", key, ex);
        }
      }

    }

    if (results.size() < READ_CONSENSUS_NUMBER) {
      logger.warn("GET for key '{}' did not meet read consensus: got {} responses, needed {}", key, results.size(), READ_CONSENSUS_NUMBER);
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
        logger.warn("PUT for key '{}' did not meet write consensus: {} nodes succeeded, needed {}", key, successCount, WRITE_CONSENSUS_NUMBER);
        throw new WriteConsensusException("Only " + successCount + " nodes updated the value, but "
            + WRITE_CONSENSUS_NUMBER + " were expected to updated that value");
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
        logger.warn("DELETE for key '{}' did not meet write consensus: {} nodes succeeded, needed {}", key, successCount, WRITE_CONSENSUS_NUMBER);
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

}
