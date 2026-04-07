package com.kvstore.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.kvstore.client.GossipClient;
import com.kvstore.common.Node;
import com.kvstore.common.NodeInformation;
import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.EmptyHardcodedNodesListException;
import com.kvstore.proto.KVStoreGrpc;
import com.kvstore.proto.KVStoreProto.ClusterViewRequest;
import com.kvstore.proto.KVStoreProto.ClusterViewResponse;
import com.kvstore.proto.KVStoreProto.DeleteRequest;
import com.kvstore.proto.KVStoreProto.DeleteResponse;
import com.kvstore.proto.KVStoreProto.GetRequest;
import com.kvstore.proto.KVStoreProto.GetResponse;
import com.kvstore.proto.KVStoreProto.GossipRequest;
import com.kvstore.proto.KVStoreProto.GossipResponse;
import com.kvstore.proto.KVStoreProto.PingRequest;
import com.kvstore.proto.KVStoreProto.PingResponse;
import com.kvstore.proto.KVStoreProto.PutRequest;
import com.kvstore.proto.KVStoreProto.PutResponse;
import com.kvstore.storage.InMemoryStore;
import com.kvstore.storage.StorageEngine;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gRPC service implementation for the KV store.
 * Handles Get, Put, and Delete RPCs from clients and delegates to a
 * StorageEngine.
 */
public class KVServer extends KVStoreGrpc.KVStoreImplBase {

  private static final Logger logger = LoggerFactory.getLogger(KVServer.class);

  private Server server;
  private ConcurrentHashMap<Node, NodeInformation> nodeToNodeInformationMap;
  private ConcurrentHashMap<Node, GossipClient> nodeToGossipClientMap;
  private ConcurrentHashMap<Node, Integer> nodeToFailedGossipAttemps;
  private StorageEngine storageEngine;
  public final static int FANOUT_FACTOR = 3;
  private final Node serverNode;
  public final static int GOSSIP_TIMEOUT_SECS = 5;
  public final static int THREADS_RUNNING_GOSSIP_LOOP = 1;
  public final static int GOSSIP_LOOP_DELAY = 0;
  public final static int GOSSIP_OTHER_SERVERS_FREQ = 1;
  public final static int MAX_GOSSIP_ATTEMPS = 3;

  public KVServer() throws EmptyHardcodedNodesListException {
    this.storageEngine = new InMemoryStore();
    nodeToNodeInformationMap = new ConcurrentHashMap<>();
    nodeToFailedGossipAttemps = new ConcurrentHashMap<>();
    nodeToGossipClientMap = new ConcurrentHashMap<>();
    this.serverNode = null;
  }

  public KVServer(String serverHost, int serverPort, Node[] hardcodeNodes) throws EmptyHardcodedNodesListException {

    if (hardcodeNodes.length == 0) {
      throw new EmptyHardcodedNodesListException("Provided hardcoded nodes list can not be empty");
    }

    nodeToNodeInformationMap = new ConcurrentHashMap<>();
    nodeToFailedGossipAttemps = new ConcurrentHashMap<>();
    nodeToGossipClientMap = new ConcurrentHashMap<>();

    this.serverNode = new Node(serverHost, serverPort);
    this.storageEngine = new InMemoryStore();
    populateHardcodedNodes(hardcodeNodes);
  }

  public void start(int portNumber) {
    server = ServerBuilder.forPort(portNumber)
        .addService(this)
        .build();
    try {
      server.start();
      logger.info("KVServer started on port {}", portNumber);
      ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(THREADS_RUNNING_GOSSIP_LOOP);
      scheduler.scheduleAtFixedRate(this::gossipOtherRandomServers, GOSSIP_LOOP_DELAY, GOSSIP_OTHER_SERVERS_FREQ,
          TimeUnit.SECONDS);
      server.awaitTermination();
      logger.info("KVServer stopped on port {}", portNumber);
    } catch (IOException ioEx) {
      logger.error("Failed to start server on port {}", portNumber, ioEx);
      throw new RuntimeException("Failed to start server on port " + portNumber, ioEx);
    } catch (InterruptedException iEx) {
      logger.error("Server interrupted while awaiting termination on port {}", portNumber, iEx);
      throw new RuntimeException("Server interrupted", iEx);
    }
  }

  private void populateHardcodedNodes(Node[] hardcodedNodes) {

    NodeInformation thisServerInfo = new NodeInformation(NodeInformation.Status.ALIVE, 1);
    nodeToNodeInformationMap.put(serverNode, thisServerInfo);

    for (int i = 0; i < hardcodedNodes.length; i++) {
      Node hardcodedNode = hardcodedNodes[i];
      if (hardcodedNode.gethost().equals(serverNode.gethost()) && hardcodedNode.getport() == serverNode.getport()) {
        continue;
      }
      NodeInformation newNodeInformation = new NodeInformation(NodeInformation.Status.ALIVE, 0);
      nodeToNodeInformationMap.put(hardcodedNode, newNodeInformation);
    }
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    logger.debug("GET request for key '{}'", request.getKey());
    GetResponse getResponse;
    try {
      Optional<VersionedValue> optionalValue = storageEngine.get(request.getKey());
      if (optionalValue.isPresent()) {
        getResponse = GetResponse
            .newBuilder()
            .setKey(request.getKey())
            .setValue(ByteString.copyFrom(optionalValue.get().getBytes()))
            .setFound(true)
            .setVersion(optionalValue.get().getVersion())
            .build();
      } else {
        getResponse = GetResponse
            .newBuilder()
            .setKey(request.getKey())
            .setFound(false)
            .build();
      }
      responseObserver.onNext(getResponse);
      responseObserver.onCompleted();
    } catch (Exception ex) {
      logger.error("Error handling GET request for key '{}'", request.getKey(), ex);
      responseObserver.onError(ex);
    }
  }

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    logger.debug("PUT request for key '{}' at version {}", request.getKey(), request.getVersion());
    try {
      storageEngine.put(request.getKey(), request.getValue().toByteArray(),
          request.getVersion());
      PutResponse putResponse = PutResponse.newBuilder()
          .setSuccessful(true)
          .build();
      responseObserver.onNext(putResponse);
      responseObserver.onCompleted();
    } catch (Exception ex) {
      logger.error("Error handling PUT request for key '{}'", request.getKey(), ex);
      responseObserver.onError(ex);
    }
  }

  @Override
  public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
    logger.debug("DELETE request for key '{}'", request.getKey());
    try {
      storageEngine.delete(request.getKey());
      DeleteResponse deleteResponse = DeleteResponse.newBuilder()
          .setSuccessful(true)
          .build();
      responseObserver.onNext(deleteResponse);
      responseObserver.onCompleted();
    } catch (Exception ex) {
      logger.error("Error handling DELETE request for key '{}'", request.getKey(), ex);
      responseObserver.onError(ex);
    }
  }

  @Override
  public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
    try {
      responseObserver.onNext(PingResponse.newBuilder().build());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      logger.error("Error handling PING request", ex);
      responseObserver.onError(ex);
    }
  }

  private List<com.kvstore.proto.KVStoreProto.NodeInformation> createProtoNodeInformationWithNodeInformation(
      ConcurrentHashMap<Node, NodeInformation> nodeInformationMap) {

    return nodeInformationMap
        .entrySet().stream()
        .map(entry -> com.kvstore.proto.KVStoreProto.NodeInformation.newBuilder()
            .setNode(com.kvstore.proto.KVStoreProto.Node.newBuilder()
                .setHost(entry.getKey().gethost())
                .setPort(entry.getKey().getport())
                .build())

            .setStatus(com.kvstore.proto.KVStoreProto.NodeStatus.valueOf(entry.getValue().getStatus().name()))
            .setHeartBeatCounter(entry.getValue().getHeartBeatCounter())
            .build())
        .collect(Collectors.toList());

  }

  @Override
  public void viewCluster(ClusterViewRequest request, StreamObserver<ClusterViewResponse> responseObserver) {
    try {

      // Format this node info to send to requesting node
      List<com.kvstore.proto.KVStoreProto.NodeInformation> responseNodes = createProtoNodeInformationWithNodeInformation(
          nodeToNodeInformationMap);

      ClusterViewResponse clusterViewResponse = ClusterViewResponse.newBuilder()
          .addAllNodes(responseNodes)
          .build();

      logger.info("Cluster view requested; responding with {} nodes", responseNodes.size());
      // Send info
      responseObserver.onNext(clusterViewResponse);
      responseObserver.onCompleted();
    } catch (Exception ex) {
      logger.error("Error handling viewCluster request", ex);
      responseObserver.onError(ex);
    }
  }

  @Override
  public void gossip(GossipRequest request, StreamObserver<GossipResponse> responseObserver) {
    logger.debug("Received gossip message with {} node entries", request.getNodesCount());
    try {
      // Merge request info into this node
      request.getNodesList().forEach(protoNode -> {
        Node node = new Node(protoNode.getNode().getHost(), protoNode.getNode().getPort());
        NodeInformation incoming = new NodeInformation(
            NodeInformation.Status.valueOf(protoNode.getStatus().name()),
            protoNode.getHeartBeatCounter());
        nodeToNodeInformationMap.merge(node, incoming,
            (existing, newVal) -> newVal.getHeartBeatCounter() > existing.getHeartBeatCounter() ? newVal : existing);
      });

      // Format this node info to send to requesting node
      List<com.kvstore.proto.KVStoreProto.NodeInformation> responseNodes = createProtoNodeInformationWithNodeInformation(
          nodeToNodeInformationMap);
      GossipResponse gossipResponse = GossipResponse.newBuilder()
          .addAllNodes(responseNodes)
          .build();

      // Send info
      responseObserver.onNext(gossipResponse);
      responseObserver.onCompleted();
    } catch (Exception ex) {
      logger.error("Error handling gossip request", ex);
      responseObserver.onError(ex);
    }
  }

  public void gossipOtherRandomServers() {

    // Increase my counter before sending messages
    long newHeartBeat = nodeToNodeInformationMap.get(serverNode).getHeartBeatCounter() + 1;
    nodeToNodeInformationMap.get(serverNode).setHeartBeatCounter(newHeartBeat);
    logger.debug("Incremented heartbeat counter for {} to {}", serverNode, newHeartBeat);

    // select random nodes safely
    List<Node> aliveNodes = new ArrayList<>(nodeToNodeInformationMap.keySet())
        .stream().filter(n -> nodeToNodeInformationMap.get(n).getStatus() != NodeInformation.Status.DEAD)
        .collect(Collectors.toList());
    Collections.shuffle(aliveNodes);
    List<Node> selectedNodes = aliveNodes.subList(0, Math.min(FANOUT_FACTOR, aliveNodes.size()));

    logger.debug("Starting gossip round; selected {} target nodes", selectedNodes.size());

    // Make sure nodes have gossip client, if not create
    selectedNodes.stream().forEach(n -> {
      if (!nodeToGossipClientMap.containsKey(n)) {
        GossipClient gossipClient = new GossipClient(n.gethost(), n.getport());
        nodeToGossipClientMap.put(n, gossipClient);
      }
    });

    List<HashMap<Node, NodeInformation>> gossipMessages = new ArrayList<>();

    // Async request to other nodes
    HashMap<Future<HashMap<Node, NodeInformation>>, Node> futureToNodeMap = new HashMap<>();
    try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
      List<Future<HashMap<Node, NodeInformation>>> futures = selectedNodes
          .stream()
          .map(n -> executor.submit(() -> {
            GossipClient client = nodeToGossipClientMap.get(n);
            HashMap<Node, NodeInformation> normalMap = new HashMap<>(nodeToNodeInformationMap);
            return client.gossip(normalMap);
          }))
          .collect(Collectors.toList());

      // Is same order? I think yes but might be a problem in future
      for (int i = 0; i < futures.size(); i++) {
        futureToNodeMap.put(futures.get(i), selectedNodes.get(i));
      }

      // Unpack into list
      for (Future<HashMap<Node, NodeInformation>> future : futures) {
        Node node = futureToNodeMap.get(future);
        try {
          gossipMessages.add(future.get(GOSSIP_TIMEOUT_SECS, TimeUnit.SECONDS));
          nodeToNodeInformationMap.get(node).setStatus(NodeInformation.Status.ALIVE);
          if (nodeToFailedGossipAttemps.containsKey(node)) {
            nodeToFailedGossipAttemps.put(node, 0);
          }
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
          break;
        } catch (TimeoutException | ExecutionException ex) {
          int failedAttempts = nodeToFailedGossipAttemps.getOrDefault(node, 0) + 1;
          nodeToFailedGossipAttemps.put(node, failedAttempts);
          logger.warn("Gossip to node {} failed (attempt {}/{})", node, failedAttempts, MAX_GOSSIP_ATTEMPS);
          if (nodeToFailedGossipAttemps.get(node) >= MAX_GOSSIP_ATTEMPS) {
            nodeToNodeInformationMap.get(node).setStatus(NodeInformation.Status.DEAD);
            logger.warn("Node {} marked as DEAD after {} failed gossip attempts", node, failedAttempts);
          } else {
            nodeToNodeInformationMap.get(node).setStatus(NodeInformation.Status.SUSPECT);
          }
        }
      }
    }

    // Iterate over responses, update node information in this server with gossips
    for (HashMap<Node, NodeInformation> message : gossipMessages) {
      for (Node currentNode : message.keySet()) {
        NodeInformation currentNodeInfo = message.get(currentNode);
        if (!nodeToNodeInformationMap.containsKey(currentNode)) {
          nodeToNodeInformationMap.put(currentNode, currentNodeInfo);
          logger.info("Discovered new node {} via gossip; added to cluster view", currentNode);
        } else {
          long heartBeatRegisteredInThisServer = nodeToNodeInformationMap.get(currentNode).getHeartBeatCounter();
          long heartBeatInThisGossip = message.get(currentNode).getHeartBeatCounter();
          if (heartBeatInThisGossip > heartBeatRegisteredInThisServer) {
            nodeToNodeInformationMap.put(currentNode, currentNodeInfo);
          }
        }
      }
    }

  }

}
