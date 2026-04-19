package com.kvstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.client.GossipClient;
import com.kvstore.common.Node;
import com.kvstore.common.NodeInformation;
import com.kvstore.server.KVServer;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class GossipClientKVServerIntegrationTest {

  private GossipClient client;
  private Server server;

  public GossipClientKVServerIntegrationTest() {
  }

  @BeforeEach
  public void setup() throws Exception {

    String serverName = "test-server";

    server = InProcessServerBuilder
        .forName(serverName)
        .addService(new KVServer())
        .build()
        .start();

    ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverName)
        .build();

    client = new GossipClient(managedChannel);

  }

  @AfterEach
  public void teardown() {
    client.shutdown();
    server.shutdown();
  }

  @Test
  public void gossipShouldReturnTest() {
    HashMap<Node, NodeInformation> nodeToNodeInfo = new HashMap<>();
    Node testNode = new Node("test1", 1);
    NodeInformation testNodeInfo = new NodeInformation(NodeInformation.Status.ALIVE, 1, 0);
    nodeToNodeInfo.put(testNode, testNodeInfo);
    HashMap<Node, NodeInformation> returnedNodeInfo = client.gossip(nodeToNodeInfo);
    assertTrue(returnedNodeInfo.containsKey(testNode));
    assertEquals(returnedNodeInfo.get(testNode).getHeartBeatCounter(), 1);
    assertEquals(returnedNodeInfo.get(testNode).getStatus(), NodeInformation.Status.ALIVE);
  }

  @Test
  public void gossipHigherHeartShouldRemain() {
    HashMap<Node, NodeInformation> nodeToNodeInfo = new HashMap<>();
    Node testNode1 = new Node("test1", 1);
    NodeInformation testNodeInfo1 = new NodeInformation(NodeInformation.Status.ALIVE, 10, 0);
    nodeToNodeInfo.put(testNode1, testNodeInfo1);
    HashMap<Node, NodeInformation> returnedNodeInfo = client.gossip(nodeToNodeInfo);
    testNodeInfo1 = new NodeInformation(NodeInformation.Status.ALIVE, 1, 0);
    nodeToNodeInfo.put(testNode1, testNodeInfo1);
    returnedNodeInfo = client.gossip(nodeToNodeInfo);
    assertTrue(returnedNodeInfo.containsKey(testNode1));
    assertEquals(returnedNodeInfo.get(testNode1).getHeartBeatCounter(), 10);
    assertEquals(returnedNodeInfo.get(testNode1).getStatus(), NodeInformation.Status.ALIVE);
  }

}
