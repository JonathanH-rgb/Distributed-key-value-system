package com.kvstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.client.GossipClient;
import com.kvstore.client.GossipClientFactory;
import com.kvstore.client.KVClient;
import com.kvstore.common.Node;
import com.kvstore.common.NodeInformation;
import com.kvstore.common.exceptions.EmptyHardcodedNodesListException;
import com.kvstore.server.ClusterConfig;
import com.kvstore.server.KVServer;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class SeverGossipsIntegrationTest {

  private final int NUMBER_OF_SERVERS = 5;
  private Server[] servers = new Server[NUMBER_OF_SERVERS];
  private KVServer[] kvServers = new KVServer[NUMBER_OF_SERVERS];
  private KVClient[] kvClients = new KVClient[NUMBER_OF_SERVERS];
  private String[] serverNames = new String[NUMBER_OF_SERVERS];
  private Node[] nodes = new Node[NUMBER_OF_SERVERS];

  private class TestGossipClientFactory implements GossipClientFactory {
    @Override
    public GossipClient create(String host, int port) {
      ManagedChannel managedChannel = InProcessChannelBuilder.forName(host + port)
          .build();
      return new GossipClient(managedChannel);
    }
  }

  public SeverGossipsIntegrationTest() {
  }

  @BeforeEach
  public void setup() throws Exception {

    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      String host = "localHost" + i;
      int port = 8080 + i;
      serverNames[i] = host + port;
      nodes[i] = new Node(host, port);
    }

    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      final int index = i;
      Node[] otherNodes = Arrays.stream(nodes)
          .filter(n -> !nodes[index].equals(n))
          .toArray(Node[]::new);
      // fanout=NUMBER_OF_SERVERS-1 ensures all nodes always selected (no randomness)
      // timeout=1, delay=0, freq=1, maxAttempts=3
      ClusterConfig testConfig = new ClusterConfig(NUMBER_OF_SERVERS - 1, 1, 0, 1, 3);
      kvServers[i] = new KVServer(nodes[i].gethost(), nodes[i].getport(),
          otherNodes, new TestGossipClientFactory(), testConfig);
      servers[i] = InProcessServerBuilder
          .forName(serverNames[i])
          .addService(kvServers[i])
          .build()
          .start();

      ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverNames[i])
          .build();
      kvClients[i] = new KVClient(managedChannel);
    }

    // Start gossip only after all in-process servers are up
    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      kvServers[i].startGossip();
    }
  }

  @AfterEach
  public void teardown() throws InterruptedException {
    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      kvClients[i].shutdown();
      kvServers[i].shutdown();
      servers[i].shutdown();
      servers[i].awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  public void allNodesShouldBeAliveAfterGossipRounds() throws InterruptedException {
    Thread.sleep(kvServers[0].GOSSIP_OTHER_SERVERS_FREQ_SECS * 1000 * 2);

    HashMap<Node, NodeInformation> clusterView = kvClients[0].viewCluster();

    assertEquals(NUMBER_OF_SERVERS, clusterView.size());
    for (Map.Entry<Node, NodeInformation> entry : clusterView.entrySet()) {
      assertEquals(NodeInformation.Status.ALIVE, entry.getValue().getStatus());
      assertTrue(entry.getValue().getHeartBeatCounter() > 1);
    }
  }

  @Test
  public void deadNodeShouldBeMarkedDeadAfterGossipRounds() throws InterruptedException {
    // Let gossip stabilize first
    Thread.sleep(kvServers[0].GOSSIP_OTHER_SERVERS_FREQ_SECS * 1000 * 2);

    // Shut down server 0 to simulate a failure
    servers[0].shutdown();
    servers[0].awaitTermination(5, TimeUnit.SECONDS);

    // Wait long enough for MAX_GOSSIP_ATTEMPTS rounds to pass
    long amoutTowait = 1000
        * ((kvServers[0].GOSSIP_TIMEOUT_SECS * kvServers[0].MAX_GOSSIP_ATTEMPS) + kvServers[0].GOSSIP_LOOP_DELAY_SECS
            + 1);
    Thread.sleep(amoutTowait);

    // Observe from server 1's perspective
    HashMap<Node, NodeInformation> clusterView = kvClients[1].viewCluster();

    assertEquals(NodeInformation.Status.DEAD, clusterView.get(nodes[0]).getStatus());
  }

  @Test
  public void suspectToAliveNodeShouldBeUpdatedInClusterTest()
      throws InterruptedException, EmptyHardcodedNodesListException, IOException {
    // Let gossip stabilize first
    Thread.sleep(kvServers[0].GOSSIP_OTHER_SERVERS_FREQ_SECS * 1000 * 2);

    // Shut down server 0 to simulate a failure
    final int index = 0;
    servers[index].shutdown();
    servers[index].awaitTermination(5, TimeUnit.SECONDS);

    // Wait for exactly 1 gossip round (+ buffer) to catch SUSPECT before DEAD
    Thread.sleep(kvServers[0].GOSSIP_OTHER_SERVERS_FREQ_SECS * 1000 + 500);

    // Observe from server 1's perspective
    HashMap<Node, NodeInformation> clusterView = kvClients[1].viewCluster();
    if (kvServers[0].MAX_GOSSIP_ATTEMPS == 1) {
      assertEquals(NodeInformation.Status.DEAD, clusterView.get(nodes[index]).getStatus());
    } else {
      assertEquals(NodeInformation.Status.SUSPECT, clusterView.get(nodes[index]).getStatus());
    }

    Node[] otherNodes = Arrays.stream(nodes)
        .filter(n -> !nodes[index].equals(n))
        .toArray(Node[]::new);
    ClusterConfig testConfig = new ClusterConfig(3, 1, 0, 1, 2);
    kvServers[index] = new KVServer(nodes[index].gethost(), nodes[index].getport(),
        otherNodes, new TestGossipClientFactory(), testConfig);
    servers[index] = InProcessServerBuilder
        .forName(serverNames[index])
        .addService(kvServers[index])
        .build()
        .start();
    kvServers[index].startGossip();

    Thread.sleep(kvServers[0].GOSSIP_OTHER_SERVERS_FREQ_SECS * 1000 * 2);
    // update
    clusterView = kvClients[1].viewCluster();
    assertEquals(NodeInformation.Status.ALIVE, clusterView.get(nodes[index]).getStatus());
    assertTrue(clusterView.get(nodes[index]).getHeartBeatCounter() > 0);
  }

}
