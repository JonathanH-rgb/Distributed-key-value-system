package com.kvstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
      kvServers[i] = new KVServer(nodes[i].gethost(), nodes[i].getport(),
          otherNodes, new TestGossipClientFactory());
      kvServers[i].startGossip();
      servers[i] = InProcessServerBuilder
          .forName(serverNames[i])
          .addService(kvServers[i])
          .build()
          .start();

      ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverNames[i])
          .build();
      kvClients[i] = new KVClient(managedChannel);
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
    Thread.sleep(KVServer.GOSSIP_OTHER_SERVERS_FREQ * 1000 * 2);

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
    Thread.sleep(KVServer.GOSSIP_OTHER_SERVERS_FREQ * 1000 * 2);

    // Shut down server 0 to simulate a failure
    servers[0].shutdown();
    servers[0].awaitTermination(5, TimeUnit.SECONDS);

    // Wait long enough for MAX_GOSSIP_ATTEMPTS rounds to pass
    long amoutTowait = 1000
        * ((KVServer.GOSSIP_TIMEOUT_SECS * KVServer.MAX_GOSSIP_ATTEMPS)
            * +KVServer.GOSSIP_LOOP_DELAY + 1);
    Thread.sleep(amoutTowait);

    // Observe from server 1's perspective
    HashMap<Node, NodeInformation> clusterView = kvClients[1].viewCluster();

    assertEquals(NodeInformation.Status.DEAD, clusterView.get(nodes[0]).getStatus());
  }

}
