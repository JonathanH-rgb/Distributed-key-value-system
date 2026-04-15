package com.kvstore;

import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import com.kvstore.client.GossipClient;
import com.kvstore.client.GossipClientFactory;
import com.kvstore.common.Node;
import com.kvstore.server.KVServer;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class SeverGossipsIntegrationTest {

  private final int NUMBER_OF_SERVERS = 5;
  private Server[] servers = new Server[NUMBER_OF_SERVERS];
  private KVServer[] kvServers = new KVServer[NUMBER_OF_SERVERS];
  private String[] serverNames = new String[NUMBER_OF_SERVERS];
  private Node[] nodes = new Node[NUMBER_OF_SERVERS];

  private class TestGossipClientFactory implements GossipClientFactory {
    @Override
    public GossipClient create(String host, int port) {
      // Here we have to use the same convention to name the server as in serverNames
      // or won't work
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
      Node[] otherNodes = Arrays.stream(
          nodes).filter(n -> !nodes[index].equals(n))
          .toArray(Node[]::new);
      kvServers[i] = new KVServer(nodes[i].gethost(), nodes[i].getport(),
          otherNodes, new TestGossipClientFactory());
      servers[i] = InProcessServerBuilder
          .forName(serverNames[i])
          .addService(kvServers[i])
          .build()
          .start();
    }
  }

  @AfterEach
  public void teardown() {
    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      servers[i].shutdown();
      kvServers[i].shutdown();
    }
  }

}
