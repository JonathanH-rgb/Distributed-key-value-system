package com.kvstore;

import org.junit.jupiter.api.BeforeEach;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyHardcodedNodesListException;
import com.kvstore.server.KVServer;

public class KVServerTest {

  public final int NUMBER_OF_SERVERS = 5;
  private Node[] nodes;
  private KVServer[] servers;

  @BeforeEach
  public void init() throws EmptyHardcodedNodesListException {

    // String serverName = "test-server";
    //
    // server = InProcessServerBuilder
    // .forName(serverName)
    // .addService(new KVServer())
    // .build()
    // .start();
    //
    // ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverName)
    // .build();
    //
    // client = new KVClient(managedChannel);

    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      nodes[i] = new Node("localhost" + i, 8083 + i);
    }

    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      Node[] otherNodes = new Node[NUMBER_OF_SERVERS - 1];
      for (int j = 0; j < NUMBER_OF_SERVERS; j++) {
        if (i != j) {
          otherNodes[j] = nodes[j];
        }
      }

      servers[i] = new KVServer(nodes[i].gethost(), nodes[i].getport(), otherNodes);

    }
  }
}
