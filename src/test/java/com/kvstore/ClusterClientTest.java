package com.kvstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.client.ClusterClient;
import com.kvstore.client.KVClient;
import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyRingException;
import com.kvstore.consistenHashing.HashRing;
import com.kvstore.server.KVServer;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class ClusterClientTest {

  private final int NUMBER_OF_SERVERS = 4;
  private final int NUMBER_VIRTUAL_NODES = 10;
  private Server[] servers = new Server[4];
  private KVClient[] clients = new KVClient[4];
  private ClusterClient clusterClient;

  public ClusterClientTest() {
  }

  @BeforeEach
  public void setup() throws Exception {

    String serverName;
    ConcurrentHashMap<Node, KVClient> clientPool = new ConcurrentHashMap<>();
    HashRing hashRing = new HashRing(NUMBER_VIRTUAL_NODES);

    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {

      serverName = "test-server-" + i;
      servers[i] = InProcessServerBuilder
          .forName(serverName)
          .addService(new KVServer())
          .build()
          .start();

      ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverName)
          .build();

      String nodeName = "node" + i;
      int portNumber = 8080 + i;

      clients[i] = new KVClient(managedChannel);
      Node node = new Node(nodeName, portNumber);
      clientPool.put(node, clients[i]);
      hashRing.addNode(node);
    }

    clusterClient = new ClusterClient(hashRing, clientPool);
  }

  @AfterEach
  public void teardown() {
    for (int i = 0; i < NUMBER_OF_SERVERS; i++) {
      servers[i].shutdown();
      clients[i].shutdown();
    }
  }

  @Test
  public void putAndGetTest() throws EmptyRingException {
    String key = "key1";
    byte[] value = "value".getBytes();
    clusterClient.putValue(key, value);
    assertArrayEquals(value, clusterClient.getValue(key).get().getBytes());
  }

  @Test
  public void deleteTest() throws EmptyRingException {
    String key = "key1";
    clusterClient.putValue(key, "value".getBytes());
    clusterClient.deleteValue(key);
    assertTrue(clusterClient.getValue(key).isEmpty());
  }

  @Test
  public void deleteDoesNotAffectOtherKeysTest() throws EmptyRingException {
    String key1 = "key1";
    String key2 = "key2";
    byte[] value2 = "value2".getBytes();
    clusterClient.putValue(key1, "value1".getBytes());
    clusterClient.putValue(key2, value2);
    clusterClient.deleteValue(key1);
    assertArrayEquals(value2, clusterClient.getValue(key2).get().getBytes());
  }
}
