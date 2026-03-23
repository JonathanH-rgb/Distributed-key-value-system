package com.kvstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;
import com.kvstore.common.exceptions.NotEnoughNodesException;
import com.kvstore.consistenHashing.HashRing;

public class HashRingTest {

  private HashRing hashRing;
  private final int NUMBER_OF_VIRTUAL_NODES = HashRing.MIN_NUMBER_OF_VIRTUAL_NODES;
  private final String host = "localhost";
  private final int port = 8099;

  @BeforeEach
  public void createHashRingInstance() {
    this.hashRing = new HashRing(NUMBER_OF_VIRTUAL_NODES);
  }

  @Test
  public void addNodeThatAlreadyExistsExceptionTest() throws NodeAlreadyInRingException {
    final Node node = new Node(host, port);
    hashRing.addNode(node);
    assertThrows(NodeAlreadyInRingException.class, () -> hashRing.addNode(node));
  }

  @Test
  public void getNodeWithNoNodesExceptionTest() {
    final String key = "key";
    final int PARTITION_FACTOR = 1;
    assertThrows(NotEnoughNodesException.class, () -> hashRing.determineNodesForKey(key, PARTITION_FACTOR));
  }

  @Test
  public void getNodeWithNotEnoughNodesExceptionTest() throws NodeAlreadyInRingException {
    final String key = "key";
    final int PARTITION_FACTOR = 2;
    final Node node = new Node(host, port);
    hashRing.addNode(node);
    assertThrows(NotEnoughNodesException.class, () -> hashRing.determineNodesForKey(key, PARTITION_FACTOR));
  }

  @Test
  public void removeNodeThatIsNotInTheRingExceptionTest() {
    final Node node = new Node(host, port);
    assertThrows(NodeNotInRingException.class, () -> hashRing.removeNode(node));
  }

  @Test
  public void addMultipleNodeTest() throws NodeAlreadyInRingException {
    final int EXPECTED_NUMBER_NODES = 2;
    final Node node1 = new Node(host + "1", port);
    hashRing.addNode(node1);
    final Node node2 = new Node(host + "2", port);
    hashRing.addNode(node2);
    assertEquals(hashRing.getCopyOfNodesInRing().size(), EXPECTED_NUMBER_NODES);
  }

  @Test
  public void removeNodeTest() throws NodeAlreadyInRingException, NodeNotInRingException {
    final int EXPECTED_NUMBER_NODES = 0;
    final Node node = new Node(host, port);
    hashRing.addNode(node);
    hashRing.removeNode(node);
    assertEquals(hashRing.getCopyOfNodesInRing().size(), EXPECTED_NUMBER_NODES);
  }

  @Test
  public void getNodeTest() throws NodeAlreadyInRingException, NotEnoughNodesException {
    final Node node = new Node(host, port);
    final int PARTITION_FACTOR = 1;
    hashRing.addNode(node);
    final String key = "key";
    final Node providedNode = hashRing.determineNodesForKey(key, PARTITION_FACTOR)
        .stream().findFirst().get();
    assertEquals(node, providedNode);
  }

  @Test
  public void checkDistributionOfKeys() throws NodeAlreadyInRingException, NotEnoughNodesException {

    final int numberOfVirtualNodes = 150;
    this.hashRing = new HashRing(numberOfVirtualNodes);
    final int numberOfNodes = 4;
    final int numberOfKeys = 10_000;
    final int PARTITION_FACTOR = 1;

    final int expectedKeysPerNode = numberOfKeys / numberOfNodes;
    final int lowerExpectedKeysPerNode = (int) (expectedKeysPerNode * 0.5);
    final int upperExpectedKeysPerNode = (int) (expectedKeysPerNode * 1.5);

    final HashMap<Node, Integer> keysPerNode = new HashMap<>();
    final Node[] nodes = new Node[numberOfNodes];

    for (int i = 0; i < numberOfNodes; i++) {
      final String host = "host" + i;
      final int port = 8088 + i;
      nodes[i] = new Node(host, port);
      keysPerNode.put(nodes[i], 0);
      hashRing.addNode(nodes[i]);
    }

    for (int i = 0; i < numberOfKeys; i++) {
      final String key = "key" + i;
      final Node node = hashRing.determineNodesForKey(key, PARTITION_FACTOR)
          .stream().findFirst().get();
      keysPerNode.put(node, keysPerNode.get(node) + 1);
    }

    for (final Node node : keysPerNode.keySet()) {
      final int keysInThisNode = keysPerNode.get(node);
      assertTrue(keysInThisNode >= lowerExpectedKeysPerNode
          && keysInThisNode <= upperExpectedKeysPerNode);
    }
  }

}
