package com.kvstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyRingException;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;
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
    Node node = new Node(host, port);
    hashRing.addNode(node);
    assertThrows(NodeAlreadyInRingException.class, () -> hashRing.addNode(node));
  }

  @Test
  public void getNodeWithNoNodesExceptionTest() {
    String key = "key";
    assertThrows(EmptyRingException.class, () -> hashRing.determineNodeForKey(key));
  }

  @Test
  public void removeNodeThatIsNotInTheRingExceptionTest() {
    Node node = new Node(host, port);
    assertThrows(NodeNotInRingException.class, () -> hashRing.removeNode(node));
  }

  @Test
  public void addMultipleNodeTest() throws NodeAlreadyInRingException {
    final int EXPECTED_NUMBER_NODES = 2;
    Node node1 = new Node(host + "1", port);
    hashRing.addNode(node1);
    Node node2 = new Node(host + "2", port);
    hashRing.addNode(node2);
    assertEquals(hashRing.getCopyOfNodesInRing().size(), EXPECTED_NUMBER_NODES);
  }

  @Test
  public void removeNodeTest() throws NodeAlreadyInRingException, NodeNotInRingException {
    final int EXPECTED_NUMBER_NODES = 0;
    Node node = new Node(host, port);
    hashRing.addNode(node);
    hashRing.removeNode(node);
    assertEquals(hashRing.getCopyOfNodesInRing().size(), EXPECTED_NUMBER_NODES);
  }

  @Test
  public void getNodeTest() throws NodeAlreadyInRingException, EmptyRingException {
    Node node = new Node(host, port);
    hashRing.addNode(node);
    String key = "key";
    Node providedNode = hashRing.determineNodeForKey(key);
    assertEquals(node, providedNode);
  }

  @Test
  public void checkDistributionOfKeys() throws NodeAlreadyInRingException, EmptyRingException {

    int numberOfVirtualNodes = 150;
    this.hashRing = new HashRing(numberOfVirtualNodes);
    int numberOfNodes = 4;
    int numberOfKeys = 10_000;

    int expectedKeysPerNode = numberOfKeys / numberOfNodes;
    int lowerExpectedKeysPerNode = (int) (expectedKeysPerNode * 0.5);
    int upperExpectedKeysPerNode = (int) (expectedKeysPerNode * 1.5);

    HashMap<Node, Integer> keysPerNode = new HashMap<>();
    Node[] nodes = new Node[numberOfNodes];

    for (int i = 0; i < numberOfNodes; i++) {
      String host = "host" + i;
      int port = 8088 + i;
      nodes[i] = new Node(host, port);
      keysPerNode.put(nodes[i], 0);
      hashRing.addNode(nodes[i]);
    }

    for (int i = 0; i < numberOfKeys; i++) {
      String key = "key" + i;
      Node node = hashRing.determineNodeForKey(key);
      keysPerNode.put(node, keysPerNode.get(node) + 1);
    }

    for (Node node : keysPerNode.keySet()) {
      int keysInThisNode = keysPerNode.get(node);
      assertTrue(keysInThisNode >= lowerExpectedKeysPerNode
          && keysInThisNode <= upperExpectedKeysPerNode);
    }
  }

}
