package com.kvstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

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

}
