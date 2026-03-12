package com.kvstore.consistenHashing;

import java.util.Set;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyRingException;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;

public interface HashRingInterface {

  public void addNode(final Node node) throws NodeAlreadyInRingException;

  public Node determineNodeForKey(String key) throws EmptyRingException;

  public void removeNode(Node node) throws NodeNotInRingException;

  public Set<Node> getCopyOfNodesInRing();

}
