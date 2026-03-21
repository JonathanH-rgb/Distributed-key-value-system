package com.kvstore.consistenHashing;

import java.util.HashSet;
import java.util.Set;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;
import com.kvstore.common.exceptions.NotEnoughNodesException;

public interface HashRingInterface {

  public void addNode(final Node node) throws NodeAlreadyInRingException;

  public HashSet<Node> determineNodesForKey(String key, int numberOfNodes) throws NotEnoughNodesException;

  public void removeNode(Node node) throws NodeNotInRingException;

  public Set<Node> getCopyOfNodesInRing();

}
