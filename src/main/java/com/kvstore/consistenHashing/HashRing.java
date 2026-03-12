package com.kvstore.consistenHashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyRingException;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;

public class HashRing implements HashRingInterface {

  private final int virtualNodes;

  public int getVirtualNodes() {
    return virtualNodes;
  }

  private final TreeMap<Long, VirtualNode> virtualNodeMap = new TreeMap<>();

  private final Set<Node> nodesSet = new HashSet<>();

  public static final int MIN_NUMBER_OF_VIRTUAL_NODES = 5;

  private class VirtualNode {

    private final Node nodeReference;
    private final int index;

    public VirtualNode(final Node nodeReference, final int index) {
      this.nodeReference = nodeReference;
      this.index = index;
    }

    public int getindex() {
      return index;
    }

    public Node getNodeReference() {
      return nodeReference;
    }
  }

  public HashRing(final int virtualNodes) {
    if (virtualNodes < MIN_NUMBER_OF_VIRTUAL_NODES) {
      throw new IllegalArgumentException("Please assign at least " + MIN_NUMBER_OF_VIRTUAL_NODES + " virtual nodes");
    }
    this.virtualNodes = virtualNodes;
  }

  private long computeHashForRing(final String arg) {
    MessageDigest messageDigest;
    try {
      messageDigest = MessageDigest.getInstance("MD5");
    } catch (final NoSuchAlgorithmException ex) {
      throw new RuntimeException(ex);
    }
    final byte[] digest = messageDigest.digest(arg.getBytes());
    long hash = 0;
    for (int i = 0; i < 8; i++) {
      hash = (hash << 8) | (digest[i] & 0xFF);
    }
    return hash;
  }

  private String createVirtualNodeIdentifier(final String host, int port, int index) {
    return host + ":" + port + "-" + index;
  }

  private void createVirtualNodes(final Node node) {
    for (int i = 0; i < virtualNodes; i++) {
      final VirtualNode virtualNode = new VirtualNode(node, i);
      final String virtualNodeIdentifier = createVirtualNodeIdentifier(virtualNode.getNodeReference().gethost(),
          virtualNode.getNodeReference().getport(), virtualNode.getindex());
      final long virtualNodeHash = computeHashForRing(virtualNodeIdentifier);
      this.virtualNodeMap.put(virtualNodeHash, virtualNode);
    }
  }

  public void addNode(final Node node) throws NodeAlreadyInRingException {
    if (nodesSet.contains(node)) {
      throw new NodeAlreadyInRingException("Node: " + node.toString() + "already in the ring");
    }
    createVirtualNodes(node);
    nodesSet.add(node);
  }

  public Node determineNodeForKey(String key) throws EmptyRingException {
    long keyHash = computeHashForRing(key);
    long nodeHash;
    if (nodesSet.size() == 0) {
      throw new EmptyRingException("No node has been added to the ring");
    }
    if (virtualNodeMap.ceilingEntry(keyHash) != null) {
      nodeHash = virtualNodeMap.ceilingKey(keyHash);
    } else {
      nodeHash = virtualNodeMap.firstKey();
    }
    return virtualNodeMap.get(nodeHash).getNodeReference();
  }

  public void removeNode(Node node) throws NodeNotInRingException {
    if (!nodesSet.contains(node)) {
      throw new NodeNotInRingException("Node: " + node.toString() + "isn't in the ring");
    }
    for (int i = 0; i < virtualNodes; i++) {
      final String virtualNodeIdentifier = createVirtualNodeIdentifier(node.gethost(),
          node.getport(), i);
      final long virtualNodeHash = computeHashForRing(virtualNodeIdentifier);
      virtualNodeMap.remove(virtualNodeHash);
    }
    nodesSet.remove(node);
  }

  public Set<Node> getCopyOfNodesInRing() {
    // TODO: make this method return a copy so client can't change the nodes?
    // for now it returns the set
    return nodesSet;
  }

}
