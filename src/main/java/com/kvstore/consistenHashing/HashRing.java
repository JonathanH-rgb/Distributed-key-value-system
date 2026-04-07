package com.kvstore.consistenHashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.NodeAlreadyInRingException;
import com.kvstore.common.exceptions.NodeNotInRingException;
import com.kvstore.common.exceptions.NotEnoughNodesException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consistent hash ring for distributing keys across cluster nodes.
 * Each physical node is assigned multiple positions on the ring (virtual nodes)
 * to ensure even key distribution. Given a key, returns the nearest node
 * clockwise.
 */
public class HashRing implements HashRingInterface {

  private static final Logger logger = LoggerFactory.getLogger(HashRing.class);

  private final int virtualNodes;

  public int getVirtualNodes() {
    return virtualNodes;
  }

  private final TreeMap<Long, VirtualNode> virtualNodeMap = new TreeMap<>();

  private final Set<Node> nodesSet = new HashSet<>();

  public static final int MIN_NUMBER_OF_VIRTUAL_NODES = 5;

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

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
      logger.error("MD5 algorithm not available", ex);
      throw new RuntimeException(ex);
    }
    final byte[] digest = messageDigest.digest(arg.getBytes());
    long hash = 0;
    for (int i = 0; i < 8; i++) {
      hash = (hash << 8) | (digest[i] & 0xFF);
    }
    return hash;
  }

  private String createVirtualNodeIdentifier(final String host, final int port, final int index) {
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
    lock.writeLock().lock();
    try {
      if (nodesSet.contains(node)) {
        throw new NodeAlreadyInRingException("Node: " + node.toString() + "already in the ring");
      }
      createVirtualNodes(node);
      nodesSet.add(node);
      logger.info("Node {} added to the hash ring ({} virtual nodes)", node, virtualNodes);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public HashSet<Node> determineNodesForKey(final String key, int partitionFactor)
      throws NotEnoughNodesException {
    lock.readLock().lock();

    try {

      if (nodesSet.size() < partitionFactor) {
        throw new NotEnoughNodesException(
            "The ring has " + nodesSet.size() + " nodes but " + partitionFactor + " where requested");
      }

      HashSet<Node> nodes = new HashSet<>();
      long currentNodeHash = -1;
      long keyHash = -1;
      int i = 0;

      while (nodes.size() < partitionFactor) {
        if (i == 0) {
          keyHash = computeHashForRing(key);
        } else {
          keyHash = currentNodeHash + 1;
        }

        if (virtualNodeMap.ceilingEntry(keyHash) != null) {
          currentNodeHash = virtualNodeMap.ceilingKey(keyHash);
        } else {
          currentNodeHash = virtualNodeMap.firstKey();
        }

        Node node = virtualNodeMap.get(currentNodeHash).getNodeReference();
        // Remember we have virtual nodes, so we have to check for duplicates
        if (nodes.contains(node)) {
          continue;
        } else {
          nodes.add(node);
          i++;
        }

      }

      return nodes;

    } finally {
      lock.readLock().unlock();
    }
  }

  public void removeNode(final Node node) throws NodeNotInRingException {
    lock.writeLock().lock();
    try {
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
      logger.info("Node {} removed from the hash ring", node);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<Node> getCopyOfNodesInRing() {
    // TODO: make this method return a copy so client can't change the nodes?
    // for now it returns the set
    lock.readLock().lock();
    try {
      return nodesSet;
    } finally {
      lock.readLock().unlock();
    }
  }

}
