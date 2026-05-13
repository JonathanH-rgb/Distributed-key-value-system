package com.kvstore.client;

/**
 * Factory for creating GossipClient instances.
 * Abstracted as an interface so tests can inject in-process channels instead of real network connections.
 */
public interface GossipClientFactory {
  GossipClient create(String host, int port);
}
