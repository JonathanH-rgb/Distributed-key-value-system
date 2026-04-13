package com.kvstore.client;

public interface GossipClientFactory {
  GossipClient create(String host, int port);
}
