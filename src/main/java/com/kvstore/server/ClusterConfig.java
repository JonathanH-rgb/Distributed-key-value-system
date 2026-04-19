package com.kvstore.server;

import java.io.IOException;
import java.util.Properties;

/**
 * Gossip configuration for a server node.
 * Loaded from cluster.properties on the classpath in production,
 * or constructed directly with values for tests.
 */
public class ClusterConfig {

  public final int FANOUT_FACTOR;
  public final int GOSSIP_TIMEOUT_SECS;
  public final int GOSSIP_LOOP_DELAY_SECS;
  public final int GOSSIP_OTHER_SERVERS_FREQ_SECS;
  public final int MAX_GOSSIP_ATTEMPS;

  public ClusterConfig() throws IOException {
    Properties properties = new Properties();
    properties.load(getClass().getClassLoader().getResourceAsStream("cluster.properties"));
    FANOUT_FACTOR = Integer.parseInt(properties.getProperty("FANOUT_FACTOR"));
    GOSSIP_TIMEOUT_SECS = Integer.parseInt(properties.getProperty("GOSSIP_TIMEOUT_SECS"));
    GOSSIP_LOOP_DELAY_SECS = Integer.parseInt(properties.getProperty("GOSSIP_LOOP_DELAY_SECS"));
    GOSSIP_OTHER_SERVERS_FREQ_SECS = Integer.parseInt(properties.getProperty("GOSSIP_OTHER_SERVERS_FREQ_SECS"));
    MAX_GOSSIP_ATTEMPS = Integer.parseInt(properties.getProperty("MAX_GOSSIP_ATTEMPS"));
  }

  public ClusterConfig(int fanoutFactor, int gossipTimeoutSecs, int gossipLoopDelaySecs,
      int gossipOtherServersFreqSecs, int maxGossipAttempts) {
    this.FANOUT_FACTOR = fanoutFactor;
    this.GOSSIP_TIMEOUT_SECS = gossipTimeoutSecs;
    this.GOSSIP_LOOP_DELAY_SECS = gossipLoopDelaySecs;
    this.GOSSIP_OTHER_SERVERS_FREQ_SECS = gossipOtherServersFreqSecs;
    this.MAX_GOSSIP_ATTEMPS = maxGossipAttempts;
  }

}
