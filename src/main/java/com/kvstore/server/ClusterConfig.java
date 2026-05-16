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
    properties.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
    FANOUT_FACTOR = Integer.parseInt(properties.getProperty("cluster.fanout.factor"));
    GOSSIP_TIMEOUT_SECS = Integer.parseInt(properties.getProperty("cluster.gossip.timeout.secs"));
    GOSSIP_LOOP_DELAY_SECS = Integer.parseInt(properties.getProperty("cluster.gossip.loop.delay.secs"));
    GOSSIP_OTHER_SERVERS_FREQ_SECS = Integer.parseInt(properties.getProperty("cluster.gossip.freq.secs"));
    MAX_GOSSIP_ATTEMPS = Integer.parseInt(properties.getProperty("cluster.gossip.max.attempts"));
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
