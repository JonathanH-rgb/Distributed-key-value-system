package com.kvstore.client;

import java.io.IOException;
import java.util.Properties;

/**
 * Configuration for the ClusterClient.
 * Loaded from client.properties on the classpath in production,
 * or constructed directly with values for tests.
 */
public class ClientConfig {

  public final int PARTITION_FACTOR;
  public final int READ_CONSENSUS_NUMBER;
  public final int WRITE_CONSENSUS_NUMBER;
  public final int TIMEOUT_LIMIT_SECS_GET;
  public final int TIMEOUT_LIMIT_SECS_DELETE;
  public final int TIMEOUT_LIMIT_SECS_PUT;
  public final int REFRESH_RATE_NODE_INFORMATION_SECS;
  public final int REFRESH_RATE_NODE_INFORMATION_DELAY_SECS;
  public final int THREADS_RUNNING_REFRESH_LOOP;

  public ClientConfig() throws IOException {
    Properties properties = new Properties();
    properties.load(getClass().getClassLoader().getResourceAsStream("config.properties"));
    PARTITION_FACTOR = Integer.parseInt(properties.getProperty("client.partition.factor"));
    READ_CONSENSUS_NUMBER = Integer.parseInt(properties.getProperty("client.read.consensus"));
    WRITE_CONSENSUS_NUMBER = Integer.parseInt(properties.getProperty("client.write.consensus"));
    TIMEOUT_LIMIT_SECS_GET = Integer.parseInt(properties.getProperty("client.timeout.get.secs"));
    TIMEOUT_LIMIT_SECS_DELETE = Integer.parseInt(properties.getProperty("client.timeout.delete.secs"));
    TIMEOUT_LIMIT_SECS_PUT = Integer.parseInt(properties.getProperty("client.timeout.put.secs"));
    REFRESH_RATE_NODE_INFORMATION_SECS = Integer.parseInt(properties.getProperty("client.refresh.rate.secs"));
    REFRESH_RATE_NODE_INFORMATION_DELAY_SECS = Integer
        .parseInt(properties.getProperty("client.refresh.delay.secs"));
    THREADS_RUNNING_REFRESH_LOOP = Integer.parseInt(properties.getProperty("client.refresh.threads"));
  }

  public ClientConfig(int partitionFactor, int readConsensusNumber, int writeConsensusNumber,
      int timeoutLimitSecsGet, int timeoutLimitSecsDelete, int timeoutLimitSecsPut,
      int refreshRateNodeInformationSecs, int refreshRateNodeInformationDelaySecs,
      int threadsRunningRefreshLoop) {
    this.PARTITION_FACTOR = partitionFactor;
    this.READ_CONSENSUS_NUMBER = readConsensusNumber;
    this.WRITE_CONSENSUS_NUMBER = writeConsensusNumber;
    this.TIMEOUT_LIMIT_SECS_GET = timeoutLimitSecsGet;
    this.TIMEOUT_LIMIT_SECS_DELETE = timeoutLimitSecsDelete;
    this.TIMEOUT_LIMIT_SECS_PUT = timeoutLimitSecsPut;
    this.REFRESH_RATE_NODE_INFORMATION_SECS = refreshRateNodeInformationSecs;
    this.REFRESH_RATE_NODE_INFORMATION_DELAY_SECS = refreshRateNodeInformationDelaySecs;
    this.THREADS_RUNNING_REFRESH_LOOP = threadsRunningRefreshLoop;
  }

}
