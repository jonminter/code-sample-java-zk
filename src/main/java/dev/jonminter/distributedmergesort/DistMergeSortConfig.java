package dev.jonminter.distributedmergesort;

public class DistMergeSortConfig {
  private final String myNodeName;
  private final String clusterNamespace;
  private final String zookeeperConnectionString;
  private final String inputDataPath;
  private final String outputDataPath;
  private final String redisHost;
  private final int redisPort;
  private final int grpcPort;
  private boolean debugMode;

  public DistMergeSortConfig(
      String myNodeName,
      String clusterNamespace,
      String zookeeperConnectionString,
      String inputDataPath,
      String outputDataPath,
      int grpcPort,
      String redisHost,
      int redisPort,
      boolean debugMode) {
    this.myNodeName = myNodeName;
    this.clusterNamespace = clusterNamespace;
    this.zookeeperConnectionString = zookeeperConnectionString;
    this.inputDataPath = inputDataPath;
    this.outputDataPath = outputDataPath;
    this.grpcPort = grpcPort;
    this.redisHost = redisHost;
    this.redisPort = redisPort;
    this.debugMode = debugMode;
  }

  public String getMyNodeName() {
    return myNodeName;
  }

  public String getClusterNamespace() {
    return clusterNamespace;
  }

  public String getZookeeperConnectionString() {
    return zookeeperConnectionString;
  }

  public String getInputDataPath() {
    return inputDataPath;
  }

  public String getOutputDataPath() {
    return outputDataPath;
  }

  public int getGrpcPort() {
    return grpcPort;
  }

  public String getRedisHost() {
    return redisHost;
  }

  public int getRedisPort() {
    return redisPort;
  }

  public boolean inDebugMode() {
    return debugMode;
  }

}
