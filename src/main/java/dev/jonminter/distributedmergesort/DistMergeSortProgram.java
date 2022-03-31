package dev.jonminter.distributedmergesort;

import dev.jonminter.distributedmergesort.Util.Tuple;
import dev.jonminter.distributedmergesort.employees.EmployeeComparator;
import dev.jonminter.distributedmergesort.employees.EmployeeLookupTableBuilder;
import dev.jonminter.distributedmergesort.employees.EmployeeLookupTables;
import dev.jonminter.distributedmergesort.employees.output.EmployeeWithDepartmentAndManager;
import dev.jonminter.distributedmergesort.grpc.DistMergeSortGrpcService;
import dev.jonminter.distributedmergesort.nodestate.NodeStateMachineV2;
import dev.jonminter.distributedmergesort.nodestate.StateMachineEventDispatcher;
import dev.jonminter.distributedmergesort.nodestate.events.JoinedGroupEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FinishedBuildingLookupTablesEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FollowingLeaderEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.AllFollowersFinishedSortEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.ElectedLeaderEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.FinishedWaitingForStragglersEvent;
import dev.jonminter.distributedmergesort.nodestate.handlers.JoinedGroupHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.follower.BuildLookupTablesHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.follower.LocalSortHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.leader.DivideWorkHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.leader.ElectedLeaderHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.leader.GlobalMergeHandler;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import reactor.util.function.Tuple2;

public class DistMergeSortProgram {
  public static void main(String[] args)
      throws Exception {
    logger.info("Starting distributed merge sort node...");
    DistMergeSortConfig config = buildConfig();
    logger.log(Level.INFO, "Joining cluster: {0}, Node name: {1}", new Object[] { config.getClusterNamespace(), config.getMyNodeName() });

    RedissonClient redissonClient = createRedissionClient(config);
    Tuple<EmployeeLookupTableBuilder, EmployeeLookupTables> lookupTables = createLookupTables(config, redissonClient);
    PriorityQueue<EmployeeWithDepartmentAndManager> sortedEmployees = new PriorityQueue<>(
        new EmployeeComparator());
    DistMergeSortContext context = startCuratorAndBuildContext(config, lookupTables, sortedEmployees);
    NodeStateMachineV2 stateMachine = buildStateMachine(config, context);
    Server grpcServer = createGrpcServer(config, context, stateMachine);

    grpcServer.start();
    logger.info("gRPC Server started, listening on " + config.getGrpcPort());

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("Attempting graceful shutdown since JVM is shutting down");
        try {
          shutdown(context, grpcServer, redissonClient);
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
        }
        System.err.println("Server shut down!");
      }
    });

    stateMachine.fire(new JoinedGroupEvent(context.getNodeInfo()));

    logger.info("Main thread waiting for state machine to complete...");
    stateMachine.getStateMachineFinishedLatch().await();
    logger.info("Finished execution shutting down...");
    shutdown(context, grpcServer, redissonClient);
    logger.info("Finished shutdown, program done!");
  }

  private static void shutdown(DistMergeSortContext context, Server grpcServer, RedissonClient redissonClient)
      throws InterruptedException {
    System.out.println("Shutting down hazelcast");
    redissonClient.shutdown();
    System.out.println("Closing all closeable resources...");
    context.releaseCloseables();
    System.out.println("Shutting down gRCP server...");
    grpcServer.shutdown().awaitTermination(30, TimeUnit.SECONDS);
    System.out.println("Finished shutdown!");
  }

  private static final Logger logger = Logger.getLogger(DistMergeSortProgram.class.getName());

  private static final Set<String> REQUIRED_ENV_VARS = new HashSet<>();
  static {
    REQUIRED_ENV_VARS.add(EnvironmentVars.MY_HOSTNAME);
    REQUIRED_ENV_VARS.add(EnvironmentVars.SORT_CLUSTER_NAMESPACE);
    REQUIRED_ENV_VARS.add(EnvironmentVars.ZOOKEEPER_HOST);
    REQUIRED_ENV_VARS.add(EnvironmentVars.ZOOKEEPER_PORT);
    REQUIRED_ENV_VARS.add(EnvironmentVars.INPUT_DATA_PATH);
    REQUIRED_ENV_VARS.add(EnvironmentVars.OUTPUT_DATA_PATH);
    REQUIRED_ENV_VARS.add(EnvironmentVars.MY_GRPC_PORT);
    REQUIRED_ENV_VARS.add(EnvironmentVars.REDIS_HOST);
    REQUIRED_ENV_VARS.add(EnvironmentVars.REDIS_PORT);
  }

  private static DistMergeSortConfig buildConfig() {
    Map<String, String> envVars = System.getenv();

    boolean hasErrors = false;
    StringBuilder errorMessageBuilder = new StringBuilder()
        .append("Missing required env vars: [");
    for (String envVar : REQUIRED_ENV_VARS) {
      if (!envVars.containsKey(envVar)) {
        hasErrors = true;
        errorMessageBuilder
            .append(envVar)
            .append(",");
      }
    };
    if (hasErrors) {
      errorMessageBuilder.append("]");
      throw new RuntimeException(errorMessageBuilder.toString());
    }

    String nodeName = envVars.get(EnvironmentVars.MY_HOSTNAME);
    String clusterNamespace = envVars.get(EnvironmentVars.SORT_CLUSTER_NAMESPACE);
    String zookeeperConnectionString = String.format("%s:%s",
        envVars.get(EnvironmentVars.ZOOKEEPER_HOST),
        envVars.get(EnvironmentVars.ZOOKEEPER_PORT));
    String inputDataPath = envVars.get(EnvironmentVars.INPUT_DATA_PATH);
    String outputDataPath = envVars.get(EnvironmentVars.OUTPUT_DATA_PATH);
    int grpcPort = Integer.parseInt(envVars.get(EnvironmentVars.MY_GRPC_PORT), 10);
    String redisHost = envVars.get(EnvironmentVars.REDIS_HOST);
    int redisPort = Integer.parseInt(envVars.get(EnvironmentVars.REDIS_PORT), 10);
    boolean debugMode = envVars.containsKey(EnvironmentVars.SORT_DEBUG_MODE) &&
        envVars.get(EnvironmentVars.SORT_DEBUG_MODE).equals("true");

    return new DistMergeSortConfig(
        nodeName,
        clusterNamespace,
        zookeeperConnectionString,
        inputDataPath,
        outputDataPath,
        grpcPort,
        redisHost,
        redisPort,
        debugMode);
  }

  private static RedissonClient createRedissionClient(DistMergeSortConfig config) {
    Config redisConfig = new Config();
    redisConfig.useSingleServer()
        .setAddress(String.format("redis://%s:%d", config.getRedisHost(), config.getRedisPort()));
    RedissonClient client = Redisson.create(redisConfig);
    return client;
  }

  private static DistMergeSortContext startCuratorAndBuildContext(
      DistMergeSortConfig config,
      Tuple<EmployeeLookupTableBuilder, EmployeeLookupTables> lookupTables,
      PriorityQueue<EmployeeWithDepartmentAndManager> sortedEmployees) {
    CuratorFramework curatorClient = CuratorFrameworkFactory
        .builder()
        .namespace(config.getClusterNamespace())
        .connectString(config.getZookeeperConnectionString())
        .retryPolicy(new ExponentialBackoffRetry(1000, 3))
        .build();
    curatorClient.start();
    GroupMember member = new GroupMember(curatorClient, Constants.GROUP_MEMBERS_PATH, config.getMyNodeName());
    member.start();

    logger.info("Joined cluster!");

    return DistMergeSortContext.newBuilder()
      .withConfig(config)
      .withCuratorClient(curatorClient)
      .withNodeInfo(member)
      .withEmployeeLookupTableBuilder(lookupTables.getT1())
      .withEmployeeLookupTables(lookupTables.getT2())
      .withEmployeeMinHeap(sortedEmployees)
      .build();
  }

  private static NodeStateMachineV2 buildStateMachine(DistMergeSortConfig config, DistMergeSortContext context) {
    StateMachineEventDispatcher dispatcher = new StateMachineEventDispatcher();
    NodeStateMachineV2 stateMachine = NodeStateMachineV2.newBuilder()
        .withEventHandler(JoinedGroupEvent.class, new JoinedGroupHandler(config, context, dispatcher))
        .withEventHandler(ElectedLeaderEvent.class, new ElectedLeaderHandler(config, context, dispatcher))
        .withEventHandler(FinishedWaitingForStragglersEvent.class, new DivideWorkHandler(config, context, dispatcher))
        .withEventHandler(AllFollowersFinishedSortEvent.class, new GlobalMergeHandler(config, context, dispatcher))
        .withEventHandler(FollowingLeaderEvent.class, new BuildLookupTablesHandler(config, context, dispatcher))
        .withEventHandler(FinishedBuildingLookupTablesEvent.class, new LocalSortHandler(config, context, dispatcher))
        .build();

    dispatcher.setStateMachine(stateMachine);
    return stateMachine;
  }

  private static Tuple<EmployeeLookupTableBuilder, EmployeeLookupTables> createLookupTables(DistMergeSortConfig  config, RedissonClient redisClient) {
    Map<String, String> distributedEmployeeManagerMap = redisClient
        .getMap(String.format(Constants.DIST_EMPLOYEE_MANAGER_MAP, config.getClusterNamespace()));

    Map<String, String> distributedEmployeeNameMap = redisClient
        .getMap(String.format(Constants.DIST_EMPLOYEE_NAME_MAP, config.getClusterNamespace()));

    Map<String, String> departmentThatEmployeeHeadsLookup = new HashMap<>();
    Map<String, String> employeeDepartmentHeadLookup = new HashMap<>();

    EmployeeLookupTableBuilder lookupTableBuilder = new EmployeeLookupTableBuilder(
        distributedEmployeeManagerMap,
        distributedEmployeeNameMap,
        departmentThatEmployeeHeadsLookup,
        employeeDepartmentHeadLookup);

    EmployeeLookupTables lookupTables = new EmployeeLookupTables(
        distributedEmployeeManagerMap,
        distributedEmployeeNameMap,
        departmentThatEmployeeHeadsLookup,
        employeeDepartmentHeadLookup);

    return new Tuple<>(lookupTableBuilder, lookupTables);
  }

  private static Server createGrpcServer(DistMergeSortConfig config, DistMergeSortContext context, NodeStateMachineV2 stateMachine) {
    int port = config.getGrpcPort();
    return ServerBuilder
        .forPort(port)
        .addService(new DistMergeSortGrpcService(context)).build();
  }
}
