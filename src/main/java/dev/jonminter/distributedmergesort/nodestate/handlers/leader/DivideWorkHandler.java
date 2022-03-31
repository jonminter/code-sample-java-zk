package dev.jonminter.distributedmergesort.nodestate.handlers.leader;

import dev.jonminter.distributedmergesort.Constants;
import dev.jonminter.distributedmergesort.DistMergeSortConfig;
import dev.jonminter.distributedmergesort.DistMergeSortContext;
import dev.jonminter.distributedmergesort.cluster.NodeFileAssignment;
import dev.jonminter.distributedmergesort.cluster.ZookeeperModels;
import dev.jonminter.distributedmergesort.nodestate.AbstractDistMergeSortHandler;
import dev.jonminter.distributedmergesort.nodestate.StateMachineEventDispatcher;
import dev.jonminter.distributedmergesort.nodestate.events.leader.AllFollowersFinishedSortEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.FinishedWaitingForStragglersEvent;
import dev.jonminter.distributedmergesort.server.DistributedMergeSortGrpc;
import dev.jonminter.distributedmergesort.server.NotifyFollowersOfLeaderRequest;
import dev.jonminter.distributedmergesort.server.NotifyFollowersOfLeaderResponse;
import io.grpc.ManagedChannel;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;

public class DivideWorkHandler extends
    AbstractDistMergeSortHandler<FinishedWaitingForStragglersEvent> {
  public static class FailedAssigningFilesToNodesException extends Exception {
    public FailedAssigningFilesToNodesException(String message, Exception cause) {
      super(message, cause);
    }
  }

  private static final Logger logger = Logger.getLogger(DivideWorkHandler.class.getName());

  public DivideWorkHandler(
      DistMergeSortConfig config,
      DistMergeSortContext context,
      StateMachineEventDispatcher dispatcher) {
    super(config, context, dispatcher);
  }

  @Override
  public void handleEvent(FinishedWaitingForStragglersEvent event) throws Exception {
    logger.info("Starting to divide work...");
    // Find out how many nodes we have in cluster besides leader
    GroupMember nodeInfo = context.getNodeInfo();
    List<String> nodeNames = context.getOtherClusterNodeNames();

    for (String nodeName : nodeNames) {
      logger.log(Level.INFO, "Notifying follower {0} that we are leader...", nodeName);
      ManagedChannel channel = context.getChannelToNode(nodeName);
      DistributedMergeSortGrpc.DistributedMergeSortBlockingStub client = DistributedMergeSortGrpc
          .newBlockingStub(channel);
      NotifyFollowersOfLeaderResponse res = client.notifyFollowerOfLeader(NotifyFollowersOfLeaderRequest
          .newBuilder()
          .setLeaderNode(config.getMyNodeName())
          .build());

      logger.log(Level.INFO, "Done. On to next... Ack: {0}", res.getAck());
    }

    int nodeCount = nodeInfo.getCurrentMembers().size() - 1;
    logger.log(Level.INFO, "Splitting files over {0} nodes", nodeCount);

    // Count how many files on data volume
    List<String> allEmployeeDataFiles = getEmployeeDataFiles();

    context.getCuratorClient()
        .create()
        .orSetData()
        .forPath(Constants.NODE_FILE_ASSIGNMENTS_ROOT_PATH);

    // Split amongst nodes
    int fileCount = allEmployeeDataFiles.size();
    long filesPerNode = (long)Math.ceil((double)fileCount / nodeCount);
    logger.log(Level.INFO, "{0} total files and sending {1} files to each node", new Object[] {fileCount, filesPerNode});
    AtomicInteger counter = new AtomicInteger(0);
    AtomicBoolean failedAssigningFiles = new AtomicBoolean(false);
    AtomicReference<Exception> exception = new AtomicReference<>();
    BiConsumer<Long, List<String>> assignFilesToNode = (i,s) -> {
      String nodeName = nodeNames.get(i.intValue());
      logger.log(Level.INFO, "#{2} - Assigning {0} files to node {1}", new Object[] {s, nodeName, i});
      try {

        ModelSpec<NodeFileAssignment> resolvedSpec = ZookeeperModels.NODE_FILE_ASSIGNMENT_SPEC.resolved(nodeName);
        ModeledFramework<NodeFileAssignment> nodeAssignmentClient = ModeledFramework.wrap(
            context.getAsyncCuratorClient(),
            resolvedSpec);
        nodeAssignmentClient.set(new NodeFileAssignment(s));
      } catch (Exception e) {
        exception.set(e);
        logger.log(Level.SEVERE, "Failed creating node assignments node! {0}", e);
        failedAssigningFiles.set(true);
      }
    };
    allEmployeeDataFiles
        .stream()
        .collect(Collectors.groupingBy(s -> counter.getAndIncrement()/filesPerNode))
        .forEach(assignFilesToNode);

    if (failedAssigningFiles.get()) {
      throw new FailedAssigningFilesToNodesException("Failure creating node assignments znode", exception.get());
    }

    logger.info("Creating start barrier, followers should start once all have entered...");

    // Create Double Barrier for followers to signal they are starting/finishing local sort
    DistributedDoubleBarrier localSortBarrier = new DistributedDoubleBarrier(
        context.getCuratorClient(),
        Constants.LOCAL_SORT_BARRIER_PATH,
        nodeInfo.getCurrentMembers().size());

    DistributedBarrier waitForMergeToFinishBarrier = new DistributedBarrier(
        context.getCuratorClient(),
        Constants.WAIT_FOR_MERGE_BARRIER_PATH);
    waitForMergeToFinishBarrier.setBarrier();

    localSortBarrier.enter();
    logger.info("Leader entered local sort barrier, waiting for followers to all leave...");
    // This only happens once all followers leave i.e. have sorted their slice of data
    localSortBarrier.leave();
    logger.info("Followers have all finished their local sort.");

    dispatcher.fire(new AllFollowersFinishedSortEvent(waitForMergeToFinishBarrier));
  }

  private List<String> getEmployeeDataFiles() throws IOException {
    logger.log(Level.INFO, "Root path: {0}, glob pattern: {1}", new Object[] {config.getInputDataPath(), Constants.EMPLOYEES_JSON_GLOB});
    PathMatcher matcher = FileSystems.getDefault().getPathMatcher(Constants.EMPLOYEES_JSON_GLOB);
    List<String> dataFiles = new LinkedList<>();

    Files.walkFileTree(Paths.get(config.getInputDataPath()), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path path,
          BasicFileAttributes attrs) throws IOException {
        logger.log(Level.INFO, "Testing file {0}", path.getFileName());
        if (matcher.matches(path)) {
          logger.info("Found match to glob!");
          dataFiles.add(path.toString());
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFileFailed(Path file, IOException exc)
          throws IOException {
        return FileVisitResult.CONTINUE;
      }
    });

    return dataFiles;
  }
}
