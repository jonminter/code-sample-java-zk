package dev.jonminter.distributedmergesort;

import dev.jonminter.distributedmergesort.employees.EmployeeLookupTableBuilder;
import dev.jonminter.distributedmergesort.employees.EmployeeLookupTableBuilder.CannotModifyFrozenLookupTablesException;
import dev.jonminter.distributedmergesort.employees.EmployeeLookupTables;
import dev.jonminter.distributedmergesort.employees.output.EmployeeWithDepartmentAndManager;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.x.async.AsyncCuratorFramework;

/**
 * Setters aren't thread safe, they should only be called from main method and state machine thread
 * Getters are used in gRCP server thread as well but only using the sorted employees queue
 * which is created when app initializes
 */
@NotThreadSafe
public class DistMergeSortContext {
  public static class LeaderSelectionNotRunYetException extends RuntimeException {
    public LeaderSelectionNotRunYetException() {
      super("Cannot retrieve leader selection results as we have not elected a leader yet!");
    }
  }

  private static final Logger logger = Logger.getLogger(DistMergeSortContext.class.getName());

  public DistMergeSortContext(
      DistMergeSortConfig config,
      CuratorFramework curatorClient,
      GroupMember nodeInfo,
      PriorityQueue<EmployeeWithDepartmentAndManager> sortedEmployees,
      EmployeeLookupTableBuilder employeeLookupTableBuilder,
      EmployeeLookupTables employeeLookupTables) {
    this.config = config;
    this.curatorClient = curatorClient;
    this.nodeInfo = nodeInfo;
    this.asyncCuratorClient = AsyncCuratorFramework.wrap(curatorClient);
    this.sortedEmployees = sortedEmployees;
    this.employeeLookupTableBuilder = employeeLookupTableBuilder;
    this.employeeLookupTables = employeeLookupTables;

    this.resourcesToCloseWhenDone.add(curatorClient);
    this.resourcesToCloseWhenDone.add(nodeInfo);
  }

  public CuratorFramework getCuratorClient() {
    return curatorClient;
  }

  public AsyncCuratorFramework getAsyncCuratorClient() {
    return asyncCuratorClient;
  }

  public GroupMember getNodeInfo() {
    return nodeInfo;
  }

  public List<String> getOtherClusterNodeNames() {
    return nodeInfo
        .getCurrentMembers()
        .keySet()
        .stream()
        .filter(n -> !n.equals(config.getMyNodeName()))
        .collect(Collectors.toList());
  }

  public LeaderLatch getLeaderLatch() {
    if (leaderLatch == null) {
      throw new LeaderSelectionNotRunYetException();
    }
    return leaderLatch;
  }

  public void setLeaderLatch(LeaderLatch leaderLatch) {
    this.leaderLatch = leaderLatch;
    this.resourcesToCloseWhenDone.add(leaderLatch);
  }

  public DistMergeSortConfig getConfig() {
    return config;
  }

  public PriorityQueue<EmployeeWithDepartmentAndManager> getSortedEmployees() {
    return sortedEmployees;
  }

  public EmployeeLookupTableBuilder getEmployeeLookupTableBuilder()
      throws CannotModifyFrozenLookupTablesException {
    if (lookupTableBuilt) {
      throw new EmployeeLookupTableBuilder.CannotModifyFrozenLookupTablesException();
    }
    return employeeLookupTableBuilder;
  }

  public EmployeeLookupTables getEmployeeLookupTables() {
    return employeeLookupTables;
  }

  public void releaseCloseables() {
    // Close gRPC channels
    openChannels.forEach((k, v) -> {
      try {
        v.shutdown().awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        System.out.println(String.format("Failed closing channel %s with error %s", k, e));
      }
    });

    // Close closeable resources (Curator etc) do it in reverse order theyre added
    for (ListIterator<Closeable> it = resourcesToCloseWhenDone.listIterator(); it.hasPrevious(); ) {
      Closeable closeable = it.previous();
      System.out.println(String.format("Closing resource %s", closeable));
      try {
        closeable.close();
      } catch (IOException e) {
        System.out.println(String.format("Failed closing resource with error: %s", e.toString()));
      }
    }
  }

  public void startDistMergeSort() {
    postLeaderElectionStartLatch.countDown();
  }

  /**
   * Once this flag is set no longer allow access to lookup table builder
   */
  public void finishedBuildingLookupTables() {
    this.lookupTableBuilt = true;
    this.employeeLookupTableBuilder.freeze();
  }

  public CountDownLatch getPostLeaderElectionStartLatch() {
    return postLeaderElectionStartLatch;
  }

  public ManagedChannel getChannelToNode(String nodeName) {
    openChannels.computeIfAbsent(nodeName, n -> ManagedChannelBuilder
          .forAddress(n, config.getGrpcPort())
          .usePlaintext()
          .build());
    return openChannels.get(nodeName);
  }

  private final DistMergeSortConfig config;
  private final CuratorFramework curatorClient;
  private final AsyncCuratorFramework asyncCuratorClient;
  private final GroupMember nodeInfo;
  private LeaderLatch leaderLatch;
  private Map<String, ManagedChannel> openChannels = new HashMap<>();

  private final PriorityQueue<EmployeeWithDepartmentAndManager> sortedEmployees;
  private final EmployeeLookupTableBuilder employeeLookupTableBuilder;
  private final EmployeeLookupTables employeeLookupTables;
  private boolean lookupTableBuilt = false;

  private final CountDownLatch postLeaderElectionStartLatch = new CountDownLatch(1);

  private final List<Closeable> resourcesToCloseWhenDone = new LinkedList<>();

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private DistMergeSortConfig config;
    private CuratorFramework curatorClient;
    private PriorityQueue<EmployeeWithDepartmentAndManager> sortedEmployees;
    private EmployeeLookupTableBuilder employeeLookupTableBuilder;
    private EmployeeLookupTables employeeLookupTables;
    private GroupMember nodeInfo;

    public Builder withConfig(DistMergeSortConfig config) {
      this.config = config;
      return this;
    }

    public Builder withCuratorClient(CuratorFramework curatorClient) {
      this.curatorClient = curatorClient;
      return this;
    }

    public Builder withNodeInfo(GroupMember member) {
      this.nodeInfo = member;
      return this;
    }

    public Builder withEmployeeMinHeap(PriorityQueue<EmployeeWithDepartmentAndManager> employeeMinHeap) {
      this.sortedEmployees = employeeMinHeap;
      return this;
    }

    public Builder withEmployeeLookupTableBuilder(EmployeeLookupTableBuilder employeeLookupTableBuilder) {
      this.employeeLookupTableBuilder = employeeLookupTableBuilder;
      return this;
    }

    public Builder withEmployeeLookupTables(EmployeeLookupTables employeeLookupTables) {
      this.employeeLookupTables = employeeLookupTables;
      return this;
    }

    public DistMergeSortContext build() {
      return new DistMergeSortContext(
          config,
          curatorClient,
          nodeInfo,
          sortedEmployees,
          employeeLookupTableBuilder,
          employeeLookupTables);
    }

  }
}
