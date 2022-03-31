package dev.jonminter.distributedmergesort.nodestate.handlers.leader;

import dev.jonminter.distributedmergesort.Constants;
import dev.jonminter.distributedmergesort.DistMergeSortConfig;
import dev.jonminter.distributedmergesort.DistMergeSortContext;
import dev.jonminter.distributedmergesort.employees.EmployeeComparator;
import dev.jonminter.distributedmergesort.employees.output.EmployeeWithDepartmentAndManager;
import dev.jonminter.distributedmergesort.grpc.EmployeeTransformer;
import dev.jonminter.distributedmergesort.nodestate.AbstractDistMergeSortHandler;
import dev.jonminter.distributedmergesort.nodestate.StateMachineEventDispatcher;
import dev.jonminter.distributedmergesort.nodestate.events.leader.AllFollowersFinishedSortEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.FinishedGlobalSortEvent;
import dev.jonminter.distributedmergesort.server.DistributedMergeSortGrpc;
import dev.jonminter.distributedmergesort.server.Employee;
import dev.jonminter.distributedmergesort.server.NextBatchOfSortedEmployeesRequest;
import io.grpc.ManagedChannel;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class GlobalMergeHandler extends
    AbstractDistMergeSortHandler<AllFollowersFinishedSortEvent> {
  private static final Logger logger = Logger.getLogger(GlobalMergeHandler.class.getName());

  private static class NextEmployeeMinHeapItem implements Comparable {
    private final String fromNode;
    private final EmployeeWithDepartmentAndManager employee;
    public NextEmployeeMinHeapItem(String fromNode, EmployeeWithDepartmentAndManager employee) {
      this.fromNode = fromNode;
      this.employee = employee;
    }

    @Override
    public int compareTo(Object o) {
      if (o instanceof NextEmployeeMinHeapItem) {
        return EmployeeComparator.COMPARATOR.compare(
            this.employee, ((NextEmployeeMinHeapItem) o).employee);
      }
      return 0;
    }
  }

  public GlobalMergeHandler(DistMergeSortConfig config,
      DistMergeSortContext context,
      StateMachineEventDispatcher dispatcher) {
    super(config, context, dispatcher);
  }

  @Override
  public void handleEvent(AllFollowersFinishedSortEvent event) throws Exception {
    logger.info("Performing final merge and sort...");

    String outputFile = config.getOutputDataPath() + "/" + Constants.EMPLOYEE_OUTPUT_FILE;
    BufferedWriter sortedEmployeesFileWriter = new BufferedWriter(new FileWriter(outputFile));
    // Min heap with buffer of elements to stream to output file
    PriorityQueue<NextEmployeeMinHeapItem> employeeToFileBuffer = new PriorityQueue<>();
    // Buffers for each follower node that hold a batch of sorted items from that node
    // reduces overhead from having less requests to each node
    Map<String, LinkedList<EmployeeWithDepartmentAndManager>> employeeFromFollowersBuffers = new HashMap<>();
    List<String> followerNodeNames = context.getOtherClusterNodeNames();
    int numNonExhaustedNodes = followerNodeNames.size();
    int whichNodeNext = 0;

    logger.info("Pre-filling buffers...");
    for (String node : followerNodeNames) {
      employeeFromFollowersBuffers.put(node, new LinkedList<>());
      fetchNextBatchOfEmployeesFrom(node, employeeFromFollowersBuffers.get(node));

      employeeToFileBuffer.add(new NextEmployeeMinHeapItem(node, employeeFromFollowersBuffers
          .get(node)
          .removeFirst()));
    }

    logger.log(Level.INFO, "Merging and writing to output file {0}", outputFile);
    sortedEmployeesFileWriter.write("id,name,department,manager\n");
    while (!finished(employeeToFileBuffer, employeeFromFollowersBuffers, numNonExhaustedNodes)) {
      NextEmployeeMinHeapItem nextEmployee = employeeToFileBuffer.poll();
      sortedEmployeesFileWriter.write(String.format("%s,%s,%s,%s\n",
          nextEmployee.employee.getId(),
          nextEmployee.employee.getName(),
          nextEmployee.employee.getDepartment(),
          nextEmployee.employee.getManager()));

      String nodeToFetchNextItemFrom = nextEmployee.fromNode;

      // Check if buffer has items
      LinkedList<EmployeeWithDepartmentAndManager> buffer = employeeFromFollowersBuffers.get(nodeToFetchNextItemFrom);
      if (buffer.size() == 0) {
        //Buffer is empty, fetch next batch from follower node
        fetchNextBatchOfEmployeesFrom(nodeToFetchNextItemFrom, buffer);
      }

      if (buffer.size() != 0) {
        // Pull next item from buffer for this node and add to min heap
        employeeToFileBuffer.add(new NextEmployeeMinHeapItem(nodeToFetchNextItemFrom, buffer.removeFirst()));
      } else {
        // Buffer is still empty meaning we've exhausted this node, flag that it's exhausted
        numNonExhaustedNodes--;
      }
    }

    logger.info("Finished writing sorted output file!");
    sortedEmployeesFileWriter.close();
    logger.info("Releasing barrier so followers can quite...");
    event.getWaitForMergeToFinishBarrier().removeBarrier();

    dispatcher.fire(new FinishedGlobalSortEvent());
  }

  private boolean finished(
      PriorityQueue<NextEmployeeMinHeapItem> nodeMins,
      Map<String, LinkedList<EmployeeWithDepartmentAndManager>> nodeBuffers,
      int numNonExhaustedNodes) {
    return nodeMins.size() == 0 &&
        allBuffersExhausted(nodeBuffers) &&
        numNonExhaustedNodes == 0;
  }

  private boolean allBuffersExhausted(Map<String, LinkedList<EmployeeWithDepartmentAndManager>> nodeBuffers) {
    return nodeBuffers
        .entrySet()
        .stream()
        .allMatch(b -> b.getValue().size() == 0);
  }

  private void fetchNextBatchOfEmployeesFrom(String nodeName, LinkedList<EmployeeWithDepartmentAndManager> buffer) {
    ManagedChannel channel = context.getChannelToNode(nodeName);
    DistributedMergeSortGrpc.DistributedMergeSortBlockingStub client = DistributedMergeSortGrpc
        .newBlockingStub(channel);
    Iterator<Employee> res = client.nextBatchOfSortedEmployees(
        NextBatchOfSortedEmployeesRequest
            .newBuilder()
            .setBatchSize(Constants.MERGE_SORT_FETCH_BUFFER)
            .build());

    Stream.generate(() -> null)
        .takeWhile(x -> res.hasNext())
        .map(x -> EmployeeTransformer.fromGrpcEmployee(res.next()))
        .forEach(e -> buffer.add(e));
  }
}
