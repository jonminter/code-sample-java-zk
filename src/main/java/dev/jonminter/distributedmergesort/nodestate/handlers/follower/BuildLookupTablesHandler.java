package dev.jonminter.distributedmergesort.nodestate.handlers.follower;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import dev.jonminter.distributedmergesort.Constants;
import dev.jonminter.distributedmergesort.DistMergeSortConfig;
import dev.jonminter.distributedmergesort.DistMergeSortContext;
import dev.jonminter.distributedmergesort.cluster.NodeFileAssignment;
import dev.jonminter.distributedmergesort.cluster.ZookeeperModels;
import dev.jonminter.distributedmergesort.employees.EmployeeLookupTableBuilder;
import dev.jonminter.distributedmergesort.employees.EmployeeLookupTableBuilder.CannotModifyFrozenLookupTablesException;
import dev.jonminter.distributedmergesort.employees.input.Department;
import dev.jonminter.distributedmergesort.employees.input.Employee;
import dev.jonminter.distributedmergesort.nodestate.AbstractDistMergeSortHandler;
import dev.jonminter.distributedmergesort.nodestate.StateMachineEventDispatcher;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FinishedBuildingLookupTablesEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FollowingLeaderEvent;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;

public class BuildLookupTablesHandler extends AbstractDistMergeSortHandler<FollowingLeaderEvent> {
  private static final Logger logger = Logger.getLogger(BuildLookupTablesHandler.class.getName());

  public BuildLookupTablesHandler(DistMergeSortConfig config,
      DistMergeSortContext context,
      StateMachineEventDispatcher dispatcher) {
    super(config, context, dispatcher);
  }


  @Override
  public void handleEvent(FollowingLeaderEvent event) throws Exception {
    logger.info("Will start building lookups once all nodes have entered local sort barrier...");
    // Wait for start signal
    GroupMember nodeInfo = context.getNodeInfo();
    DistributedDoubleBarrier localSortBarrier = new DistributedDoubleBarrier(
        context.getCuratorClient(),
        Constants.LOCAL_SORT_BARRIER_PATH,
        nodeInfo.getCurrentMembers().size());
    localSortBarrier.enter();

    DistributedDoubleBarrier buildLookupTablesBarrier = new DistributedDoubleBarrier(
        context.getCuratorClient(),
        Constants.BUILD_LOOKUP_TABLES_BARRIER_PATH,
        nodeInfo.getCurrentMembers().size() - 1);
    buildLookupTablesBarrier.enter();

    logger.info("Building lookup tables...");

    // BUILD LOOKUPS HERE
    List<Department> departments = loadDepartmentsIntoMemory();
    List<Employee> employees = loadEmployeesIntoMemory(getAssignedDataFiles());
    buildLookupTables(employees, departments);

    logger.info("Finished building lookup tables for this node");
    // Set flag so we can no longer update lookup tables after this
    context.finishedBuildingLookupTables();

    buildLookupTablesBarrier.leave();
    logger.info("All nodes have finished building lookup tables, about to perform local sort...");
    dispatcher.fire(new FinishedBuildingLookupTablesEvent(localSortBarrier, employees, departments));
  }

  private List<String> getAssignedDataFiles() throws ExecutionException, InterruptedException {
    String nodeName = config.getMyNodeName();
    ModelSpec<NodeFileAssignment> resolvedSpec = ZookeeperModels.NODE_FILE_ASSIGNMENT_SPEC.resolved(nodeName);
    ModeledFramework<NodeFileAssignment> nodeAssignmentClient = ModeledFramework.wrap(
        context.getAsyncCuratorClient(),
        resolvedSpec);
    NodeFileAssignment files = nodeAssignmentClient.read().toCompletableFuture().get();
    return files.getFiles();
  }

  private static List<Employee> parseInputFile(Gson gson, String filename) throws FileNotFoundException {
    FileReader reader = new FileReader(filename);
    return gson.fromJson(reader, new TypeToken<List<Employee>>(){}.getType());
  }

  private List<Employee> loadEmployeesIntoMemory(List<String> dataFiles) {
    Gson gson = new Gson();
    return dataFiles.stream()
        .flatMap(
            f -> {
              try {
                return parseInputFile(gson, f).stream();
              } catch (FileNotFoundException e) {
                logger.log(Level.WARNING, "File does not exist cannot open: {0}", f);
              }
              return Stream.empty();
            })
        .collect(Collectors.toList());
  }

  private List<Department> loadDepartmentsIntoMemory() throws FileNotFoundException {
    Gson gson = new Gson();
    String departmentsFile = config.getInputDataPath() + "/" + Constants.DEPARTMENTS_JSON_PATH;
    FileReader reader = new FileReader(departmentsFile);
    return gson.fromJson(reader, new TypeToken<List<Department>>(){}.getType());
  }

  private void buildLookupTables(List<Employee> employees, List<Department> departments)
      throws CannotModifyFrozenLookupTablesException {
    EmployeeLookupTableBuilder lookupTableBuilder = context.getEmployeeLookupTableBuilder();

    logger.info("Building department head lookups");
    for (Department department : departments) {
      lookupTableBuilder
          .setDepartmentHead(department.getName(), department.getDepartmentHeadEmployeeId());
    }

    logger.info("Building employee manager and name lookups");
    int numMissingManager = 0;
    for (Employee employee : employees) {
      if (employee.getManagerId() != null) {
        lookupTableBuilder.setManagerForEmployee(employee.getId(), employee.getManagerId());
      } else {
        numMissingManager++;
      }
      lookupTableBuilder.mapEmployeeIdToName(employee.getId(), employee.getName());
    }

    if (numMissingManager > 1) {
      logger.log(Level.WARNING, "Encountered more than one employee without a manager ({0}) there should only be ONE in all data files (CEO)!", numMissingManager);
    }
  }
}
