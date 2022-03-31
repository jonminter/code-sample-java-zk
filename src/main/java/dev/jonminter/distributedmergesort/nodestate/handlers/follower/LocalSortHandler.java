package dev.jonminter.distributedmergesort.nodestate.handlers.follower;

import dev.jonminter.distributedmergesort.Constants;
import dev.jonminter.distributedmergesort.DistMergeSortConfig;
import dev.jonminter.distributedmergesort.DistMergeSortContext;
import dev.jonminter.distributedmergesort.employees.EmployeeLookupTables;
import dev.jonminter.distributedmergesort.employees.input.Department;
import dev.jonminter.distributedmergesort.employees.input.Employee;
import dev.jonminter.distributedmergesort.employees.intermediate.DepartmentAndHead;
import dev.jonminter.distributedmergesort.employees.output.EmployeeWithDepartmentAndManager;
import dev.jonminter.distributedmergesort.nodestate.AbstractDistMergeSortHandler;
import dev.jonminter.distributedmergesort.nodestate.StateMachineEventDispatcher;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FinishedBuildingLookupTablesEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FinishedLocalSortEvent;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;

public class LocalSortHandler extends AbstractDistMergeSortHandler<FinishedBuildingLookupTablesEvent> {
  private static final Logger logger = Logger.getLogger(LocalSortHandler.class.getName());

  public LocalSortHandler(DistMergeSortConfig config,
      DistMergeSortContext context,
      StateMachineEventDispatcher dispatcher) {
    super(config, context, dispatcher);
  }

  @Override
  public void handleEvent(FinishedBuildingLookupTablesEvent event) throws Exception {
    logger.info("Performing local sort...");
    // LOCAL SORT HERE
    EmployeeLookupTables lookupTables = context.getEmployeeLookupTables();
    Map<String, DepartmentAndHead> managerDeptCache = new HashMap<>();

    for (Employee employee: event.getEmployeeList()) {
      DepartmentAndHead employeeDepartment = findEmployeeDepartment(employee.getId(), lookupTables, managerDeptCache);

      Optional<String> employeeManager = lookupTables.getManagerForEmployee(employee.getId())
          .flatMap(lookupTables::getEmployeeName);

      boolean isDeptHead = employeeDepartment.getDepartmentHeadEmployeeId().equals(employee.getId());

       context.getSortedEmployees().add(new EmployeeWithDepartmentAndManager(
          employee.getId(),
          employee.getName(),
          employeeDepartment.getDepartment(),
          employeeManager.orElse(""),
          isDeptHead
      ));
    }

    if (config.inDebugMode()) {
      debugWriteLocalSortToFile();
    }

    logger.info("Done with sorting slice of employee data, waiting for other nodes to finish");

    // Signal to leader that we're done with local sort
    event.getLocalSortBarrier().leave();
    logger.info("All nodes finished with local sort...");

    logger.info("Waiting for leader to finish with merge...");
    DistributedBarrier waitForMergeToFinishBarrier = new DistributedBarrier(
        context.getCuratorClient(),
        Constants.WAIT_FOR_MERGE_BARRIER_PATH);
    waitForMergeToFinishBarrier.waitOnBarrier();
    logger.info("Leader finished with merge, we can quit!");
    dispatcher.fire(new FinishedLocalSortEvent());
  }

  /**
   Finds the employees department by recursively looking up the manager ID and then checking to
   see if the manager is the head of a department, if yes then found department, if not then keep
   going and check that manager's manager.

   If employee has no manager then they are CEO so output Corporate
   */
  private DepartmentAndHead findEmployeeDepartment(String employeeId, EmployeeLookupTables lookupTables, Map<String, DepartmentAndHead> managerDeptCache) {
    String currentEmployeeId = employeeId;
    DepartmentAndHead departmentResult = null;

    do {
      Optional<String> departmentCurrentEmployeeHeads = lookupTables.getDepartmentThatEmployeeHeads(currentEmployeeId);
      if (departmentCurrentEmployeeHeads.isPresent()) {
        departmentResult =
            new DepartmentAndHead(departmentCurrentEmployeeHeads.get(), currentEmployeeId);
        break;
      }

      Optional<String> managerId = lookupTables.getManagerForEmployee(currentEmployeeId);
      if (managerId.isPresent()) {
        currentEmployeeId = managerId.get();
      } else {
        break;
      }
    } while (true);

    if (departmentResult == null) {
      departmentResult = new DepartmentAndHead("Corporate", currentEmployeeId);
    }

    return departmentResult;
  }

  private void debugWriteLocalSortToFile() throws IOException {
    String outputFile = config.getOutputDataPath() + "/" + String.format(Constants.LOCALLY_SORTED_EMPLOYEE_FILE, config.getMyNodeName());

    logger.log(Level.INFO, "Opening file {0} to write this nodes sorted records to...", outputFile);
    BufferedWriter localSortOutputFile = new BufferedWriter(new FileWriter(outputFile));
    localSortOutputFile.write("id,name,department,manager\n");

    EmployeeWithDepartmentAndManager[] employees = context.getSortedEmployees().toArray(new EmployeeWithDepartmentAndManager[context.getSortedEmployees().size()]);
    Arrays.sort(employees, context.getSortedEmployees().comparator());
    int deptHeads = 0;
    for (EmployeeWithDepartmentAndManager employee : employees) {
      if (employee.isDepartmentHeaad()) {
        logger.log(Level.INFO, "Found a department head for {0}, the dept head is {1}({2})",
            new Object[] { employee.getDepartment(), employee.getName(), employee.getId() });
        deptHeads++;
      }

      localSortOutputFile.write(String.format("%s,%s,%s,%s,%s\n",
          employee.getId(),
          employee.getName(),
          employee.getDepartment(),
          employee.getManager(),
          employee.isDepartmentHeaad()));
    }

    logger.log(Level.INFO, "Found {0} department heads on this node", deptHeads);

    logger.info("Finished writing to this nodes output file, closing...");
    localSortOutputFile.close();
  }
}
