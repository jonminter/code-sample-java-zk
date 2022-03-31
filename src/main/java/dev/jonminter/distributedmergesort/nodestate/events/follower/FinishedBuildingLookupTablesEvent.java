package dev.jonminter.distributedmergesort.nodestate.events.follower;

import dev.jonminter.distributedmergesort.employees.input.Department;
import dev.jonminter.distributedmergesort.employees.input.Employee;
import java.util.List;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.jeasy.states.api.AbstractEvent;

public class FinishedBuildingLookupTablesEvent extends AbstractEvent {
  private final DistributedDoubleBarrier localSortBarrier;
  private final List<Employee> employeeList;
  private final List<Department> departmentList;

  public FinishedBuildingLookupTablesEvent(
      DistributedDoubleBarrier localSortBarrier,
      List<Employee> employeeList,
      List<Department> departmentList) {
    super("FinishedBuildingLookupTables");
    this.localSortBarrier = localSortBarrier;
    this.employeeList = employeeList;
    this.departmentList = departmentList;
  }

  public DistributedDoubleBarrier getLocalSortBarrier() {
    return localSortBarrier;
  }
  public List<Employee> getEmployeeList() { return employeeList; }
  public List<Department> getDepartmentList() { return departmentList; }
}
