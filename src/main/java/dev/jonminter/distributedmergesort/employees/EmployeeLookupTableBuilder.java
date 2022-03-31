package dev.jonminter.distributedmergesort.employees;

import java.util.Map;

public class EmployeeLookupTableBuilder {
  public static class CannotModifyFrozenLookupTablesException extends Exception {
    public CannotModifyFrozenLookupTablesException() {
      super("Not allowed to modify lookup tables after they have been built");
    }
  }

  private final Map<String, String> employeeManagerLookup;
  private final Map<String, String> departmentEmployeeHeadsLookup;
  private final Map<String, String> departmentHeadEmployeeIdLookup;
  private final Map<String, String> employeeNameLookup;
  private boolean isFrozen = false;

  public EmployeeLookupTableBuilder(
      Map<String, String> employeeManagerLookup,
      Map<String, String> employeeNameLookup,
      Map<String, String> departmentEmployeeHeadsLookup,
      Map<String, String> departmentHeadEmployeeIdLookup) {
    this.employeeManagerLookup = employeeManagerLookup;
    this.departmentEmployeeHeadsLookup = departmentEmployeeHeadsLookup;
    this.departmentHeadEmployeeIdLookup = departmentHeadEmployeeIdLookup;
    this.employeeNameLookup = employeeNameLookup;
  }

  public void setManagerForEmployee(String employeeId, String managerEmployeeId)
      throws CannotModifyFrozenLookupTablesException {
    if (isFrozen) {
      throw new CannotModifyFrozenLookupTablesException();
    }

    employeeManagerLookup.put(employeeId, managerEmployeeId);
  }

  public void setDepartmentHead(String department, String deptHeadEmployeeId)
      throws CannotModifyFrozenLookupTablesException {
    if (isFrozen) {
      throw new CannotModifyFrozenLookupTablesException();
    }
    departmentEmployeeHeadsLookup.put(deptHeadEmployeeId, department);
    departmentHeadEmployeeIdLookup.put(department, deptHeadEmployeeId);
  }

  public void mapEmployeeIdToName(String employeeId, String name)
      throws CannotModifyFrozenLookupTablesException {
    if (isFrozen) {
      throw new CannotModifyFrozenLookupTablesException();
    }
    employeeNameLookup.put(employeeId, name);
  }

  public void freeze() {
    this.isFrozen = true;
  }
}
