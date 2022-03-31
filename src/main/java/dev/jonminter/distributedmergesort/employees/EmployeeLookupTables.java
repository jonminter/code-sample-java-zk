package dev.jonminter.distributedmergesort.employees;

import java.util.Map;
import java.util.Optional;

public class EmployeeLookupTables {
  private final Map<String, String> employeeManagerLookup;
  private final Map<String, String> departmentEmployeeHeadsLookup;
  private final Map<String, String> departmentHeadEmployeeIdLookup;
  private final Map<String, String> employeeNameLookup;

  public EmployeeLookupTables(
      Map<String, String> employeeManagerLookup,
      Map<String, String> employeeNameLookup,
      Map<String, String> departmentEmployeeHeadsLookup,
      Map<String, String> departmentHeadEmployeeIdLookup) {
    this.employeeManagerLookup = employeeManagerLookup;
    this.departmentEmployeeHeadsLookup = departmentEmployeeHeadsLookup;
    this.departmentHeadEmployeeIdLookup = departmentHeadEmployeeIdLookup;
    this.employeeNameLookup = employeeNameLookup;
  }

  public Optional<String> getDepartmentThatEmployeeHeads(String employeeId) {
    String department = departmentEmployeeHeadsLookup.get(employeeId);
    return Optional.ofNullable(department);
  }

  public Optional<String> getEmployeeThatHeadsDepartment(String department) {
    String employeeId = departmentHeadEmployeeIdLookup.get(department);
    return Optional.ofNullable(employeeId);
  }

  public Optional<String> getManagerForEmployee(String employeeId) {
    String managerId = employeeManagerLookup.get(employeeId);
    return Optional.ofNullable(managerId);
  }

  public Optional<String> getEmployeeName(String employeeId) {
    return Optional.ofNullable(employeeNameLookup.get(employeeId));
  }
}
