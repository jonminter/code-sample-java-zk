package dev.jonminter.distributedmergesort.employees.intermediate;

public class DepartmentAndHead {
  private final String department;
  private final String departmentHead;

  public DepartmentAndHead(String department, String departmentHead) {
    this.department = department;
    this.departmentHead = departmentHead;
  }

  public String getDepartment() {
    return department;
  }

  public String getDepartmentHeadEmployeeId() {
    return departmentHead;
  }
}
