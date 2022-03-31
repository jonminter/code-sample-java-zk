package dev.jonminter.distributedmergesort.grpc;

import dev.jonminter.distributedmergesort.employees.output.EmployeeWithDepartmentAndManager;
import dev.jonminter.distributedmergesort.server.Employee;

public class EmployeeTransformer {
  public static Employee fromEmployeeWithManagerAmdDepartment(EmployeeWithDepartmentAndManager source) {
    return Employee.newBuilder()
        .setId(source.getId())
        .setName(source.getName())
        .setDepartment(source.getDepartment())
        .setManager(source.getManager())
        .setIsDepartmentHead(source.isDepartmentHeaad())
        .build();
  }

  public static EmployeeWithDepartmentAndManager fromGrpcEmployee(Employee e) {
    return new EmployeeWithDepartmentAndManager(
        e.getId(),
        e.getName(),
        e.getDepartment(),
        e.getManager(),
        e.getIsDepartmentHead()
    );
  }
}
