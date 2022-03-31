package dev.jonminter.distributedmergesort.employees;

import dev.jonminter.distributedmergesort.employees.output.EmployeeWithDepartmentAndManager;
import java.util.Comparator;

public class EmployeeComparator implements Comparator<EmployeeWithDepartmentAndManager> {
  public final static Comparator<EmployeeWithDepartmentAndManager> COMPARATOR = Comparator
      .comparing(EmployeeWithDepartmentAndManager::getDepartment)
      .thenComparing((a,b) -> {
        int aIsDeptHead = a.isDepartmentHeaad() ? 1 : 0;

        int bIsDeptHead = b.isDepartmentHeaad() ? 1 : 0;

        return bIsDeptHead - aIsDeptHead;
      })
      .thenComparing(EmployeeWithDepartmentAndManager::getName);

  @Override
  public int compare(EmployeeWithDepartmentAndManager a,
      EmployeeWithDepartmentAndManager b) {
    return COMPARATOR.compare(a, b);
  }
}
