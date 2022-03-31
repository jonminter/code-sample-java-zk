package dev.jonminter.distributedmergesort.employees.input;

import com.google.gson.annotations.SerializedName;

public class Department {
  private String id;
  private String name;

  @SerializedName("department_head_id")
  private String departmentHeadId;

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getDepartmentHeadEmployeeId() {
    return departmentHeadId;
  }
}
