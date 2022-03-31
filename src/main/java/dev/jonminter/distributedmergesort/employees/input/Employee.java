package dev.jonminter.distributedmergesort.employees.input;

import com.google.gson.annotations.SerializedName;

public class Employee {
  private String id;
  private String name;
  @SerializedName("manager_id")
  private String managerId;

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getManagerId() {
    return managerId;
  }
}
