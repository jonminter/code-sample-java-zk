package dev.jonminter.distributedmergesort;

public class Constants {
  public static final String GROUP_MEMBERS_PATH = "/members";
  public static final String LEADER_ELECTION_PATH = "/leader_election";
  public static final String WAIT_FOR_STRAGGLERS_BARRIER = "/wait_for_stragglers_barrier";
  public static final int WAIT_FOR_STRAGGLERS_TIME = 60000;
  public static final String START_BARRIER_PATH = "/start_barrier";
  public static final String DEPARTMENTS_JSON_PATH = "departments.json";
  public static final String EMPLOYEES_JSON_GLOB = "regex:.*employees\\..+\\.json";
  public static final String NODE_FILE_ASSIGNMENTS_ROOT_PATH = "/node_assignments";
  public static final String NODE_FILE_ASSIGNMENTS_PATH = "/node_assignments/{nodeName}";
  public static final String LOCAL_SORT_BARRIER_PATH = "/local_sort_barrier";
  public static final String BUILD_LOOKUP_TABLES_BARRIER_PATH = "/build_lookup_tables_barrier";
  public static final String DIST_EMPLOYEE_MANAGER_MAP = "%s:employee-manager-map";
  public static final String DIST_EMPLOYEE_NAME_MAP = "%s:employee-name-map";
  public static final int MERGE_SORT_FETCH_BUFFER = 1000;
  public static final String EMPLOYEE_OUTPUT_FILE = "sorted-employees.csv";
  public static final String LOCALLY_SORTED_EMPLOYEE_FILE = "%s-locally-sorted-employees.csv";
  public static final String WAIT_FOR_MERGE_BARRIER_PATH = "/wait_for_merge_complete";
}
