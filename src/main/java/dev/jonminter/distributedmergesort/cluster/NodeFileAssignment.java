package dev.jonminter.distributedmergesort.cluster;

import java.util.List;

public class NodeFileAssignment {
  public NodeFileAssignment() {}

  public NodeFileAssignment(List<String> files) {
    this.files = files;
  }

  public List<String> getFiles() {
    return files;
  }

  private List<String> files;
}
