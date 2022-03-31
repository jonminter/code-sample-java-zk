package dev.jonminter.distributedmergesort.cluster;

import dev.jonminter.distributedmergesort.Constants;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ZPath;

public class ZookeeperModels {
  public final static ZPath NODE_FILE_ASSIGNMENT_ZPATH = ZPath.parseWithIds(Constants.NODE_FILE_ASSIGNMENTS_PATH);
  public final static ModelSpec<NodeFileAssignment> NODE_FILE_ASSIGNMENT_SPEC = ModelSpec
      .builder(NODE_FILE_ASSIGNMENT_ZPATH, JacksonModelSerializer.build(NodeFileAssignment.class))
      .build();

}
