package dev.jonminter.distributedmergesort.nodestate.events.leader;

import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.jeasy.states.api.AbstractEvent;

public class AllFollowersFinishedSortEvent extends AbstractEvent {
  private final DistributedBarrier waitForMergeToFinishBarrier;
  public AllFollowersFinishedSortEvent(DistributedBarrier waitForMergeToFinishBarrier) {
    super("AllFollowersFinishedSort");
    this.waitForMergeToFinishBarrier = waitForMergeToFinishBarrier;
  }

  public DistributedBarrier getWaitForMergeToFinishBarrier() {
    return waitForMergeToFinishBarrier;
  }
}
