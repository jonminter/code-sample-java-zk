package dev.jonminter.distributedmergesort.nodestate.events.follower;

import org.jeasy.states.api.AbstractEvent;

public class FollowingLeaderEvent extends AbstractEvent {
  public FollowingLeaderEvent() {
    super("FollowingLeader");
  }
}
