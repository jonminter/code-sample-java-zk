package dev.jonminter.distributedmergesort.nodestate.events.leader;

import org.jeasy.states.api.AbstractEvent;

public class ElectedLeaderEvent extends AbstractEvent {
  public ElectedLeaderEvent() {
    super("ElectedLeader");
  }
}
