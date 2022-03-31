package dev.jonminter.distributedmergesort.nodestate.events.follower;

import org.jeasy.states.api.AbstractEvent;

public class FinishedLocalSortEvent extends AbstractEvent {
  public FinishedLocalSortEvent() {
    super("FinishedLocalSortEvent");
  }
}
