package dev.jonminter.distributedmergesort.nodestate.events.leader;

import org.jeasy.states.api.AbstractEvent;

public class FinishedWaitingForStragglersEvent extends AbstractEvent {
  public FinishedWaitingForStragglersEvent() {
    super("FinishedWaitingForStragglers");
  }
}
