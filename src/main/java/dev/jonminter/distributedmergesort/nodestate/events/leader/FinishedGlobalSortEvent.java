package dev.jonminter.distributedmergesort.nodestate.events.leader;

import org.jeasy.states.api.AbstractEvent;

public class FinishedGlobalSortEvent extends AbstractEvent {
  public FinishedGlobalSortEvent() {
    super("FinishedGlobalSort");
  }
}
