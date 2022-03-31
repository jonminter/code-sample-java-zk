package dev.jonminter.distributedmergesort.nodestate;

import dev.jonminter.distributedmergesort.DistMergeSortConfig;
import dev.jonminter.distributedmergesort.DistMergeSortContext;
import org.jeasy.states.api.Event;
import org.jeasy.states.api.EventHandler;

public abstract class AbstractDistMergeSortHandler<T extends Event> implements EventHandler<T> {
  protected final DistMergeSortConfig config;
  protected final DistMergeSortContext context;
  protected final StateMachineEventDispatcher dispatcher;

  public AbstractDistMergeSortHandler(DistMergeSortConfig config, DistMergeSortContext context, StateMachineEventDispatcher dispatcher) {
    this.config = config;
    this.context = context;
    this.dispatcher = dispatcher;
  }
}
