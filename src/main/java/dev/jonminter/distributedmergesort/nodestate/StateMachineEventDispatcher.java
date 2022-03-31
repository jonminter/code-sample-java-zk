package dev.jonminter.distributedmergesort.nodestate;

import org.jeasy.states.api.AbstractEvent;

/**
 * For handling the circular reference issue when initializing the
 * state machine and its handler. This is a wrapper around `fire` method
 * for StateMachine. Event handlers need to fire events to change the current state
 * when some event they're waiting for happens. When constructing the
 * event handler we don't have the state machine yet because its in the process of
 * being built. Allows the event handler implementations to be simple and not require
 * setting the state machine later. Just handle updating with the finalized state
 * machine here after it's been built only one time.
 */
public class StateMachineEventDispatcher {

  public void setStateMachine(NodeStateMachineV2 stateMachine) {
    this.stateMachine = stateMachine;
  }

  public NodeStateMachineV2.State fire(AbstractEvent event) throws Exception {
    return stateMachine.fire(event);
  }

  private NodeStateMachineV2 stateMachine;
}
