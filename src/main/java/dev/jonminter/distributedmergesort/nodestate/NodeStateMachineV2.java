package dev.jonminter.distributedmergesort.nodestate;

import dev.jonminter.distributedmergesort.nodestate.events.JoinedGroupEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FinishedBuildingLookupTablesEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FinishedLocalSortEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FollowingLeaderEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.AllFollowersFinishedSortEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.ElectedLeaderEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.FinishedGlobalSortEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.FinishedWaitingForStragglersEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.jeasy.states.api.Event;
import org.jeasy.states.api.EventHandler;

public class NodeStateMachineV2 {
  public static class InvalidEventException extends Exception {
    public InvalidEventException(String message) {
      super(message);
    }
  }

  public enum State {
    INITIAL {
      @Override
      public State handleEvent(Event e) throws InvalidEventException{
        if (e instanceof JoinedGroupEvent) {
          return JOINED_GROUP;
        }
        throw new InvalidEventException(String.format("Event %s not allowed in INITIAL state", e.getName()));
      }
    },
    JOINED_GROUP {
      @Override
      public State handleEvent(Event e) throws InvalidEventException {
        if (e instanceof ElectedLeaderEvent) {
          return LEADER_WAIT_FOR_STRAGGLERS;
        } else if (e instanceof FollowingLeaderEvent) {
          return FOLLOWER_BUILD_LOOKUP_TABLES;
        }
        throw new InvalidEventException(String.format("Event %s not allowed in JOINED_GROUP state", e.getName()));
      }
    },

    LEADER_WAIT_FOR_STRAGGLERS {
      @Override
      public State handleEvent(Event e) throws InvalidEventException {
        if (e instanceof FinishedWaitingForStragglersEvent) {
          return LEADER_WAIT_FOR_FOLLOWER_SORT;
        }
        throw new InvalidEventException(String.format("Event %s not allowed in LEADER_WAIT_FOR_STRAGGLERS state", e.getName()));
      }
    },
    LEADER_WAIT_FOR_FOLLOWER_SORT {
      @Override
      public State handleEvent(Event e) throws InvalidEventException {
        if (e instanceof AllFollowersFinishedSortEvent) {
          return LEADER_MERGE_SORT;
        }
        throw new InvalidEventException(String.format("Event %s not allowed in LEADER_WAIT_FOR_FOLLOWER_SORT state", e.getName()));
      }
    },
    LEADER_MERGE_SORT {
      @Override
      public State handleEvent(Event e) throws InvalidEventException {
        if (e instanceof FinishedGlobalSortEvent) {
          return LEADER_DONE;
        }
        throw new InvalidEventException(String.format("Event %s not allowed in LEADER_MERGE_SORT state", e.getName()));
      }
    },
    LEADER_DONE {
      @Override
      public State handleEvent(Event e) throws InvalidEventException {
        throw new InvalidEventException("Leader is done, state machine in final state!");
      }
    },

    FOLLOWER_BUILD_LOOKUP_TABLES {
      @Override
      public State handleEvent(Event e) throws InvalidEventException {
        if (e instanceof FinishedBuildingLookupTablesEvent) {
          return FOLLOWER_LOCAL_SORT;
        }
        throw new InvalidEventException(String.format("Event %s not allowed in FOLLOWER_BUILD_LOOKUP_TABLES state", e.getName()));
      }
    },
    FOLLOWER_LOCAL_SORT {
      @Override
      public State handleEvent(Event e) throws InvalidEventException {
        if (e instanceof FinishedLocalSortEvent) {
          return FOLLOWER_DONE;
        }
        throw new InvalidEventException(String.format("Event %s not allowed in FOLLOWER_LOCAL_SORT state", e.getName()));
      }
    },
    FOLLOWER_DONE {
      @Override
      public State handleEvent(Event e) throws InvalidEventException {
        throw new InvalidEventException("Follower is done, state machine in final state!");
      }
    };
    public abstract State handleEvent(Event e) throws InvalidEventException;
  }

  public NodeStateMachineV2(final Map<Class, EventHandler> eventHandlers) {
    this.eventHandlers = eventHandlers;
  }

  private State currentState = State.INITIAL;
  private final Map<Class, EventHandler> eventHandlers;
  private final CountDownLatch stateMachineFinishedLatch = new CountDownLatch(1);

  /**
   * @// TODO: 3/1/21  Get an unchecked warning, maybe there is a better way to handle mapping for
   * event handlers which are defined as generic to the event type they handle.
   */
  @SuppressWarnings("unchecked")
  public synchronized State fire(Event e) throws Exception {
    State oldState = currentState;
    currentState = currentState.handleEvent(e);

    if (eventHandlers.containsKey(e.getClass())) {
      eventHandlers.get(e.getClass()).handleEvent(e);
    }

    if (isFinished()) {
      stateMachineFinishedLatch.countDown();
    }

    return oldState;
  }

  public synchronized boolean isFinished() {
    return currentState == State.FOLLOWER_DONE || currentState == State.LEADER_DONE;
  }

  public synchronized State getCurrentState() {
    return currentState;
  }

  public CountDownLatch getStateMachineFinishedLatch() {
    return stateMachineFinishedLatch;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Map<Class, EventHandler> eventHandlerMap = new HashMap<>();

    public <T extends Event> Builder withEventHandler(Class<T> eventClass, EventHandler<T> eventHandler) {
      eventHandlerMap.put(eventClass, eventHandler);
      return this;
    }

    public NodeStateMachineV2 build() {
      return new NodeStateMachineV2(eventHandlerMap);
    }

  }
}
