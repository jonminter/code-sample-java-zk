package dev.jonminter.distributedmergesort.nodestate;

import dev.jonminter.distributedmergesort.DistMergeSortConfig;
import dev.jonminter.distributedmergesort.DistMergeSortContext;
import dev.jonminter.distributedmergesort.nodestate.events.JoinedGroupEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FinishedBuildingLookupTablesEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FinishedLocalSortEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FollowingLeaderEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.AllFollowersFinishedSortEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.ElectedLeaderEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.FinishedGlobalSortEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.FinishedWaitingForStragglersEvent;
import dev.jonminter.distributedmergesort.nodestate.handlers.JoinedGroupHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.follower.BuildLookupTablesHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.follower.LocalSortHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.leader.DivideWorkHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.leader.ElectedLeaderHandler;
import dev.jonminter.distributedmergesort.nodestate.handlers.leader.GlobalMergeHandler;
import java.util.HashSet;
import java.util.Set;
import org.jeasy.states.api.FiniteStateMachine;
import org.jeasy.states.api.State;
import org.jeasy.states.api.Transition;
import org.jeasy.states.core.FiniteStateMachineBuilder;
import org.jeasy.states.core.TransitionBuilder;

/**
 * Had started using this library J-Easy to implement the state machine but ran into a problem
 * where when you fire an event the handler gets called BEFORE the state transition happens.
 * In my handlers I wanted the ability to be able to fire another event and change to another
 * state within that handler. There was no way to do this with that library as it only had
 * one type of handler. I ended up implementing my own state machine with enums
 * in NodeStateMachineV2.java. This is no longer used but I am still using the Event/EventHandler
 * objects from J-Easy, did not get to refactoring them out.
 */
@Deprecated
public class NodeStateMachine {
  public static final State INITIAL = new State("initial");
  public static final State JOINED_GROUP = new State("joined-group");

  public static final State LEADER_WAIT_FOR_STRAGGLERS = new State("leader-wait-for-stragglers");
  public static final State LEADER_WAIT_FOR_FOLLOWER_SORT = new State("leader-wait-for-follower-sort");
  public static final State LEADER_MERGE_SORT = new State("leader-merge-sort");
  public static final State LEADER_DONE = new State("leader-done");

  public static final State FOLLOWER_BUILD_LOOKUP_TABLES = new State("follower-build-lookup-tables");
  public static final State FOLLOWER_LOCAL_SORT = new State("follower-build-local-sort");
  public static final State FOLLOWER_DONE = new State("follower-done");

  public static final Set<State> ALL_STATES = new HashSet<>();
  static {
    ALL_STATES.add(INITIAL);
    ALL_STATES.add(JOINED_GROUP);

    ALL_STATES.add(LEADER_WAIT_FOR_STRAGGLERS);
    ALL_STATES.add(LEADER_WAIT_FOR_FOLLOWER_SORT);
    ALL_STATES.add(LEADER_MERGE_SORT);
    ALL_STATES.add(LEADER_DONE);

    ALL_STATES.add(FOLLOWER_BUILD_LOOKUP_TABLES);
    ALL_STATES.add(FOLLOWER_LOCAL_SORT);
    ALL_STATES.add(FOLLOWER_DONE);
  }

  public static final Set<State> FINAL_STATES = new HashSet<>();
  static {
    FINAL_STATES.add(FOLLOWER_DONE);
    FINAL_STATES.add(LEADER_DONE);
  }

  public static class Builder {
    private StateMachineEventDispatcher dispatcher;
    private DistMergeSortContext context;
    private DistMergeSortConfig config;

    public Builder withStateMachineDispatcher(StateMachineEventDispatcher dispatcher) {
      this.dispatcher = dispatcher;
      return this;
    }

    public Builder withConfig(DistMergeSortConfig config) {
      this.config = config;
      return this;
    }

    public Builder withContext(DistMergeSortContext context) {
      this.context = context;
      return this;
    }

    public FiniteStateMachine build() {
      Transition joinGroup =
          new TransitionBuilder()
              .name("join-group")
              .sourceState(INITIAL)
              .eventType(JoinedGroupEvent.class)
              .eventHandler(new JoinedGroupHandler(config, context, dispatcher))
              .targetState(JOINED_GROUP)
              .build();

      Transition electedLeader =
          new TransitionBuilder()
              .name("elected-leader")
              .sourceState(JOINED_GROUP)
              .eventType(ElectedLeaderEvent.class)
              .eventHandler(new ElectedLeaderHandler(config, context, dispatcher))
              .targetState(LEADER_WAIT_FOR_STRAGGLERS)
              .build();
      Transition leaderDivideWork =
          new TransitionBuilder()
              .name("leader-divide-work")
              .sourceState(LEADER_WAIT_FOR_STRAGGLERS)
              .eventType(FinishedWaitingForStragglersEvent.class)
              .eventHandler(new DivideWorkHandler(config, context, dispatcher))
              .targetState(LEADER_WAIT_FOR_FOLLOWER_SORT)
              .build();
      Transition leaderGlobalMerge =
          new TransitionBuilder()
              .name("leader-global-merge")
              .sourceState(LEADER_WAIT_FOR_FOLLOWER_SORT)
              .eventType(AllFollowersFinishedSortEvent.class)
              .eventHandler(new GlobalMergeHandler(config, context, dispatcher))
              .targetState(LEADER_MERGE_SORT)
              .build();
      Transition leaderDone =
          new TransitionBuilder()
              .name("leader-done")
              .sourceState(LEADER_MERGE_SORT)
              .eventType(FinishedGlobalSortEvent.class)
              .targetState(LEADER_DONE)
              .build();

      Transition followerBuildLookups =
          new TransitionBuilder()
              .name("follower-build-lookup-tables")
              .sourceState(JOINED_GROUP)
              .eventType(FollowingLeaderEvent.class)
              .eventHandler(new BuildLookupTablesHandler(config, context, dispatcher))
              .targetState(FOLLOWER_BUILD_LOOKUP_TABLES)
              .build();
      Transition followerLocalSort =
          new TransitionBuilder()
              .name("follower-local-sort")
              .sourceState(FOLLOWER_BUILD_LOOKUP_TABLES)
              .eventType(FinishedBuildingLookupTablesEvent.class)
              .eventHandler(new LocalSortHandler(config, context, dispatcher))
              .targetState(FOLLOWER_LOCAL_SORT)
              .build();
      Transition followerDone =
          new TransitionBuilder()
              .name("follower-done")
              .sourceState(FOLLOWER_LOCAL_SORT)
              .eventType(FinishedLocalSortEvent.class)
              .targetState(FOLLOWER_DONE)
              .build();

      return new FiniteStateMachineBuilder(ALL_STATES, INITIAL)
          .registerTransition(joinGroup)
          .registerTransition(electedLeader)
          .registerTransition(leaderDivideWork)
          .registerTransition(leaderGlobalMerge)
          .registerTransition(leaderDone)
          .registerTransition(followerBuildLookups)
          .registerTransition(followerLocalSort)
          .registerTransition(followerDone)
          .registerFinalStates(FINAL_STATES)
          .build();
    }
  }
}
