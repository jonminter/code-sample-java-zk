package dev.jonminter.distributedmergesort.nodestate.handlers;

import dev.jonminter.distributedmergesort.Constants;
import dev.jonminter.distributedmergesort.DistMergeSortConfig;
import dev.jonminter.distributedmergesort.DistMergeSortContext;
import dev.jonminter.distributedmergesort.nodestate.AbstractDistMergeSortHandler;
import dev.jonminter.distributedmergesort.nodestate.StateMachineEventDispatcher;
import dev.jonminter.distributedmergesort.nodestate.events.JoinedGroupEvent;
import dev.jonminter.distributedmergesort.nodestate.events.follower.FollowingLeaderEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.ElectedLeaderEvent;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;

public class JoinedGroupHandler extends AbstractDistMergeSortHandler<JoinedGroupEvent> {
  private static final Logger logger = Logger.getLogger(JoinedGroupHandler.class.getName());

  public JoinedGroupHandler(DistMergeSortConfig config,
      DistMergeSortContext context,
      StateMachineEventDispatcher dispatcher) {
    super(config, context, dispatcher);
  }


  @Override
  public void handleEvent(JoinedGroupEvent event) throws Exception {
    AtomicBoolean isLeader = new AtomicBoolean(false);
    LeaderLatch leaderLatch = new LeaderLatch(
        context.getCuratorClient(),
        Constants.LEADER_ELECTION_PATH,
        config.getMyNodeName(),
        LeaderLatch.CloseMode.NOTIFY_LEADER);
    leaderLatch.addListener(new LeaderLatchListener() {

      @Override
      public void isLeader() {
        logger.info("I am the LEADER now!");
        try {
          isLeader.set(true);
          context.startDistMergeSort();
        } catch (Exception e) {
          logger.severe(e.toString());
          throw new RuntimeException(e);
        }
      }

      @Override
      public void notLeader() {
        logger.info("A new leader has been chosen! But not me... :(");
      }
    });
    context.setLeaderLatch(leaderLatch);

    logger.info("Running leader election...");
    leaderLatch.start();
    logger.info("Leader election started");

    logger.info("Follower waiting to be see who the leader is...");
    context.getPostLeaderElectionStartLatch().await();

    if (isLeader.get()) {
      dispatcher.fire(new ElectedLeaderEvent());
    } else {
      dispatcher.fire(new FollowingLeaderEvent());
    }
  }
}
