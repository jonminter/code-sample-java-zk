package dev.jonminter.distributedmergesort.nodestate.handlers.leader;

import dev.jonminter.distributedmergesort.Constants;
import dev.jonminter.distributedmergesort.DistMergeSortConfig;
import dev.jonminter.distributedmergesort.DistMergeSortContext;
import dev.jonminter.distributedmergesort.nodestate.AbstractDistMergeSortHandler;
import dev.jonminter.distributedmergesort.nodestate.StateMachineEventDispatcher;
import dev.jonminter.distributedmergesort.nodestate.events.leader.ElectedLeaderEvent;
import dev.jonminter.distributedmergesort.nodestate.events.leader.FinishedWaitingForStragglersEvent;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ElectedLeaderHandler extends AbstractDistMergeSortHandler<ElectedLeaderEvent> {
  private static final Logger logger = Logger.getLogger(ElectedLeaderHandler.class.getName());
  private static final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

  public ElectedLeaderHandler(DistMergeSortConfig config,
      DistMergeSortContext context,
      StateMachineEventDispatcher dispatcher) {
    super(config, context, dispatcher);
  }

  @Override
  public void handleEvent(ElectedLeaderEvent event) throws Exception {
    logger.log(Level.INFO, "Waiting [{0}]ms for all nodes to join group...", Constants.WAIT_FOR_STRAGGLERS_TIME);

    // Start timer to wait for all nodes to join group
    scheduler.schedule(() -> {
      logger.info("Done waiting!");

      try {
        dispatcher.fire(new FinishedWaitingForStragglersEvent());
      } catch (Exception e) {
        logger.log(Level.SEVERE, "FSM invalid state! Error: {0}", e);
        throw new RuntimeException(e);
      }
    }, Constants.WAIT_FOR_STRAGGLERS_TIME, TimeUnit.MILLISECONDS);
  }
}
