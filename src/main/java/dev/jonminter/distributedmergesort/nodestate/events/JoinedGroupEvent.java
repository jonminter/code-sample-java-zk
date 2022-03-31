package dev.jonminter.distributedmergesort.nodestate.events;

import org.apache.curator.framework.recipes.nodes.GroupMember;
import org.jeasy.states.api.AbstractEvent;

public class JoinedGroupEvent extends AbstractEvent {
  private final GroupMember member;

  public JoinedGroupEvent(GroupMember member) {
    super("JoinedGroup");
    this.member = member;
  }

  public GroupMember getMember() {
    return member;
  }
}
