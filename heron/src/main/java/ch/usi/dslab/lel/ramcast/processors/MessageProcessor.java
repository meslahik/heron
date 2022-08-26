package ch.usi.dslab.lel.ramcast.processors;

import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpointGroup;
import ch.usi.dslab.lel.ramcast.models.AtomicVectorClock;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageProcessor {
  private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
  //
  private Queue<RamcastMessage> processing;
  private Queue<PendingTimestamp> pendingTimestamps;
  //    private Queue<RamcastMessage> ordered;
  //
  //  private ConcurrentSkipListSet<RamcastMessage> processing;
  //  private ConcurrentSkipListSet<PendingTimestamp> pendingTimestamps;
  private ConcurrentSkipListSet<RamcastMessage> ordered;

  private RamcastEndpointGroup group;
  private RamcastAgent agent;
  private Thread pendingMessageProcessor;
  private boolean isRunning = true;

  //  AtomicInteger counter = new AtomicInteger(0);
  //  int expectedCounter = 1;
  AtomicVectorClock counter;
  AtomicVectorClock expectedCounter;

  public MessageProcessor(RamcastEndpointGroup group, RamcastAgent agent) {
    this.group = group;
    this.agent = agent;
    //        this.pendingTimestamps = new ConcurrentLinkedQueue<>();
    //        this.processing = new ConcurrentLinkedQueue<>();
    //        this.ordered = new ConcurrentLinkedQueue<>();
    this.pendingTimestamps = new ConcurrentLinkedQueue<>();
    this.processing = new ConcurrentLinkedQueue<>();
    this.ordered = new ConcurrentSkipListSet<>(Comparator.comparingInt(RamcastMessage::getFinalTs));

    counter = new AtomicVectorClock(agent.getGroupId(), 0);
    expectedCounter = new AtomicVectorClock(agent.getGroupId(), 1);

    this.pendingMessageProcessor =
            new Thread(
                    () -> {
                      long time = System.currentTimeMillis();
                      long time2 = System.currentTimeMillis();

                      try {
                        MDC.put("ROLE", agent.getGroupId() + "/" + agent.getNode().getNodeId());
                        while (!Thread.interrupted()) {
                          if (!isRunning) Thread.yield();

                          long currentTime = System.currentTimeMillis();
                          if (currentTime - time > 2000) {
//                            logger.debug("pendingTimestamps {}", pendingTimestamps);
//                            logger.debug("processing {}", processing);
//                            logger.debug("ordered {}", ordered);
                            time = currentTime;
                          }

                          if (pendingTimestamps.size() > 0)
                            for (PendingTimestamp pending : pendingTimestamps) {
                              // the read order is important
                              // the problematic case with this one is when tmpRound is 0, write happens, then tmpClock and tmpCounter has value
//                              int tmpRound = pending.getRound();
//                              int tmpClock = pending.getClock();
//                              int tmpCounter = pending.getCounter();

                              // only when tmpCounter is read and is as expected, tmpClock and tmpRound can be guaranteed that is written
                              int tmpCounter = pending.getCounter();
                              int tmpClock = pending.getClock();
                              int tmpRound = pending.getRound();

                              // Counter
                              if (tmpCounter <= 0 || tmpClock <= 0 || tmpRound <= 0) {
                                Thread.yield();
                                continue;
                              }

                              // Leader Election
                              if (tmpRound != group.getRound().get()) {
//                                if (RamcastConfig.LOG_ENABLED)
//                                  logger.trace(
//                                          "[{}] timestamp [{}/{}] has babllot doesn't match to local {}",
//                                          pending.message.getId(),
//                                          tmpRound,
//                                          tmpClock,
//                                          group.getRound().get());
                                logger.error("LEADER !!!!");
                                Thread.yield();
                                continue;
                              }

                              if (tmpCounter == expectedCounter.get()) {
                                if (RamcastConfig.LOG_ENABLED)
                                  logger.debug("[{}] expectedCounter {} visited in pending [{}/{}/{}] for message {}, pending size {}",
                                          pending.message.getId(),
                                          expectedCounter.getValue(),
                                          tmpRound,
                                          tmpClock,
                                          tmpCounter,
                                          pending.message.getId(),
                                          pendingTimestamps.size());
//                                expectedCounter++;
                                expectedCounter.incrementAndGet();
                              } else if (this.agent.isLeader() && pending.groupId != this.agent.getGroupId() && pending.shouldPropagate()) {
                                // do nothing, continue processing pending

//                                  int newClock = group.getClock().incrementAndGet();
//                                  if (RamcastConfig.LOG_ENABLED)
//                                    logger.debug(
//                                            "[{}] remove pending timestamp [{}/{}/{}] of group {} index {}  -- LEADER process propagate ts with value [{}/{}]",
//                                            pending.message.getId(),
//                                            tmpRound,
//                                            tmpClock,
//                                            tmpCounter,
//                                            pending.groupId,
//                                            pending.groupIndex,
//                                            tmpRound,
//                                            tmpClock);
                                synchronized (lock) {
                                  int clock = group.getClock().get();
                                  int newClock = Math.max(tmpClock, clock);
                                  while (!group.getClock().compareAndSet(clock, newClock)) {
//                                System.out.println("Clock value changed!!!");
                                    clock = group.getClock().get();
                                    newClock = Math.max(tmpClock, clock);
                                  }

                                  int newCounter = counter.incrementAndGet();
                                  int newCounterValue = counter.getValue();
                                  group.leaderPropageTs(
                                          pending.message,
                                          tmpRound,
                                          tmpClock,
                                          pending.groupId,
                                          pending.groupIndex,
                                          newCounter);
                                  if (RamcastConfig.LOG_ENABLED)
                                    logger.debug("[{}] propagated Ts [{}/{}/{}] of group {} to followers",
                                            pending.message.getId(),
                                            tmpRound,
                                            tmpClock,
                                            newCounterValue,
                                            pending.groupId);
                                }
                                pending.shouldPropagate = false;

                                Thread.yield();
                                continue;

                              } else {
                                Thread.yield();
                                continue;
                              }

//                              // set clock
//                              int clock = group.getClock().get();
//                              int newClock = Math.max(tmpClock, clock);
//                              while (!group.getClock().compareAndSet(clock, newClock)) {
////                                System.out.println("Clock value changed!!!");
//                                clock = group.getClock().get();
//                                newClock = Math.max(tmpClock, clock);
//                              }

//                              if (RamcastConfig.LOG_ENABLED)
//                                logger.trace(
//                                        "[{}] receive ts: [{}/{}] of group {}. Local value [{}/{}], pendingTimestamps {}",//, TS memory: \n {}",
//                                        pending.message.getId(),
//                                        tmpRound,
//                                        tmpClock,
//                                        pending.groupId,
//                                        group.getRound().get(),
//                                        group.getClock().get(),
//                                        pendingTimestamps
////                                        group.getTimestampBlock()
//                                );

                              // if this is leader, this should propagate the msg to local group if possible
//                              if (this.agent.isLeader()) {
//                                if (pending.groupId != this.agent.getGroupId() && pending.shouldPropagate()) {
////                                  int newClock = group.getClock().incrementAndGet();
////                                  if (RamcastConfig.LOG_ENABLED)
////                                    logger.debug(
////                                            "[{}] remove pending timestamp [{}/{}/{}] of group {} index {}  -- LEADER process propagate ts with value [{}/{}]",
////                                            pending.message.getId(),
////                                            tmpRound,
////                                            tmpClock,
////                                            tmpCounter,
////                                            pending.groupId,
////                                            pending.groupIndex,
////                                            tmpRound,
////                                            tmpClock);
//                                  group.leaderPropageTs(
//                                          pending.message,
//                                          tmpRound,
//                                          tmpClock,
//                                          pending.groupId,
//                                          pending.groupIndex,
//                                          counter.incrementAndGet());
//                                  if (RamcastConfig.LOG_ENABLED)
//                                    logger.debug("[{}] propagated Ts of group {} with counter {} in followers",
//                                            pending.message.getId(),
//                                            pending.groupId,
//                                            counter.get());
//                                  pending.shouldPropagate = false;
////                                  Thread.yield();
////                                  continue;
//                                }
//                              }


//                              if (this.ts.keySet().contains(tmpClock)) {
//                                System.out.println("DUPLICATED CLOCK " + tmpClock + " - old " + this.ts.get(tmpClock) + " - new " + pending.message.getId());
//                              } else {
//                                this.ts.put(tmpClock, pending.message.getId());
//                              }

//                              if (RamcastConfig.LOG_ENABLED)
//                                logger.trace(
//                                        "[{}] remove pending timestamp [{}/{}] of group {} index {}",
//                                        pending.message.getId(),
//                                        tmpRound,
//                                        tmpClock,
//                                        pending.groupId,
//                                        pending.groupIndex);
//                              if (RamcastConfig.LOG_ENABLED)
//                                logger.trace(
//                                        "[{}] pending.shouldAck(): {}, pending.isSendAck(): {}, this.agent.isLeader(): {}",
//                                        pending.message.getId(),
//                                        pending.shouldAck(),
//                                        pending.isSendAck(),
//                                        this.agent.isLeader());
                              if (pending.shouldAck() && !pending.isSendAck() && !this.agent.isLeader()) {
//                                if (RamcastConfig.LOG_ENABLED)
//                                  logger.trace("[{}] Start sending ack for timestamp [{}/{}] of group {} index {}",
//                                          pending.message.getId(),
//                                          tmpRound,
//                                          tmpClock,
//                                          pending.groupId,
//                                          pending.groupIndex);
                                group.sendAck(pending.message, tmpRound, tmpClock, pending.groupId, pending.groupIndex);
                                pending.sendAck = true;
                              }

                              // add timestamp to message's ts list
                              pending.message.addTimestamp(tmpClock);

                              pendingTimestamps.remove(pending);
                            }

                          int minTs = Integer.MAX_VALUE;
                          if (pendingTimestamps.size() > 0) {
                            for (PendingTimestamp pending : pendingTimestamps) {
                              int clock = pending.getClock();
                              if (clock > 0 && minTs > clock)
                                minTs = clock;
                            }
                          }

                          if (processing.size() > 0) {
//                            if (RamcastConfig.LOG_ENABLED)
//                              logger.debug("processing queue size: {}, {}", processing.size(), Arrays.toString(processing.toArray()));
                            for (RamcastMessage message : processing) {
//                              int maxTs = group.getTimestampBlock().getMaxTimestamp(message);
                              int maxTs = message.getMaxTimestamps();
                              if (isFulfilled(message)) {
                                message.setFinalTs(maxTs);
                                this.ordered.add(message);
                                this.processing.remove(message);
//                                if (RamcastConfig.LOG_ENABLED)
//                                  logger.trace("[{}] finalts={} is fulfilled. AFTER Remove from processing, put to order. order size={}", message.getId(), message.getFinalTs(), ordered.size());
                              }
                              if (maxTs != 0 && minTs > maxTs) {
                                minTs = maxTs;
//                                if (RamcastConfig.LOG_ENABLED)
//                                  logger.trace("processing queue size: {}, minTs {}, finalTs {}, {}", processing.size(), minTs, message.getFinalTs(), Arrays.toString(processing.toArray()));
                              }
                            }
                          }

                          if (ordered.size() == 0) {
                            Thread.yield();
                            continue;
                          }

                          int minOrderedTs = Integer.MAX_VALUE;
                          if (ordered.first() != null) minOrderedTs = ordered.first().getFinalTs();

                          long currentTime2 = System.currentTimeMillis();
                          if (currentTime2 - time2 > 2000) {
                            logger.debug("minTs: {}, minOrderedTs: {}", minTs, minOrderedTs);
                            time2 = currentTime2;
                          }

                          for (RamcastMessage message : ordered) {
//                            if (RamcastConfig.LOG_ENABLED)
//                              logger.debug("[{}] inside ordered loop, order size={}, message finalts={}, minTs {}, minOrderedTs {}", message.getId(), ordered.size(), message.getFinalTs(), minTs, minOrderedTs);
                            if (message.getFinalTs() <= minTs && message.getFinalTs() <= minOrderedTs) {
//                              if (RamcastConfig.LOG_ENABLED)
//                                logger.debug("[{}] finalts={} is fulfilled. go to remove from ordered and call deliver. order size={}", message.getId(), message.getFinalTs(), ordered.size());
                              ordered.remove(message);
                              this.agent.deliver(message);
                            }
                          }

                          Thread.yield();
                        }
                      } catch (IOException e) {
                        e.printStackTrace();
                      }
                    });
    this.pendingMessageProcessor.setName("MessageProcessor");
    this.pendingMessageProcessor.start();
  }

  Object lock = new Object();

  private boolean isFulfilled(RamcastMessage message) {
//    if (!group.getTimestampBlock().isFulfilled(message)) {
    if (!message.isFulfilled()) {
//      if (RamcastConfig.LOG_ENABLED)
//        logger.debug("[{}] fulfilled: NO - doesn't have enough TS", message.getId());
      return false;
    }
    if (!message.isAcked(group.getRound().get())) {
//      if (RamcastConfig.LOG_ENABLED)
//        logger.debug("[{}] fulfilled: NO - doesn't have enough ACKS {}", message.getId(), message);
      return false;
    }
    // there is a case where ts is not available at the check on line 55, but it is now. so need to
    // check again
    for (PendingTimestamp pending : pendingTimestamps) {
      if (pending.message.getId() == message.getId()){
//        if (RamcastConfig.LOG_ENABLED)
//          logger.debug("[{}] fulfilled: NO - there is pending waiting for this message, pendings: {}",
//                  message.getId(),
//                  message);
        return false;
      }
    }
    return true;
  }

  public void handleMessage(RamcastMessage message) {
    int msgId = message.getId();

    // hack: checking if there is two process and 1 group => write bench => deliver now
    if (RamcastConfig.getInstance().getNodePerGroup() == 2 && RamcastConfig.getInstance().getGroupCount() == 1)
      try {
        this.agent.deliverWrite(message);
        return;
      } catch (IOException e) {
        e.printStackTrace();
      }

//    if (RamcastConfig.LOG_ENABLED)
//      logger.trace("[{}] Receiving new message", msgId);
//    if (RamcastConfig.LOG_ENABLED)
//      logger.trace("[Recv][Step #1][msgId={}]", msgId);
    if (agent.isLeader()) {
//      if (RamcastConfig.LOG_ENABLED) logger.trace("[{}] Leader processing...", msgId);
      try {
        // this is equivalent to first include in skeen's algorithm (before mcast step 2)
        // the reason is it is written before creating pendingTimestamps for this message
        synchronized (lock) {
          int round = group.getRound().get();
          int clock = group.getClock().incrementAndGet();
          int newCounter = this.counter.incrementAndGet();
          int newCounterValue = counter.getValue();

          group.writeTimestamp(message, round, clock, newCounter);
          if (RamcastConfig.LOG_ENABLED)
            logger.debug("[{}] wrote Ts [{}/{}/{}] of group {} to followers",
                    msgId,
                    round,
                    clock,
                    newCounterValue,
                    group.getAgent().getGroupId());
        }

      } catch (IOException e) {
        e.printStackTrace();
      }
    }

//    if (RamcastConfig.LOG_ENABLED) logger.trace("[{}] Process processing...", msgId);
    for (int groupIndex = 0; groupIndex < message.getGroupCount(); groupIndex++) {
      PendingTimestamp ts = new PendingTimestamp(message, groupIndex);
      pendingTimestamps.add(ts);
    }
    this.processing.add(message);
//    if (RamcastConfig.LOG_ENABLED)
      // for followers counter is going to always be zero
//      logger.debug("[{}] receive new message, counter {}, pendingTimestamps array {}",
//              msgId,
//              counter.get(),
//              pendingTimestamps);
  }

  //  public ConcurrentSkipListSet<RamcastMessage> getProcessing() {
  public Queue<RamcastMessage> getProcessing() {
    return processing;
  }

  public ConcurrentSkipListSet<RamcastMessage> getOrdered() {
    return ordered;
  }

  public void setRunning(boolean running) {
    isRunning = running;
  }

  private class PendingTimestamp {
    boolean shouldAck;
    boolean shouldPropagate = true;
    int groupId;
    int groupIndex;
    RamcastMessage message;
    boolean sendAck = false;
    int msgId;
    int timestampOffset = 0;

    public PendingTimestamp(RamcastMessage message, int groupIndex) {
      this.groupId = message.getGroup(groupIndex);
      this.shouldAck = groupId == agent.getGroupId();
      this.groupIndex = groupIndex;
      this.message = message;
      this.msgId = message.getId();
      this.timestampOffset = group.getTimestampBlock().getTimestampOffset(message, groupIndex);
    }

    public int getRound() {
      return group.getTimestampBlock().getBuffer().getInt(timestampOffset);
    }

    public int getClock() {
      return group.getTimestampBlock().getBuffer().getInt(timestampOffset + 4);
    }

    public int getCounter() {
      return group.getTimestampBlock().getBuffer().getInt(timestampOffset + 8);
    }

    public String toString() {
      return "id=" + message.getId() + ":[" + getRound() + "/" + getClock() + ", groupId:" + groupId + ", groupIndex:" + groupIndex + ", agent.groupId(): " + agent.getGroupId() + "]";
    }

    public boolean shouldAck() {
      return this.shouldAck;
    }

    public boolean isSendAck() {
      return this.sendAck;
    }

    public boolean shouldPropagate() {
      return this.shouldPropagate;
    }
  }
}
