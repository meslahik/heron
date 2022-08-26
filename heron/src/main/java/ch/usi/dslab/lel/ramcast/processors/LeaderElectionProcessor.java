package ch.usi.dslab.lel.ramcast.processors;

import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpointGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

public class LeaderElectionProcessor {
  private static final Logger logger = LoggerFactory.getLogger(LeaderElectionProcessor.class);
  RamcastAgent agent;
  RamcastEndpointGroup group;
  AtomicInteger acks;

  public LeaderElectionProcessor(RamcastEndpointGroup group, RamcastAgent agent) {
    this.agent = agent;
    this.group = group;
    this.acks = new AtomicInteger(0);
  }

  public void handleLeaderElectionMessage(RamcastEndpoint endpoint, ByteBuffer buffer)
          throws IOException {
    int ticket = buffer.getInt(0);
    if (ticket == RamcastConfig.MSG_HS_C_GET_WRITE) { // msg step 1 sent from client
      int groupId = buffer.getInt(4);
      int nodeId = buffer.getInt(8);
      int ballotNumnber = buffer.getInt(12);
      if (RamcastConfig.LOG_ENABLED)
        logger.debug("[HS] Step Request Write permission Server Receiving: [{}/{}], ballot={}", groupId, nodeId, ballotNumnber);

      // if request comes from different group => don't need to check ballot number.
      // other wise have to check
      // or request from that group itself
      if ((groupId == this.agent.getGroupId()
              && nodeId == this.agent.getNodeId())
              || (groupId != this.agent.getGroupId())
              || (this.group.getRound().get() < ballotNumnber)) {
        if (RamcastConfig.LOG_ENABLED)
          logger.debug("[HS] received ballot [{}] greater than current ballot [{}]. Granting permission.", ballotNumnber, this.group.getRound());

        if (this.group.getRound().get() < ballotNumnber
                && groupId == this.agent.getGroupId()) {
          this.group.seRoundNumber(ballotNumnber);
        }
        // if request comes from same group => need to revoke permission of other nodes
        if (groupId == this.agent.getGroupId())
          this.group.revokeTimestampWritePermission();

        // set that node as leader
        this.agent.setLeader(groupId, nodeId);

        RamcastMemoryBlock memoryBlock = endpoint.registerTimestampWritePermission();
        ByteBuffer response = ByteBuffer.allocateDirect(28);
        response.putInt(RamcastConfig.MSG_HS_S_GET_WRITE);
        response.putLong(memoryBlock.getAddress());
        response.putInt(memoryBlock.getLkey());
        response.putInt(memoryBlock.getCapacity());

        // need to also send the timestamps of the pending messages
        Collection<RamcastMessage> pending = this.group.getMessageProcessor().getProcessing();
        Collection<RamcastMessage> ordered = this.group.getMessageProcessor().getOrdered();

        // add number of pending msg to the 1b
        response.putInt(pending.size());
        response.putInt(ordered.size());

        if (pending.size() > 0) {
          logger.debug("[HS] Current pending messages size {}: {}", pending.size(), pending);
          logger.debug("[HS] Filtering pending messages for node/group [{}/{}]", nodeId, groupId);
//          for (RamcastMessage message : pending) {
//            if (message.getGroupIndex(groupId) != -1) {
//              logger.debug("[HS] Current pending messages for group {}: {}", groupId, message);
//              response.putInt(message.getId());
//              int[] ts = group.getTimestampBlock().getTs(message, this.agent.getGroupId());
//              response.putInt(message.getId());
//            }
//          }
        }
        if (ordered.size() > 0) {
          logger.debug("[HS] Current order messages size {}: {}", ordered.size(), ordered);
        }
        endpoint.send(response);
      }
      endpoint.setHasExchangedPermissionData(true);
    } else if (ticket == RamcastConfig.MSG_HS_S_GET_WRITE) { // msg step 1 sent from client
      endpoint.setRemoteSharedTimestampMemoryBlock(buffer.getLong(4), buffer.getInt(12), buffer.getInt(16));
      int ack = acks.incrementAndGet();
      endpoint.setHasExchangedPermissionData(true);
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
                "[HS] Step Request Write permission CLIENT Receiving {}/{} from {}: timestampBlock addr={} lkey={} capacity={} pending={}, ordered={}",
                ack,
                RamcastGroup.getTotalNodeCount(),
                endpoint.getNode(),
                buffer.getLong(4),
                buffer.getInt(12),
                buffer.getInt(16),
                buffer.getInt(20),
                buffer.getInt(24)
        );

      // need to process the pending msgs that the remote is sending back

    } else {
      throw new IOException("Protocol msg code not found :" + ticket);
    }
  }

  public void requestWritePermission(RamcastEndpoint endpoint, int ballotNumber)
          throws IOException {
    ByteBuffer buffer = ByteBuffer.allocateDirect(16);
    buffer.putInt(RamcastConfig.MSG_HS_C_GET_WRITE);
    buffer.putInt(this.agent.getGroupId());
    buffer.putInt(this.agent.getNodeId());
    buffer.putInt(ballotNumber);
    if (RamcastConfig.LOG_ENABLED)
      logger.debug(
              "[HS] Step Request Write permission CLIENT Sending: to {} with data [{}/{}], ballot={}",
              endpoint.getNode(),
              buffer.getInt(4),
              buffer.getInt(8),
              buffer.getInt(12));
    endpoint.send(buffer);
  }

  public AtomicInteger getAcks() {
    return acks;
  }
}
