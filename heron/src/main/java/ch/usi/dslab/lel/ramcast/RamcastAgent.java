package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpointGroup;
import ch.usi.dslab.lel.ramcast.heron.HeronMemoryBlock;
import ch.usi.dslab.lel.ramcast.heron.HeronRecoveryMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvMr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RamcastAgent {
  private static final Logger logger = LoggerFactory.getLogger(RamcastAgent.class);
  RamcastConfig config = RamcastConfig.getInstance();

  private RamcastEndpointGroup endpointGroup;

  private RamcastNode node;
  private RdmaServerEndpoint<RamcastEndpoint> serverEp;

  private RamcastNode leader;
  private MessageDeliveredCallback onDeliverCallback;

  private RamcastMessage lastDelivered;

  public RamcastAgent(int groupId, int nodeId) throws Exception {
    this(
            groupId,
            nodeId,
            data -> {
              if (RamcastConfig.LOG_ENABLED) logger.debug("Delivered message {}", data);
            });
  }

  public RamcastAgent(int groupId, int nodeId, MessageDeliveredCallback onDeliverCallback)
          throws Exception {
    MDC.put("ROLE", groupId + "/" + nodeId);
    this.node = RamcastNode.getNode(groupId, nodeId);
    this.onDeliverCallback = onDeliverCallback;
  }

  public void bind() throws Exception {
    // listen for new connections
    this.endpointGroup = RamcastEndpointGroup.createEndpointGroup(this, config.getTimeout());
    this.serverEp = endpointGroup.createServerEndpoint();

    try {
      this.serverEp.bind(this.node.getInetAddress(), 100);
    } catch (Exception ex) {
      logger.error("cannot bind in this address: " + this.node.getInetAddress());
      ex.printStackTrace();
    }
    Thread listener =
            new Thread(
                    () -> {
                      MDC.put("ROLE", node.getGroupId() + "/" + node.getNodeId());
                      while (!Thread.interrupted()) {
                        try {
                          RamcastEndpoint endpoint = this.serverEp.accept();
                          while (!endpoint.isReady()) Thread.sleep(10);
                          if (RamcastConfig.LOG_ENABLED)
                            logger.debug(
                                    ">>> Server accept connection of endpoint {}. CONNECTION READY", endpoint);
                        } catch (IOException | InterruptedException e) {
                          e.printStackTrace();
                          logger.error("Error accepting connection", e);
                        }
                      }
                    });
    listener.setName("ConnectionListener");
    listener.start();
  }

  public void establishHeronBlock() throws Exception {
    // heron
    ByteBuffer heronBuffer = endpointGroup.getSharedHeronBuffer();
    if (heronBuffer == null)
      throw new Exception("heron buffer is null");
    IbvMr mr = serverEp.registerMemory(heronBuffer).execute().free().getMr();
    HeronMemoryBlock heronBlock = new HeronMemoryBlock(
            mr.getAddr(),
            mr.getLkey(),
            mr.getRkey(),
            mr.getLength(),
            heronBuffer);
    logger.debug("heron block declaration, block:{}, buffer: {}", heronBlock, heronBuffer);
    endpointGroup.setSharedHeronBlock(heronBlock);

    // heron recovery
    ByteBuffer heronRecoveryBuffer = endpointGroup.getSharedHeronRecoveryBuffer();
    if (heronRecoveryBuffer == null)
      throw new Exception("heron recovery buffer is null");
    IbvMr mr2 = serverEp.registerMemory(heronRecoveryBuffer).execute().free().getMr();
    HeronRecoveryMemoryBlock heronRecoveryBlock = new HeronRecoveryMemoryBlock(
            mr2.getAddr(),
            mr2.getLkey(),
            mr2.getRkey(),
            mr2.getLength(),
            heronRecoveryBuffer);
    logger.debug("heron recovery block declaration, block:{}, buffer: {}", heronRecoveryBlock, heronRecoveryBuffer);
    endpointGroup.setSharedHeronRecoveryBlock(heronRecoveryBlock);
  }

  public RdmaServerEndpoint<RamcastEndpoint> getServerEp() {
    return serverEp;
  }

  public void establishConnections() throws Exception {

    this.endpointGroup.connect();
    while (!Thread.interrupted()) {
      Thread.sleep(10);
      // todo: find nicer way for -1
      if (this.getEndpointMap().keySet().size() != RamcastGroup.getTotalNodeCount()) continue;
      if (this.getEndpointMap().values().stream()
              .map(RamcastEndpoint::isReady)
              .reduce(Boolean::logicalAnd)
              .get()) break;
    }

    if (this.shouldBeLeader()) {
      this.endpointGroup.requestWritePermission();
    }

    this.endpointGroup.startPollingData();

    while (!Thread.interrupted()) {
      Thread.sleep(10);
      // todo: find nicer way for -1
      if (this.getEndpointMap().keySet().size() != RamcastGroup.getTotalNodeCount()) continue;
      if (this.leader == null) continue;
      if (this.getEndpointMap().values().stream()
              .map(RamcastEndpoint::hasExchangedPermissionData)
              .reduce(Boolean::logicalAnd)
              .get()) break;
    }

    if (RamcastConfig.LOG_ENABLED)
      logger.info(
              "Agent of node {} is ready. EndpointMap: Key:{} Vlue {}, leader {}",
              this.node,
              this.getEndpointMap().keySet(),
              this.getEndpointMap().values(),
              this.leader);
  }

  public Map<RamcastNode, RamcastEndpoint> getEndpointMap() {
    return this.endpointGroup.getNodeEndpointMap();
  }

  public RamcastNode getNode() {
    return node;
  }

  public RamcastEndpointGroup getEndpointGroup() {
    return endpointGroup;
  }

  @Override
  public String toString() {
    return "RamcastAgent{" + "node=" + node + '}';
  }

  // FOR TESTING ONLY
  public void writeSync(RamcastMessage message, List<RamcastGroup> destinations)
          throws IOException {
    for (RamcastGroup group : destinations) {
      this.endpointGroup.writeTestMessage(group, message.toBuffer());
    }
    while (!isDestReadyForTestMessage(message.getId())) {
      Thread.yield();
    }
  }

  public void multicast(RamcastMessage message, List<RamcastGroup> destinations)
          throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.debug("Multicasting to dest {} message {}", destinations, message);
    for (RamcastGroup group : destinations) {
      this.endpointGroup.writeMessage(group, message.toBuffer());
    }
  }

  public void multicastSync(RamcastMessage message, List<RamcastGroup> destinations)
          throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.info("Multicasting synced to dest {} message {}", destinations, message);
    for (RamcastGroup group : destinations) {
      this.endpointGroup.writeMessage(group, message.toBuffer());
    }

    // todo: make it synced. how?
    while (!isAllDestReadyForMessage(destinations, message.getId())) {
      Thread.yield();
    }
  }


  public RamcastMessage createMessage(
          int msgId, ByteBuffer buffer, List<RamcastGroup> destinations) {
    int[] groups = new int[destinations.size()];
    destinations.forEach(group -> groups[destinations.indexOf(group)] = group.getId());
    RamcastMessage message = new RamcastMessage(buffer, groups);
    message.setId(msgId);
    this.setSlot(message, destinations);
    return message;
  }

  public void setSlot(RamcastMessage message, List<RamcastGroup> destinations) {
    short[] slots = getRemoteSlots(destinations);
    while (slots == null) {
      Thread.yield();
      slots = getRemoteSlots(destinations);
    }
    message.setSlots(slots);

    // todo: is this correct
//    short[] fullSlots = new short[4];
//    for (int i=0; i< slots.length; i++)
//      fullSlots[i] = slots[i];
//    message.setSlots(fullSlots);
  }

  public RamcastMessage createMessage(ByteBuffer buffer, List<RamcastGroup> destinations) {
    return createMessage(Objects.hash(this.node.getOrderId()), buffer, destinations);
  }

  private short[] getRemoteSlots(List<RamcastGroup> destinations) {
    short[] ret = new short[destinations.size()];
    int i = 0;
    for (RamcastGroup group : destinations) {
      int tail = -1;
      int available = 0;
      List<RamcastEndpoint> eps = endpointGroup.getGroupEndpointsMap().get(group.getId());
      for (RamcastEndpoint ep : eps) {
        if (ep.getAvailableSlots() <= 0) return null;
        tail = ep.getRemoteCellBlock().getTailOffset();
        break;
        //        if (tail < ep.getRemoteCellBlock().getTailOffset()
        //            && (available < ep.getAvailableSlots())) {
        //          tail = ep.getRemoteCellBlock().getTailOffset();
        //          available = ep.getAvailableSlots();
        //        }
      }
      ret[i++] = (short) tail;
    }
    return ret;
  }

  public void close() throws IOException, InterruptedException {
    if (RamcastConfig.LOG_ENABLED) logger.info("Agent of {} is closing down", this.node);
    this.serverEp.close();
    this.endpointGroup.close();
    RamcastGroup.close();
  }

  public void setLeader(int groupId, int nodeId) throws IOException {
    RamcastNode.removeLeader(groupId);
    RamcastNode node = RamcastNode.getNode(groupId, nodeId);
    assert node != null;
    node.setLeader(true);
    if (groupId == this.getGroupId()) this.leader = node;
    // if this is the new leader
    if (this.leader != null && this.leader.equals(this.node)) {
      // need to revoke permission of old leader
      this.getEndpointGroup().revokeTimestampWritePermission();
    }
  }

  public boolean isLeader() {
    return this.node.equals(leader);
  }

  public boolean shouldBeLeader() {
    return this.node.getNodeId() == this.getNode().getGroup().getLeaderId();
  }

  public void deliver(RamcastMessage message) throws IOException {
    endpointGroup.releaseTimestamp(message);
    if (RamcastConfig.LOG_ENABLED)
      logger.debug(
              "[{}] Delivered at ts {} !!!",
              message.getId(),
              message.getFinalTs()
      );
    onDeliverCallback.call(message);
    this.endpointGroup.releaseMemory(message);
  }

  public boolean hasClientRole() {
    return this.node.hasClientRole();
  }

  public int getGroupId() {
    return this.node.getGroupId();
  }

  public int getNodeId() {
    return this.node.getNodeId();
  }

  // this will wait until specific message is processed OR queue has space
  public boolean isAllDestReady(List<RamcastGroup> dests, int msgId) {
    for (RamcastGroup group : dests) {
      if (!endpointGroup.allEndpointReady(group.getId(), msgId)) {
        return false;
      }
    }
    return true;
  }

  // this will wait until specific message is processed
  public boolean isAllDestReadyForMessage(List<RamcastGroup> dests, int msgId) {
    for (RamcastGroup group : dests) {
      if (!endpointGroup.allEndpointReadyForMessage(group.getId(), msgId)) {
        return false;
      }
    }
    return true;
  }

  // this will wait until specific message is processed
  // FOR TESTING ONLY
  public boolean isDestReadyForTestMessage(int msgId) {
    if (!endpointGroup.endpointReadyForTestMessage(msgId)) {
      return false;
    }
    return true;
  }

  // FOR TESTING ONLY
  public void deliverWrite(RamcastMessage message) throws IOException {
    endpointGroup.releaseTimestamp(message);
    if (RamcastConfig.LOG_ENABLED)
      logger.debug(
              "[{}] Delivered at ts {} !!!",
              message.getId(),
              message.getFinalTs()
      );
    onDeliverCallback.call(message);

    // write back to the client
    RamcastMemoryBlock memoryBlock = message.getMemoryBlock();
    int freed = memoryBlock.freeSlot(message.getGroupSlot(message.getGroupIndex(this.getGroupId())));
    RamcastEndpoint endpoint = message.getMemoryBlock().getEndpoint();

    if (freed == 0) {
      return;
    }
    RamcastMemoryBlock clientBlock = endpoint.getRemoteServerHeadBlock();
    ByteBuffer buffer = message.getBuffer();
    if (RamcastConfig.LOG_ENABLED)
      logger.debug("[{}] Reply buffer size {} !!!", message.getId(), buffer.capacity());
    buffer.putInt(0, freed);
    buffer.putInt(4, message.getId());
    endpoint.writeTestResponse(
            clientBlock.getAddress(),
            clientBlock.getLkey(),
            buffer);
    message.reset();
  }
}
