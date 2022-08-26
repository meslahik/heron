package ch.usi.dslab.lel.ramcast.endpoint;

import ch.usi.dslab.lel.dynastar.tpcc.tables.Tables;
import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.heron.HeronConfig;
import ch.usi.dslab.lel.ramcast.heron.HeronMemoryBlock;
import ch.usi.dslab.lel.ramcast.heron.HeronRecoveryMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.*;
import ch.usi.dslab.lel.ramcast.processors.HandshakingProcessor;
import ch.usi.dslab.lel.ramcast.processors.LeaderElectionProcessor;
import ch.usi.dslab.lel.ramcast.processors.MessageProcessor;
import com.ibm.disni.RdmaCqProcessor;
import com.ibm.disni.RdmaCqProvider;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.util.MemoryUtils;
import com.ibm.disni.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RamcastEndpointGroup extends RdmaEndpointGroup<RamcastEndpoint> {

  private static final Logger logger = LoggerFactory.getLogger(RamcastEndpointGroup.class);
  private RamcastConfig config = RamcastConfig.getInstance();
  private RamcastAgent agent;

  private HashMap<Integer, RamcastCqProcessor<RamcastEndpoint>> cqMap;
  private int timeout;

  // for leader election
  private AtomicInteger round;
  //  private AtomicInteger currentSequenceNumber; // for checking msg when receive
  private AtomicVectorClock clock; // for clock for ordering msg

  // storing memory segment block associated with each endpoint
  //  private Map<RamcastEndpoint, RamcastMemoryBlock> endpointMemorySegmentMap;

  // storing all endponints of all nodes
  private Map<RamcastNode, RamcastEndpoint> nodeEndpointMap;
  private Map<Integer, RamcastEndpoint> incomingEndpointMap;
  private List<RamcastEndpoint> incomingEndpoints;
  private Map<Integer, List<RamcastEndpoint>> groupEndpointsMap;

  // shared memory for receiving message from clients
  private ByteBuffer sharedCircularBuffer;

  // shared memory for receiving timestamp from leaders
  private ByteBuffer sharedTimestampBuffer;
  private RamcastTsMemoryBlock timestampBlock;

  // mojtaba: heron buffer and block
  private ByteBuffer sharedHeronBuffer;
  private HeronMemoryBlock sharedHeronBlock;

  // mojtaba: heron recovery buffer and block
  private ByteBuffer sharedHeronRecoveryBuffer;
  private HeronRecoveryMemoryBlock sharedHeronRecoveryBlock;

  // mojtaba: tpcc tables
  public Tables tables;
  public Tables[][] remoteTables;

  private HandshakingProcessor handshakingProcessor;
  private LeaderElectionProcessor leaderElectionProcessor;
  private MessageProcessor messageProcessor;

  private CustomHandler customHandler;

  public RamcastEndpointGroup(RamcastAgent agent, int timeout) throws IOException {
    super(timeout);
    this.timeout = timeout;
    this.agent = agent;
    this.sharedCircularBuffer =
            allocateSharedBuffer(
                    RamcastGroup.getTotalNodeCount(), config.getQueueLength(), RamcastConfig.SIZE_MESSAGE);
    this.sharedTimestampBuffer =
            allocateShareTimestampdBuffer(
                    RamcastGroup.getTotalNodeCount(),
                    config.getQueueLength(),
                    RamcastConfig.SIZE_TIMESTAMP);
    this.timestampBlock =
            new RamcastTsMemoryBlock(0, 0, sharedTimestampBuffer.capacity(), sharedTimestampBuffer);

    // mojtaba: heron
    this.sharedHeronBuffer =
            ByteBuffer.allocateDirect(
                    RamcastConfig.getInstance().getGroupCount() * RamcastConfig.getInstance().getNodePerGroup() * HeronConfig.SIZE_COORD);
    logger.trace("Heron buffer declaration: {}", MemoryUtils.getAddress(sharedHeronBuffer));

    // mojtaba: heron recovery
    this.sharedHeronRecoveryBuffer =
            ByteBuffer.allocateDirect(
                    RamcastConfig.getInstance().getGroupCount() * RamcastConfig.getInstance().getNodePerGroup() * HeronConfig.SIZE_COORD);
    logger.trace("Heron recovery buffer declaration: {}", MemoryUtils.getAddress(sharedHeronRecoveryBuffer));

    this.nodeEndpointMap = new ConcurrentHashMap<>();
    this.incomingEndpointMap = new ConcurrentHashMap<>();
    this.groupEndpointsMap = new ConcurrentHashMap<>();
    this.cqMap = new HashMap<>();
    this.handshakingProcessor = new HandshakingProcessor(this, agent);
    this.leaderElectionProcessor = new LeaderElectionProcessor(this, agent);
    this.messageProcessor = new MessageProcessor(this, agent);
    this.round = new AtomicInteger(0);
//    this.sequenceNumber = new AtomicInteger(0);
//    this.currentSequenceNumber = new AtomicInteger(0);
    this.incomingEndpoints = new ArrayList<>(0);
    this.clock = new AtomicVectorClock(agent.getGroupId(), 0);
    //    this.endpointMemorySegmentMap = new ConcurrentHashMap<>();
  }

  public void setTpccTables(Tables tables) {
    this.tables = tables;
  }

  public void setRemoteTpccTables(Tables[][] tables) {
    this.remoteTables = tables;
  }

  public Tables getTpccTables() {
    return tables;
  }

  public Tables[][] getRemoteTables() {
    return remoteTables;
  }

  public static RamcastEndpointGroup createEndpointGroup(RamcastAgent agent, int timeout)
          throws Exception {
    RamcastEndpointGroup group = new RamcastEndpointGroup(agent, timeout);
    group.init(new RamcastEndpointFactory(group));
    return group;
  }

  public void send(int groupId, int nodeId, ByteBuffer buffer) throws IOException {
    send(RamcastNode.getNode(groupId, nodeId), buffer);
  }

  public void send(RamcastNode node, ByteBuffer buffer) throws IOException {
    this.nodeEndpointMap.get(node).send(buffer);
  }

  public void writeMessage(RamcastNode node, ByteBuffer buffer) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("Write message to {},  ep {}", node, this.nodeEndpointMap);
    this.nodeEndpointMap.get(node).writeMessage(buffer);
  }

  // FOR TESTING ONLY
  public void writeTestMessage(RamcastGroup group, ByteBuffer buffer) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "Write message to {}, buffer {}, ep {}",
              group,
              buffer,
              this.groupEndpointsMap.get(group.getId()));
    for (RamcastEndpoint endpoint : this.getGroupEndpointsMap().get(group.getId())) {
      if (endpoint.getNode().getNodeId() == 0)
        endpoint.writeMessage(buffer);
    }
  }

  public void writeMessage(RamcastGroup group, ByteBuffer buffer) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "Write message to {}, buffer {}, ep {}",
              group,
              buffer,
              this.groupEndpointsMap.get(group.getId()));
    for (RamcastEndpoint endpoint : this.getGroupEndpointsMap().get(group.getId())) {
      endpoint.writeMessage(buffer);
    }
  }

  public void writeRecoveryData(
          SVCPostSend postSend, int replicaId, long address, int lkey, int rkey, ByteBuffer buffer, int capacity)
          throws IOException {
    for (RamcastEndpoint endpoint : this.groupEndpointsMap.get(agent.getGroupId())) {
      if (endpoint.getNode().getNodeId() == replicaId) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
                  "Write message to group {} replica {}, buffer {}, ep {}",
                  agent.getGroupId(),
                  replicaId,
                  buffer,
                  endpoint);
        endpoint.writeRecoveryData(postSend, address, lkey, rkey, buffer, capacity);
      }
    }
  }

  public SVCPostSend getSVCPostSend(int replicaId) {
    for (RamcastEndpoint endpoint : this.groupEndpointsMap.get(agent.getGroupId()))
      if (endpoint.getNode().getNodeId() == replicaId)
        return endpoint.getSVCPostSend();

    return null;
  }

  public void handleProtocolMessage(RamcastEndpoint endpoint, ByteBuffer buffer)
          throws IOException {
    if (buffer.getInt(0) < 0 && buffer.getInt(0) >= -10) { // hack: hs message has ID from -1 -> -10
      this.handshakingProcessor.handleHandshakeMessage(endpoint, buffer);
    } else if (buffer.getInt(0) < -10
            && buffer.getInt(0) >= -20) { // hack: hs message has ID from -11 -> -20
      this.leaderElectionProcessor.handleLeaderElectionMessage(endpoint, buffer);
    }
  }

  public void handleReadCompletion() {
    synchronized (Tables.isRemoteReadComplete) {
      Tables.isRemoteReadComplete.notify();
      Tables.isRemoteReadComplete = true;
    }
//    if (RamcastConfig.LOG_ENABLED)
    logger.trace("dispatch Read of buffer");
  }

  public void handleReceive(RamcastEndpoint endpoint, ByteBuffer buffer) {
    if (customHandler != null) customHandler.handleReceive(buffer);
  }

  public void handleSendComplete(RamcastEndpoint endpoint, ByteBuffer buffer) {
    if (customHandler != null) customHandler.handleSendComplete(buffer);
  }

  public void handleReceiveMessage(RamcastMessage message) throws IOException {
    if (customHandler != null) customHandler.handleReceiveMessage(message);
    this.messageProcessor.handleMessage(message);
  }

  public void handlePermissionError(ByteBuffer buffer) {
    if (customHandler != null) customHandler.handlePermissionError(buffer);
  }

  public void initHandshaking(RamcastEndpoint endpoint) throws IOException {
    this.handshakingProcessor.initHandshaking(endpoint);
  }

  public void requestWritePermission() throws IOException, InterruptedException {
    this.round.incrementAndGet();
    this.leaderElectionProcessor.getAcks().set(0);
    for (RamcastEndpoint endpoint : this.getNodeEndpointMap().values()) {
      this.leaderElectionProcessor.requestWritePermission(endpoint, this.round.get());
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(">>> Client exchanged permission data to: {}.", endpoint.getNode());
    }
    // wait for receiving acks from all nodes
    while (this.leaderElectionProcessor.getAcks().get()
            // todo: find nicer way for -1
            != RamcastGroup.getTotalNodeCount()) {
      logger.trace(
              ">>> Waiting for pending acks. {}/{}",
              this.leaderElectionProcessor.getAcks().get(),
              RamcastGroup.getTotalNodeCount()
      );
      Thread.sleep(100);
    }
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              ">>> Client FINISHED exchanging permission to {} nodes.",
              this.leaderElectionProcessor.getAcks().get());
  }

  public void startPollingData() {
    Map<String, String> contextMap = MDC.getCopyOfContextMap();
    Thread serverDataPolling =
            new Thread(
                    () -> {
                      MDC.setContextMap(contextMap);
                      if (RamcastConfig.LOG_ENABLED) logger.info("Polling for incoming data");
                      while (!Thread.interrupted()) {
                        for (int i = 0; i < incomingEndpoints.size(); i++) {
                          if (incomingEndpoints.get(i) != null) {
                            incomingEndpoints.get(i).pollForData();
                          }
                        }
                        Thread.yield();
                      }
                    });
    serverDataPolling.setName("ServerDataPolling");
    serverDataPolling.start();
  }

  public void releaseTimestamp(RamcastMessage message) {
    // free ts
    timestampBlock.freeTimestamp(message);
  }

  public void releaseMemory(RamcastMessage message) throws IOException {
    RamcastMemoryBlock memoryBlock = message.getMemoryBlock();

    // need to check if this message is eligible to be released: all processes in the quorum have acked
//    while (!message.isAcked(this.getBallotNumber().get())) Thread.yield();

    //    int freed = memoryBlock.freeSlot(message.getSlotOfGroupId(agent.getGroupId()));
    int slot = message.getGroupSlot(message.getGroupIndex(agent.getGroupId()));
    int freed = memoryBlock.freeSlot(slot);
    RamcastEndpoint endpoint = message.getMemoryBlock().getEndpoint();

    if (freed == 0) {
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}] Can't release memory slot. There are pending slots", endpoint.getEndpointId());
      return;
    }
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "[{}] SERVER MEMORY after releasing memory: {}",
              endpoint.getEndpointId(),
              endpoint.getSharedCellBlock());

    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "[{}] Released memory of {} slot. Update client [{}].", message.getId(), freed, endpoint);
    this.writeRemoteHeadOnClient(endpoint, freed, message.getId());
    message.reset();
  }

  // a node writes its timestamp to ts mem of remote node, at its index
  public void writeTimestamp(RamcastMessage message, int round, int clock, int counter) throws IOException {
    // for each group in the destination
    int thisGroupIndex = message.getGroupIndex(this.agent.getGroupId());
    for (int groupIndex = 0; groupIndex < message.getGroupCount(); groupIndex++) {
      // target groupId
      int groupId = message.getGroup(groupIndex);
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}] SERVER PUT TS of group [{}] to memory of group [{}] with value [{}/{}]",
                message.getId(),
                agent.getGroupId(),
                groupId,
                round,
                clock);
      // for each endpoint of the destination group
      for (RamcastEndpoint endpoint : groupEndpointsMap.get(groupId)) {
        if (endpoint == null) continue; // endpoint could be null due to failed process
        // only update other leaders + processes of this node's own group
        if (endpoint.getNode().isLeader() || endpoint.getGroupId() == agent.getGroupId()) {
          if (endpoint.getNode().equals(agent.getNode())) { // if the node is writing to itself -> do local
            if (RamcastConfig.LOG_ENABLED)
              logger.trace(
                      "[{}] group [{}] updates ts value [{}/{}/{}] for its local buffer on {} at index {} epi {}",
                      message.getId(),
                      agent.getGroupId(),
                      round,
                      clock,
                      counter,
                      endpoint.getNode(),
                      thisGroupIndex,
                      endpoint.getEndpointId());
            timestampBlock.writeLocalTs(message, thisGroupIndex, round, clock, counter);
          } else {
            if (RamcastConfig.LOG_ENABLED)
              logger.trace(
                      "[{}] group [{}] updates ts value [{}/{}/{}] on {} at index {} epi {}",
                      message.getId(),
                      agent.getGroupId(),
                      round,
                      clock,
                      counter,
                      endpoint.getNode(),
                      thisGroupIndex,
                      endpoint.getEndpointId());
            writeTimestamp(endpoint, message, thisGroupIndex, round, clock, counter);
          }
        }
      }
    }
  }

  public void writeTimestamp(RamcastEndpoint endpoint, RamcastMessage message, int groupIndex, int round, int clock, int counter) throws IOException {
    RamcastTsMemoryBlock timestampBlock = endpoint.getRemoteTimeStampBlock();
    long address = timestampBlock.getNodeTimestampAddress(message, groupIndex);
    endpoint.writeSignal(
            address,
            timestampBlock.getLkey(),
            Integer.TYPE,
            round,
            Integer.TYPE,
            clock,
            Integer.TYPE,
            counter);
  }

  // for unit test
  public void writeTimestamp(
          RamcastEndpoint endpoint,
          int slot,
          int groupIndex,
          int round,
          int clock)
          throws IOException {
    RamcastTsMemoryBlock timestampBlock = endpoint.getRemoteTimeStampBlock();
    long address = timestampBlock.getNodeTimestampAddress(slot, endpoint.getNode(), groupIndex);
    endpoint.writeSignal(
            address,
            timestampBlock.getLkey(),
            Integer.TYPE,
            round,
            Integer.TYPE,
            clock);
  }

  public void writeRemoteHeadOnClient(RamcastEndpoint endpoint, int headOffset, int msgId)
          throws IOException {
    RamcastMemoryBlock clientBlock = endpoint.getRemoteServerHeadBlock();
    endpoint.writeSignal(
            clientBlock.getAddress(),
            clientBlock.getLkey(),
            Integer.TYPE,
            headOffset,
            Integer.TYPE,
            msgId);
  }

  public void leaderPropageTs(RamcastMessage message, int round, int clock, int groupId, int groupIndex, int counter)
          throws IOException {
    if (RamcastConfig.LOG_ENABLED) logger.trace(
            "[{}] leader of group {} propagating timestamp [{}/{}/{}] of group {} index {} ",
            message.getId(),
            agent.getGroupId(),
            round,
            clock,
            counter,
            groupId,
            groupIndex);

    for (RamcastEndpoint endpoint : groupEndpointsMap.get(agent.getGroupId())) {
      if (endpoint == null) continue;
      if (endpoint.getNode() == agent.getNode()) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
                  "[{}] group [{}] propagate ts value [{}/{}/{}] for its local buffer on {} at index {} epi {}",
                  message.getId(),
                  agent.getGroupId(),
                  round,
                  clock,
                  counter,
                  endpoint.getNode(),
                  groupIndex,
                  endpoint.getEndpointId());
        timestampBlock.writeLocalTs(message, groupIndex, round, clock, counter);
      } else {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
                  "[{}] group [{}] propagate ts value [{}/{}/{}] on {} at index {} epi {}",
                  message.getId(),
                  agent.getGroupId(),
                  round,
                  clock,
                  counter,
                  endpoint.getNode(),
                  groupIndex,
                  endpoint.getEndpointId());
        writeTimestamp(endpoint, message, groupIndex, round, clock, counter);
      }
    }
  }

  public void sendAck(
          RamcastMessage message, int ballotNumber, int sequenceNumber, int groupId, int groupIndex)
          throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "[{}] {} sending acks for timestamp [{}/{}] of group {} index {}",
              message.getId(),
              agent.getNode(),
              ballotNumber,
              sequenceNumber,
              groupId,
              groupIndex);
    for (int i = 0; i < message.getGroupCount(); i++) {
      synchronized (groupEndpointsMap.get((int) message.getGroup(i))) {
        for (RamcastEndpoint endpoint : groupEndpointsMap.get((int) message.getGroup(i))) {
          if (endpoint == null) continue;
          if (endpoint.getNode() == agent.getNode()) {
            if (RamcastConfig.LOG_ENABLED)
              logger.trace(
                      "[{}] {} sending acks for timestamp [{}/{}] of group {} index {} to local buffer {}",
                      message.getId(),
                      agent.getNode(),
                      ballotNumber,
                      sequenceNumber,
                      groupId,
                      groupIndex,
                      endpoint.getNode());
            message.writeAck(endpoint.getNode(), ballotNumber, sequenceNumber);
            if (RamcastConfig.LOG_ENABLED)
              logger.trace("[{}] message after writing local ack {}", message.getId(), message);
          } else {
            if (RamcastConfig.LOG_ENABLED)
              logger.trace(
                      "[{}] {} sending acks for timestamp [{}/{}] of group {} index {} to remote memory {}",
                      message.getId(),
                      agent.getNode(),
                      ballotNumber,
                      sequenceNumber,
                      groupId,
                      groupIndex,
                      endpoint.getNode());
            writeAck(endpoint, message, ballotNumber, sequenceNumber);
          }
        }
      }
    }
  }

  private void writeAck(
          RamcastEndpoint endpoint, RamcastMessage message, int ballotNumber, int sequenceNumber)
          throws IOException {
    // need to get memory segment in the shared memory of remote node that store msg of that node
    int blockSize = RamcastConfig.getInstance().getQueueLength() * RamcastConfig.SIZE_MESSAGE;
    int pos =
            message.getSource().getGroupId() * RamcastConfig.getInstance().getNodePerGroup() * blockSize
                    + message.getSource().getNodeId() * blockSize;

//    int slot = message.getSlot() * RamcastConfig.SIZE_MESSAGE;
//    int slot = message.getSlotOfGroupId(endpoint.getGroupId()) * RamcastConfig.SIZE_MESSAGE
    int slot = message.getGroupSlot(message.getGroupIndex(endpoint.getGroupId()));
    int slotOffset = slot * RamcastConfig.SIZE_MESSAGE;

    int offset = message.getPosAck(agent.getNode());

    long address =
            endpoint.getRemoteCircularBlock().getAddress()
                    + pos
                    + slotOffset
                    + offset
                    + RamcastConfig.SIZE_BUFFER_LENGTH; // IMPORTANT: because msg is shifted 4 bytes to the right => need to add 4 bytes here

    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "[{}] write ack to {} at slot {}, base address {}, pos in shared segment: {}, absolute addr {}, offset of this ack {}, absolute add of ack {}",
              message.getId(),
              endpoint.getNode(),
              slot,
              endpoint.getRemoteCircularBlock().getAddress(),
              pos,
              pos + endpoint.getRemoteCircularBlock().getAddress(),
              offset,
              address);

    endpoint.writeSignal(
            address,
            endpoint.getRemoteCellBlock().getLkey(),
            Integer.TYPE,
            ballotNumber,
            Integer.TYPE,
            sequenceNumber);
  }

  public boolean allEndpointReadyForMessage(int groupId, int msgId) {
    for (RamcastEndpoint endpoint : groupEndpointsMap.get(groupId)) {
      if (endpoint == null) continue;
      if (endpoint.getCompletionSignal() != msgId) {
        if (RamcastConfig.LOG_ENABLED && System.currentTimeMillis() % 100 == 0)
          logger.trace(
                  "[{}] endpoint of group [{}] node {} is not ready for message {} Completion signal:{}",
                  endpoint.getEndpointId(),
                  groupId,
                  endpoint.getNode(),
                  msgId,
                  endpoint.getCompletionSignal());
        return false;
      }
    }
    return true;
  }

  // FOR TESTING ONLY
  public boolean endpointReadyForTestMessage(int msgId) {
    for (RamcastEndpoint endpoint : groupEndpointsMap.get(0)) {
      if (endpoint == null) continue;
      if (endpoint.getNode().getNodeId() != 0) continue;
      if (endpoint.getCompletionSignal() != msgId) {
        if (RamcastConfig.LOG_ENABLED && System.currentTimeMillis() % 1000 == 0)
          logger.trace(
                  "[{}] endpoint of group [{}] node {} is not ready for message {} Completion signal:{}",
                  endpoint.getEndpointId(),
                  0,
                  endpoint.getNode(),
                  msgId,
                  endpoint.getCompletionSignal());
        return false;
      }
    }
    return true;
  }

  public boolean allEndpointReady(int groupId, int msgId) {
    for (RamcastEndpoint endpoint : groupEndpointsMap.get(groupId)) {
      if (endpoint == null) continue;
      if (endpoint.getCompletionSignal() != -1 && endpoint.getCompletionSignal() != msgId) {
        if (RamcastConfig.LOG_ENABLED && System.currentTimeMillis() % 100 == 0)
          logger.trace(
                  "[{}] endpoint of group [{}] node {} is not ready for message {} Completion signal:{}",
                  endpoint.getEndpointId(),
                  groupId,
                  endpoint.getNode(),
                  msgId,
                  endpoint.getCompletionSignal());
        return false;
      }
    }
    return true;
  }

//  public boolean allEndpointReady(int groupId, int msgId) {
//    boolean ready = false;
//    for (RamcastEndpoint endpoint : groupEndpointsMap.get(groupId)) {
//      if (endpoint == null) continue;
//      if (endpoint.getCompletionSignal() != -1 && endpoint.getCompletionSignal() == msgId) return true;
//    }
//    return false;
//  }

  public void readTimestampMemBlock(RamcastEndpoint endpoint) {
  }

  public ByteBuffer getSharedCircularBuffer() {
    return sharedCircularBuffer;
  }

  public ByteBuffer getSharedTimestampBuffer() {
    return sharedTimestampBuffer;
  }

  public void setCustomHandler(CustomHandler customHandler) {
    this.customHandler = customHandler;
  }

  public Map<Integer, RamcastEndpoint> getIncomingEndpointMap() {
    return incomingEndpointMap;
  }

  public Map<RamcastNode, RamcastEndpoint> getNodeEndpointMap() {
    return nodeEndpointMap;
  }

  public AtomicInteger getRound() {
    return round;
  }

  public Map<Integer, List<RamcastEndpoint>> getGroupEndpointsMap() {
    return groupEndpointsMap;
  }

  public RamcastAgent getAgent() {
    return agent;
  }

  private ByteBuffer allocateSharedBuffer(int connectionCount, int queueLength, int packageSize) {
    int capacity = connectionCount * queueLength * packageSize;
    return ByteBuffer.allocateDirect(capacity);
  }

  private ByteBuffer allocateShareTimestampdBuffer(int connectionCount, int queueLength, int size) {
    int capacity =
            connectionCount
                    * (queueLength * RamcastConfig.getInstance().getGroupCount() * size
                    + RamcastConfig.SIZE_FUO);
    return ByteBuffer.allocateDirect(capacity);
  }

  protected synchronized IbvQP createQP(RdmaCmId id, IbvPd pd, IbvCQ cq) throws IOException {
    IbvQPInitAttr attr = new IbvQPInitAttr();
    attr.cap().setMax_recv_wr(config.getQueueLength() * 10);
    attr.cap().setMax_send_wr(config.getQueueLength() * 10);
    attr.cap().setMax_recv_sge(1);
    attr.cap().setMax_send_sge(1);
    attr.cap().setMax_inline_data(config.getMaxinline());
    attr.setQp_type(IbvQP.IBV_QPT_RC);
    attr.setRecv_cq(cq);
    attr.setSend_cq(cq);
    return id.createQP(pd, attr);
  }

  @Override
  public RdmaCqProvider createCqProvider(RamcastEndpoint endpoint) throws IOException {
    if (RamcastConfig.LOG_ENABLED) logger.trace("setting up cq processor");
    IbvContext context = endpoint.getIdPriv().getVerbs();
    if (context != null) {
      if (RamcastConfig.LOG_ENABLED) logger.trace("setting up cq processor, context found");
      RamcastCqProcessor<RamcastEndpoint> cqProcessor;
      int key = context.getCmd_fd();
      if (!cqMap.containsKey(key)) {
        int cqSize = (config.getQueueLength() * 2) * 3 * RamcastGroup.getTotalNodeCount() * RamcastGroup.getTotalNodeCount(); // added *2 at the end. TODO: check this
        int wrSize = RamcastGroup.getTotalNodeCount() * RamcastGroup.getTotalNodeCount();
        cqProcessor =
                new RamcastCqProcessor<>(
                        context, cqSize, wrSize, 0, 1, this.timeout, config.isPolling());
        cqMap.put(context.getCmd_fd(), cqProcessor);
        cqProcessor.start();
      }
      cqProcessor = cqMap.get(context.getCmd_fd());

      return cqProcessor;
    } else {
      throw new IOException("setting up cq processor, no context found");
    }
  }

  @Override
  public IbvQP createQpProvider(RamcastEndpoint endpoint) throws IOException {
    IbvContext context = endpoint.getIdPriv().getVerbs();
    RamcastCqProcessor<RamcastEndpoint> cqProcessor = cqMap.get(context.getCmd_fd());
    IbvCQ cq = cqProcessor.getCQ();
    IbvQP qp = this.createQP(endpoint.getIdPriv(), endpoint.getPd(), cq);
    if (RamcastConfig.LOG_ENABLED) logger.trace("registering endpoint with cq");
    cqProcessor.registerQP(qp.getQp_num(), endpoint);
    return qp;
  }

  @Override
  public void allocateResources(RamcastEndpoint endpoint) throws Exception {
    endpoint.allocateResources();
  }

  public void disconnect(RamcastEndpoint endpoint) throws IOException, InterruptedException {
    messageProcessor.setRunning(false);
    RamcastNode node = endpoint.getNode();
    RamcastGroup group = RamcastGroup.getGroup(endpoint.getGroupId());
    nodeEndpointMap.remove(node);
    incomingEndpointMap.remove(endpoint.getEndpointId());
    incomingEndpoints.remove(endpoint);
    groupEndpointsMap.get(endpoint.getGroupId()).remove(endpoint);
    group.removeNode(node);
    endpoint.close();
    logger.info("endpoint {} disconnect", endpoint);

    if (node.isLeader() && agent.shouldBeLeader()) {
      this.beLeader();
    }
    messageProcessor.setRunning(true);
  }

  private void beLeader() throws IOException, InterruptedException {
    logger.info("THIS NODE {} WILL BE LEADER", agent.getNode());
    // new leader does two steps
    // sending 1A msg, get 1B with pending message
    requestWritePermission();

    // then sync the data with the other nodes in local group
    logger.info("Start syncing data");
    logger.trace("Pending messages: {}", messageProcessor.getProcessing());
    logger.trace("Timestamps: {}", timestampBlock);


    // assuming that the new leader is coming at a clean state => just need to continue
    // process whatever in the processing queue;
//    for (RamcastMessage message : messageProcessor.getProcessing()) {
//      this.processPendingMessage(message);
//    }
    // ok works.


//    syncState();
  }

//  private void processPendingMessage(RamcastMessage message) {
//    // if the message has enough ack, but doesn't have timestamp
//    // => read message timestamp from one process in the quorum and deliver it
//    if (message.isAcked(getRound().get())) {
//      if (!timestampBlock.isFulfilled(message)) {
//        // TODO: somehow obtain the ballot number
//        int ballotNumber = 100;
//        // and sequence number
//        int sequenceNumber = 100;
//        timestampBlock.writeLocalTs(message, message.getGroupIndex(agent.getGroupId()), ballotNumber, sequenceNumber, clock.getAndIncrement());
//        int finalTs = timestampBlock.getMaxTimestamp(message);
//        logger.debug("[{}] LEADER ELECT - msg is acked but doesn't have timestamp => propagate it with value [{}/{}/{}] finalTS {}", message.getId(), ballotNumber, sequenceNumber, clock.get(), finalTs);
//        message.setFinalTs(timestampBlock.getMaxTimestamp(message));
//        // remove it from processing queue
//        messageProcessor.getProcessing().remove(message);
//        // add to ordered queue
//        messageProcessor.getOrdered().add(message);
//        return;
//      } else {
//        logger.debug("[{}] LEADER ELECT - msg is acked and have ts => just wait to be delivered", message.getId());
//        return;
//      }
//    }
//
//
//    logger.debug("[{}] LEADER ELECT - msg is not acked, process as normal msg", message.getId());
//    messageProcessor.handleMessage(message);
//  }


//  private void syncState() throws IOException, InterruptedException {
//    Map<RamcastEndpoint, RamcastTsMemoryBlock> tsBlocks = new HashMap<>();
////    for (RamcastEndpoint endpoint : groupEndpointsMap.get(agent.getGroupId())) {
////      RamcastTsMemoryBlock tsBlock = endpoint.readTimestampMemorySpace();
////      logger.debug(
////              "read ts block cap {} from endpoint {} {}. the last position {}:[{}/{}/{}/{}]",
////              tsBlock.getCapacity(),
////              endpoint,
////              tsBlock,
////              tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1,
////              tsBlock.readSlotBallot(tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1),
////              tsBlock.readSlotSequence(tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1),
////              tsBlock.readSlotValue(tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1),
////              tsBlock.readSlotStatus(tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1));
////      tsBlocks.put(endpoint, tsBlock);
////    }
//
////    for (int i = 0; i < timestampBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP; i++) {
////      int thisBallot = timestampBlock.readSlotBallot(i);
////      int thisSequence = timestampBlock.readSlotSequence(i);
////      int thisValue = timestampBlock.readSlotValue(i);
////      byte thisStatus = timestampBlock.readSlotStatus(i);
////      for (Map.Entry<RamcastEndpoint, RamcastTsMemoryBlock> entry : tsBlocks.entrySet()) {
////        RamcastEndpoint endpoint = entry.getKey();
////        RamcastTsMemoryBlock tsMemoryBlock = entry.getValue();
////        int thatBallot = tsMemoryBlock.readSlotBallot(i);
////        int thatSequence = tsMemoryBlock.readSlotSequence(i);
////        int thatValue = tsMemoryBlock.readSlotValue(i);
////        byte thatStatus = tsMemoryBlock.readSlotStatus(i);
////
//////        if (thisBallot )
////      }
////    }
//  }

  public void close() throws IOException, InterruptedException {
    super.close();
    for (RamcastEndpoint endpoint : nodeEndpointMap.values()) {
      endpoint.close();
    }
    for (RamcastCqProcessor<RamcastEndpoint> cq : cqMap.values()) {
      cq.close();
    }
    logger.info("rpc group down");
  }

  public void connect() throws Exception {
    // trying to establish a bi-directional connection
    // A node only connect to node with bigger ids.
    for (RamcastNode node : RamcastGroup.getAllNodes()) {
      if (node.getOrderId() >= agent.getNode().getOrderId()) {
        Thread.sleep(50);
        if (RamcastConfig.LOG_ENABLED) logger.trace("connecting to: {}", node);
        RamcastEndpoint endpoint = createEndpoint();
        endpoint.connect(node.getInetAddress(), config.getTimeout());
        endpoint.setNode(node);
        // if the node is connecting to itself
        if (node.equals(agent.getNode())) {
          getNodeEndpointMap().put(node, endpoint);
          List<RamcastEndpoint> eps =
                  getGroupEndpointsMap().computeIfAbsent(node.getGroupId(), k -> Collections.synchronizedList(new ArrayList<>()));
          eps.add(endpoint);
        }
        initHandshaking(endpoint);
        while (!endpoint.isReady()) Thread.sleep(10);
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(">>> Client connected to: {}. CONNECTION READY", endpoint);
      }
    }
  }

  public void seRoundNumber(int ballotNumnber) {
    round.set(ballotNumnber);
  }

//  public AtomicInteger getSequenceNumber() {
//    return sequenceNumber;
//  }

  public RamcastTsMemoryBlock getTimestampBlock() {
    return timestampBlock;
  }

//  public AtomicInteger getCurrentSequenceNumber() {
//    return currentSequenceNumber;
//  }

  public MessageProcessor getMessageProcessor() {
    return messageProcessor;
  }

  public void revokeTimestampWritePermission() throws IOException {
    for (RamcastEndpoint ramcastEndpoint : getNodeEndpointMap().values()) {
      // only revoke permission of nodes in same group
      // and are not leader
      if (ramcastEndpoint.getNode().getGroupId() == agent.getNode().getGroupId()
              && !ramcastEndpoint.getNode().isLeader()) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
                  "Revoking write permission of {} on {}", ramcastEndpoint.getNode(), agent.getNode());
        ramcastEndpoint.registerTimestampReadPermission();
      }
    }
  }

  public List<RamcastEndpoint> getIncomingEndpoints() {
    return incomingEndpoints;
  }

  public AtomicVectorClock getClock() {
    return clock;
  }

  public void updateTsStatus(RamcastMessage message) {
    timestampBlock.setDelivered(message);
    timestampBlock.freeTimestamp(message);
  }

  public ByteBuffer getSharedHeronBuffer() {
    return sharedHeronBuffer;
  }

  public void setSharedHeronBuffer(ByteBuffer sharedHeronBuffer) {
    this.sharedHeronBuffer = sharedHeronBuffer;
  }

  public HeronMemoryBlock getSharedHeronBlock() {
    return sharedHeronBlock;
  }

  public void setSharedHeronBlock(HeronMemoryBlock sharedHeronBlock) {
    this.sharedHeronBlock = sharedHeronBlock;
  }

  public ByteBuffer getSharedHeronRecoveryBuffer() {
    return sharedHeronRecoveryBuffer;
  }

  public void setSharedHeronRecoveryBuffer(ByteBuffer sharedHeronRecoveryBuffer) {
    this.sharedHeronRecoveryBuffer = sharedHeronRecoveryBuffer;
  }

  public HeronRecoveryMemoryBlock getSharedHeronRecoveryBlock() {
    return sharedHeronRecoveryBlock;
  }

  public void setSharedHeronRecoveryBlock(HeronRecoveryMemoryBlock sharedHeronRecoveryBlock) {
    this.sharedHeronRecoveryBlock = sharedHeronRecoveryBlock;
  }

  public static class RamcastEndpointFactory implements RdmaEndpointFactory<RamcastEndpoint> {
    private RamcastEndpointGroup group;

    public RamcastEndpointFactory(RamcastEndpointGroup group) {
      this.group = group;
    }

    @Override
    public RamcastEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
      return new RamcastEndpoint(group, id, serverSide);
    }
  }

  private static class RamcastCqProcessor<C extends RamcastEndpoint> extends RdmaCqProcessor<C> {
    Map<String, String> contextMap = MDC.getCopyOfContextMap();

    public RamcastCqProcessor(
            IbvContext context,
            int cqSize,
            int wrSize,
            long affinity,
            int clusterId,
            int timeout,
            boolean polling)
            throws IOException {
      super(context, cqSize, wrSize, affinity, clusterId, timeout, polling);
    }

    @Override
    public void dispatchCqEvent(RamcastEndpoint endpoint, IbvWC wc) throws IOException {
      if (RamcastConfig.LOG_ENABLED) {
        if (contextMap != null) {
          MDC.setContextMap(contextMap); // set contextMap when thread start
        } else {
          MDC.clear();
        }
      }
      endpoint.dispatchCqEvent(wc);
    }
  }
}
