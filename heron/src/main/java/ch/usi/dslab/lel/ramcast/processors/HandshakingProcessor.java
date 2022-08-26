package ch.usi.dslab.lel.ramcast.processors;

import ch.usi.dslab.lel.dynastar.tpcc.rows.Row;
import ch.usi.dslab.lel.dynastar.tpcc.tables.CustomerTable;
import ch.usi.dslab.lel.dynastar.tpcc.tables.ItemsTable;
import ch.usi.dslab.lel.dynastar.tpcc.tables.StockTable;
import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpointGroup;
import ch.usi.dslab.lel.ramcast.heron.HeronConfig;
import ch.usi.dslab.lel.ramcast.heron.HeronMemoryBlock;
import ch.usi.dslab.lel.ramcast.heron.HeronRecoveryMemoryBlock;
import ch.usi.dslab.lel.ramcast.heron.HeronRecoveryMessage;
import ch.usi.dslab.lel.ramcast.models.RamcastMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
import com.ibm.disni.util.MemoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HandshakingProcessor {
  private static final Logger logger = LoggerFactory.getLogger(HandshakingProcessor.class);
  RamcastAgent agent;
  RamcastEndpointGroup group;

  public HandshakingProcessor(RamcastEndpointGroup group, RamcastAgent agent) {
    this.agent = agent;
    this.group = group;
  }

  public void initHandshaking(RamcastEndpoint endpoint) throws IOException {
    if (RamcastConfig.LOG_ENABLED) logger.trace("Init handshaking for endpoint {}", endpoint);
    RamcastMemoryBlock block = endpoint.getSharedServerHeadBlock();
    HeronMemoryBlock heronBlock = group.getSharedHeronBlock(); // mojtaba: heron
    HeronRecoveryMemoryBlock heronRecoveryBlock = group.getSharedHeronRecoveryBlock(); // mojtaba: heron recovery

    ByteBuffer buffer = ByteBuffer.allocateDirect(28 + 16 + 16 + 16 + 16*11); //mojtaba: increased size to include heron memory block , heron recovery, & tpcc tables
    buffer.putInt(RamcastConfig.MSG_HS_C1);
    buffer.putInt(this.agent.getNode().getGroupId());
    buffer.putInt(this.agent.getNode().getNodeId());
    buffer.putLong(block.getAddress());
    buffer.putInt(block.getLkey());
    buffer.putInt(block.getCapacity());

    // mojtaba: heron
    // use endpoint.getNode() for accessing destination's groupId and nodeId
    HeronMemoryBlock heronSegmentBlock =
            getHeronSegmentBlock(
                    heronBlock.getBuffer(),
                    heronBlock.getRkey(),
                    endpoint.getNode().getGroupId(),
                    endpoint.getNode().getNodeId());
    buffer.putLong(heronSegmentBlock.getAddress());
    buffer.putInt(heronSegmentBlock.getLkey());
    buffer.putInt(heronSegmentBlock.getCapacity());

    // mojtaba: heron recovery
    // use endpoint.getNode() for accessing destination's groupId and nodeId
//    HeronRecoveryMemoryBlock heronRecoverySegmentBlock =
//            getHeronRecoverySegmentBlock(
//                    heronRecoveryBlock.getBuffer(),
//                    heronRecoveryBlock.getRkey(),
//                    endpoint.getNode().getGroupId(),
//                    endpoint.getNode().getNodeId());
//    buffer.putLong(heronRecoverySegmentBlock.getAddress());
//    buffer.putInt(heronRecoverySegmentBlock.getLkey());
//    buffer.putInt(heronRecoverySegmentBlock.getCapacity());

    buffer.putLong(heronRecoveryBlock.getAddress());
    buffer.putInt(heronRecoveryBlock.getLkey());
    buffer.putInt(heronRecoveryBlock.getCapacity());

    // temp tables for state transfer of non-serialized data
    buffer.putLong(group.getTpccTables().itemsTable.getAddress());
    buffer.putInt(group.getTpccTables().itemsTable.getLkey());
    buffer.putInt(group.getTpccTables().itemsTable.getCapacity());

    // tpcc tables addresses
    buffer.putLong(group.getTpccTables().stockTable.getAddress());
    buffer.putInt(group.getTpccTables().stockTable.getLkey());
    buffer.putInt(group.getTpccTables().stockTable.getCapacity());
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("send Tables info to {}: {}",
              endpoint.getNode(),
              group.getTpccTables().stockTable);

    for (int i=1; i<11; i++) {
      buffer.putLong(group.getTpccTables().customerTables[i].getAddress());
      buffer.putInt(group.getTpccTables().customerTables[i].getLkey());
      buffer.putInt(group.getTpccTables().customerTables[i].getCapacity());
    }
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("send Tables info to {}: {}",
              endpoint.getNode(),
              group.getTpccTables().customerTables[10]);

    // if the same node, you can set the remote heron block to the same block just declared
    if (endpoint.getNode().equals(this.agent.getNode()))
      endpoint.setRemoteHeronBlock(
              heronBlock.getAddress(),
              heronBlock.getLkey(),
              heronBlock.getCapacity());

    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "[HS] Step 1 CLIENT Sending from group={}, node= {}, to group={}, node={} " +
                      "remoteHeadBlock addr={} lkey={} capacity={}," +
                      "HeronMemory block addr={} lkey={} capacity={}" +
                      "HeronRecoveryMemoryBlock addr={} lkey={} capacity={}",
              buffer.getInt(4),
              buffer.getInt(8),
              endpoint.getNode().getGroupId(),
              endpoint.getNode().getNodeId(),
              buffer.getLong(12),
              buffer.getInt(20),
              buffer.getInt(24),
              buffer.getLong(28), // mojtaba: heron
              buffer.getInt(36),
              buffer.getInt(40),
              buffer.getLong(44), // mojtaba: heron recovery
              buffer.getInt(52),
              buffer.getInt(56));
    endpoint.send(buffer);
  }


  public void handleHandshakeMessage(RamcastEndpoint endpoint, ByteBuffer buffer)
          throws IOException {
    int ticket = buffer.getInt(0);
    // todo: what is the difference between Server and client handshaking messages?
    // Aha, client and server mean the one that initiates the handshake and the one that continues the first communication
    if (ticket == RamcastConfig.MSG_HS_C1) { // msg step 1 sent from client
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[HS] Step 1 SERVER Receiving from [{}/{}] remoteHeadBlock addr={} lkey={} capacity={}" +
                        "HeronMemoryBlock addr={} lkey={} capacity={}" +
                        "HeronRecoveryMemoryBlock addr={} lkey={} capacity={}",
                buffer.getInt(4),
                buffer.getInt(8),
                buffer.getLong(12),
                buffer.getInt(20),
                buffer.getInt(24),
                buffer.getLong(28), // mojtaba: heron
                buffer.getInt(36),
                buffer.getInt(40),
                buffer.getLong(44), // mojtaba: heron recovery
                buffer.getInt(52),
                buffer.getInt(56));

      // store this endpoint information
      int groupId = buffer.getInt(4);
      int nodeId = buffer.getInt(8);
      RamcastNode node = RamcastNode.getNode(groupId, nodeId);
      if (node == null) {
        throw new IOException("Node not found: " + groupId + "/" + nodeId);
      }
      endpoint.setNode(node);

      // store address of the memory space to store remote head on client
      // mojtaba: head stored on remote node and refers to head of the buffer in this node
      endpoint.setClientMemoryBlockOfRemoteHead(
              buffer.getLong(12), buffer.getInt(20), buffer.getInt(24));
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[HS] Step 1 SERVER setClientMemoryBlockOfRemoteHead={}",
                endpoint.getRemoteServerHeadBlock());

      // mojtaba: heron
      endpoint.setRemoteHeronBlock(buffer.getLong(28), buffer.getInt(36), buffer.getInt(40));
      logger.trace("setRemoteHeronBlock {}", endpoint.getRemoteHeronBlock());

      // mojtaba: heron recovery
      endpoint.setRemoteHeronRecoveryBlock(buffer.getLong(44), buffer.getInt(52), buffer.getInt(56));
      logger.trace("setRemoteHeronRecoveryBlock {}", endpoint.getRemoteHeronRecoveryBlock());

      // mojtaba: temp memory for state transfer of non-serialized data
      group.getRemoteTables()[endpoint.getNode().getGroupId() + 1][endpoint.getNode().getNodeId()].itemsTable =
              new ItemsTable(Row.MODEL.ITEM,
                      buffer.getLong(60),
                      buffer.getInt(68),
                      buffer.getInt(72));

      // mojtaba: tpcc
      // +1 to handle indexes of warehouses easier
      group.getRemoteTables()[endpoint.getNode().getGroupId() + 1][endpoint.getNode().getNodeId()].stockTable =
              new StockTable(Row.MODEL.STOCK,
                      buffer.getLong(76),
                      buffer.getInt(84),
                      buffer.getInt(88),
                      null);
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("received Tables info from {}: {}",
                endpoint.getNode(),
                group.getRemoteTables()[endpoint.getNode().getGroupId() + 1][endpoint.getNode().getNodeId()].stockTable);

      for (int i = 1; i < 11; i++) {
        group.getRemoteTables()[endpoint.getNode().getGroupId() + 1][endpoint.getNode().getNodeId()].customerTables[i] =
                new CustomerTable(Row.MODEL.CUSTOMER,
                        buffer.getLong(92 + 16*(i-1)),
                        buffer.getInt(100 + 16*(i-1)),
                        buffer.getInt(104 + 16*(i-1)),
                        null);
      }
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("received Tables info from {}: {}",
                endpoint.getNode(),
                group.getRemoteTables()[endpoint.getNode().getGroupId() + 1][endpoint.getNode().getNodeId()].customerTables[10]);


      // send back to client data of the whole shared memory space
      RamcastMemoryBlock sharedCircularMemoryBlock = endpoint.getSharedCircularBlock();
      //      RamcastMemoryBlock sharedTimestampMemoryBlock = endpoint.getSharedTimestampBlock();
      // mojtaba: slicing sharedCircularBuffer and specify remote node's Memory block
      RamcastMemoryBlock memorySegmentBlock =
              getNodeMemorySegmentBlock(
                      group.getSharedCircularBuffer(),
                      endpoint.getSharedCircularBlock().getLkey(),
                      groupId,
                      nodeId);
      // set local block (sharedCellBlock) for this endpoint
      endpoint.setSharedCellBlock(memorySegmentBlock);
      memorySegmentBlock.setEndpoint(endpoint);
      //      group.getEndpointMemorySegmentMap().put(endpoint, memorySegmentBlock);

      // preparing response
      ByteBuffer response = ByteBuffer.allocateDirect(52);
      response.putInt(RamcastConfig.MSG_HS_S1); // response code

      response.putLong(sharedCircularMemoryBlock.getAddress());
      response.putInt(sharedCircularMemoryBlock.getLkey());
      response.putInt(sharedCircularMemoryBlock.getCapacity());

      response.putLong(memorySegmentBlock.getAddress());
      response.putInt(memorySegmentBlock.getLkey());
      response.putInt(memorySegmentBlock.getCapacity());

      //      response.putLong(sharedTimestampMemoryBlock.getAddress());
      //      response.putInt(sharedTimestampMemoryBlock.getLkey());
      //      response.putInt(sharedTimestampMemoryBlock.getCapacity());

      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[HS] Step 1 SERVER Sending []: sharedCircularMemoryBlock={} memorySegmentBlock={}",
                sharedCircularMemoryBlock,
                memorySegmentBlock);

      endpoint.send(response);
      endpoint.setHasExchangedServerData(true);

      // storing all incomming endpoints
      this.group.getIncomingEndpointMap().put(endpoint.getEndpointId(), endpoint);
      this.group.getIncomingEndpoints().add(endpoint);

      // if this connection is not from the node itself => add it as outgoing conn
      boolean exists = this.group.getNodeEndpointMap().get(node) != null;
      this.group.getNodeEndpointMap().putIfAbsent(node, endpoint);
      List<RamcastEndpoint> eps = this.group.getGroupEndpointsMap().get(groupId);
      if (eps == null) eps = new ArrayList<>();
      if (!exists) eps.add(endpoint);
      eps.sort(Comparator.comparingInt(ep -> ep.getNode().getNodeId()));
      this.group.getGroupEndpointsMap().put(groupId, eps);

      // if the node is connecting to itself -> don't need to do the reverse handshaking
      if (endpoint.getNode().equals(this.agent.getNode())) endpoint.setHasExchangedClientData(true);
      if (!endpoint.hasExchangedClientData()) this.initHandshaking(endpoint);
    } else if (ticket == RamcastConfig.MSG_HS_S1) { // msg step 1 sent from server
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[HS] Step 1 CLIENT Receiving from [{}] sharedBlock addr={} lkey={} capacity={} sharedSegment addr={} lkey={} capacity={}",
                endpoint.getNode(),
                buffer.getLong(4),
                buffer.getInt(12),
                buffer.getInt(16),
                buffer.getLong(20),
                buffer.getInt(28),
                buffer.getInt(32));

      endpoint.setRemoteCircularBlock(buffer.getLong(4), buffer.getInt(12), buffer.getInt(16));
      endpoint.setRemoteSharedMemoryCellBlock(
              buffer.getLong(20), buffer.getInt(28), buffer.getInt(32));
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[HS] Step 1 CLIENT. setRemoteSharedMemoryBlock={}", endpoint.getRemoteCircularBlock());
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[HS] Step 1 CLIENT. setRemoteSharedMemoryCellBlock={}",
                endpoint.getRemoteSharedMemoryCellBlock());

      endpoint.setHasExchangedClientData(true);
      if (endpoint.getNode().equals(this.agent.getNode())) endpoint.setHasExchangedServerData(true);

    } else {
      throw new IOException("Protocol msg code not found :" + ticket);
    }
  }
  //
  //  public void requestWritePermission(RamcastEndpoint endpoint, int ballotNumber)
  //      throws IOException {
  //    ByteBuffer buffer = ByteBuffer.allocateDirect(16);
  //    buffer.putInt(RamcastConfig.MSG_HS_C_GET_WRITE);
  //    buffer.putInt(this.agent.getNode().getGroupId());
  //    buffer.putInt(this.agent.getNode().getNodeId());
  //    buffer.putInt(ballotNumber);
  //    if (RamcastConfig.LOG_ENABLED) logger.debug(
  //        "[HS] Step Request Write permission CLIENT Sending: [{}/{}], ballot={}",
  //        buffer.getInt(4),
  //        buffer.getInt(8),
  //        buffer.getInt(12));
  //    endpoint.send(buffer);
  //  }

  public RamcastMemoryBlock getNodeMemorySegmentBlock(
          ByteBuffer sharedBuffer, int lkey, int groupId, int nodeId) {
    sharedBuffer.clear();
    int blockSize = RamcastConfig.getInstance().getQueueLength() * RamcastConfig.SIZE_MESSAGE;
    int pos =
            groupId * RamcastConfig.getInstance().getNodePerGroup() * blockSize + nodeId * blockSize;
    sharedBuffer.position(pos);
    sharedBuffer.limit(pos + blockSize);
    ByteBuffer buffer = sharedBuffer.slice();
    return new RamcastMemoryBlock(MemoryUtils.getAddress(buffer), lkey, buffer.capacity(), buffer);
  }

  // todo: hardcoded HeronConfig.COORD_SIZE
  public synchronized HeronMemoryBlock getHeronSegmentBlock(
          ByteBuffer sharedBuffer, int rkey, int groupId, int nodeId) {
    sharedBuffer.clear();
    int pos = groupId * RamcastConfig.getInstance().getNodePerGroup() * 6 + nodeId * 6;
    sharedBuffer.position(pos);
    sharedBuffer.limit(pos + 6);
    ByteBuffer buffer = sharedBuffer.slice();
    return new HeronMemoryBlock(MemoryUtils.getAddress(buffer), rkey, buffer.capacity(), buffer);
  }

  public synchronized HeronRecoveryMemoryBlock getHeronRecoverySegmentBlock(
          ByteBuffer sharedBuffer, int rkey, int groupId, int nodeId) {
    sharedBuffer.clear();
    int pos = groupId * RamcastConfig.getInstance().getNodePerGroup() * 6 + nodeId * 6;
    sharedBuffer.position(pos);
    sharedBuffer.limit(pos + 6);
    ByteBuffer buffer = sharedBuffer.slice();
    return new HeronRecoveryMemoryBlock(MemoryUtils.getAddress(buffer), rkey, buffer.capacity(), buffer);
  }
}
