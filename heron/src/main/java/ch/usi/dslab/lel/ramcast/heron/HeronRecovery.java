package ch.usi.dslab.lel.ramcast.heron;

import ch.usi.dslab.bezerra.sense.monitors.LatencyDistributionPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.dynastar.tpcc.rows.Row;
import ch.usi.dslab.lel.dynastar.tpcc.tables.ItemsTable;
import ch.usi.dslab.lel.dynastar.tpcc.tables.StockTable;
import ch.usi.dslab.lel.dynastar.tpcc.tables.Tables;
import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpointGroup;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.SVCPostSend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.nio.ByteBuffer;
import java.util.List;

public class HeronRecovery extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(HeronRecovery.class);

    RamcastAgent agent;
    int groupCount;
    int nodePerGroup;
//    Map<Integer, List<RamcastEndpoint>> groupEndpointsMap;
    RamcastEndpointGroup endpointGroup;
    List<RamcastEndpoint> groupEndpoints;
    HeronRecoveryMemoryBlock recoveryBlock;
    ByteBuffer recoveryBuffer;
    ByteBuffer recoveryBuffer2;

    long start;

    int recoveryNum = 0;

    public Tables tables;
    public Tables[][] remoteTables;

    LatencyPassiveMonitor latencyMonitor;
    LatencyDistributionPassiveMonitor latencyDistributionPassiveMonitor;
    ThroughputPassiveMonitor readMonitor;
    ThroughputPassiveMonitor writeMonitor;
    ThroughputPassiveMonitor throughputPassiveMonitor;

    public HeronRecovery(RamcastAgent agent, HeronRecoveryMemoryBlock heronRecoveryMemoryBlock,
                         Tables tables, Tables[][] remoteTables) {
        this.agent = agent;
        this.groupCount = RamcastConfig.getInstance().getGroupCount();
        this.nodePerGroup = RamcastConfig.getInstance().getNodePerGroup();
//        this.groupEndpointsMap = agent.getEndpointGroup().getGroupEndpointsMap();
        this.endpointGroup = agent.getEndpointGroup();
        this.groupEndpoints = endpointGroup.getGroupEndpointsMap().get(agent.getGroupId());
        this.recoveryBlock = heronRecoveryMemoryBlock;
        this.recoveryBuffer = heronRecoveryMemoryBlock.getBuffer().slice();
        this.recoveryBuffer.clear();
        this.recoveryBuffer2 = heronRecoveryMemoryBlock.getBuffer();
        this.recoveryBuffer2.clear();
        this.tables = tables;
        this.remoteTables = remoteTables;
        if (agent.getNodeId() == 0) {
            this.writeMonitor = new ThroughputPassiveMonitor(2, "write_count");
        }
        if (agent.getNodeId() == 2) {
            this.latencyMonitor = new LatencyPassiveMonitor(2, "state_transfer");
            this.latencyDistributionPassiveMonitor =
                    new LatencyDistributionPassiveMonitor(2, "state_transfer");
            this.readMonitor = new ThroughputPassiveMonitor(2, "read_count");
            this.throughputPassiveMonitor = new ThroughputPassiveMonitor(2, "throughput_tp");
        }
    }

    @Override
    public void run() {
        logger.debug("Recovery thread started in {}", agent.getNode());
        MDC.put("ROLE", agent.getGroupId() + "/" + agent.getNode().getNodeId());

        while (true) {
            recoveryBuffer.clear();

//            StringBuilder sb = new StringBuilder();
//            sb.append("|");

            for (int i=0; i < groupCount; i++) {
                for (int j=0; j < nodePerGroup; j++) {
                    short status = recoveryBuffer.getShort();
                    int replicaId = recoveryBuffer.getInt();

                    // todo: only replica 0 act to recover data
//                    if (status == 1 && replicaId != agent.getNodeId())
                    if (status == 1 && agent.getNodeId() == 0) {
//                        try {
//                            Thread.sleep(4000);
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }

                        // serialized data
//                        doStateTransfer1Row(replicaId);
//                        doStateTransferMultiple(replicaId);
//                        endRecovery(replicaId);

                        // non-serialized data
                        doStateTransferNonSerialized(replicaId);
                        endRecovery(replicaId);
                    }

//                    sb.append(status).append(",").append(replicaId).append("|");
                }
            }

//            if (RamcastConfig.LOG_ENABLED)
//                logger.trace("Heron recovery block: {}", sb);
        }
    }

    public void initRecovery() {
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("init recovery #{}", ++recoveryNum);
        start = System.nanoTime();
        writeRecoverySignal(agent.getNodeId(), agent.getGroupId(), 1);
//        logger.info("recovery request time: {} ns", System.nanoTime() - start);
//        int pos = (0*nodePerGroup + 2)*6;
//        recoveryBuffer2.position(pos);
//        recoveryBuffer2.putShort((short)0);
//        recoveryBuffer2.putInt(agent.getNodeId());
    }

    public boolean isRecovered() {
        int pos = (agent.getGroupId() * nodePerGroup + agent.getNodeId()) * 6;

        while (true) {
            if (recoveryBuffer2.position(pos).getShort() == 0) {
                // recovered
                if (RamcastConfig.LOG_ENABLED)
                    logger.debug("Recovery #{} done in {} ns. recovery block {}\n",
                            recoveryNum,
                            System.nanoTime() - start,
                            recoveryBlock.toFullString());
                throughputPassiveMonitor.incrementCount();
                latencyMonitor.logLatency(start, System.nanoTime());
                latencyDistributionPassiveMonitor.logLatencyForDistribution(start, System.nanoTime());
                break;
            }
            if (System.nanoTime() - start > 1000000000) {
                logger.error("NOT RECOVERED! recovery block {}\n", recoveryBlock.toFullString());
                start = System.nanoTime();
            }
        }

        return true;
    }

    public boolean isRecoveredNonSerialized() {
        ByteBuffer buffer = tables.itemsTable.tempBuffer;  // buffer to read from
        int readSize = RamcastConfig.getInstance().getPayloadSize();  // size of each remote write
        int readCount = 0;  // number of remote writes that has been read, specifies the read address

        if (RamcastConfig.LOG_ENABLED)
            logger.debug("temp buffer: {}", buffer);

        while (true) {
            // check flag at the end of each write whether incoming data is all written
            int flagPos = (readCount + 1) * readSize - 4;
            buffer.clear().position(flagPos);
            int flag = buffer.getInt();

            if (flag == 100) {
                if (RamcastConfig.LOG_ENABLED)
                    logger.debug("flag became 100 at position {}", flagPos);

                int readPos = readCount * readSize;
                buffer.position(readPos);

                int numRows = buffer.getInt();

                if (RamcastConfig.LOG_ENABLED)
                    logger.debug("numRows {}", numRows);

                if (numRows == -100) {  // state transfer finished, there is no new row
                    // recovered
                    readMonitor.incrementCount();
                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug("Recovery #{} done in {} ns. recovery block {}\n",
                                recoveryNum,
                                System.nanoTime() - start,
                                recoveryBlock.toFullString());
                    break;
                }

                for (int i = 0; i < numRows; i++) {
                    int pos = readPos + 4 + i * Row.MODEL.ITEM.getRowSize();
                    int nextPos = readPos + 4 + (i + 1) * Row.MODEL.ITEM.getRowSize();

                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug("item id to be read: {}, at pos {}", buffer.clear().position(pos).getInt(), pos);

                    ByteBuffer rowBuff = buffer.clear().position(pos).limit(nextPos).slice();
                    tables.itemsTable.setByte(rowBuff);
                }
                readCount++;
                readMonitor.incrementCount();
            }
        }

        return true;
    }

    public void endRecovery(int nodeId) {
        writeRecoverySignal(nodeId, agent.getGroupId(), 0);
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("recovery #{} done in sender side", ++recoveryNum);
    }

    public void writeRecoverySignal (int nodeId, int groupId, int signalValue) {
        try {
            // local buffer: write local first to ensure local buffer write is first one to be written
            int pos = (groupId*nodePerGroup + nodeId)*6;
            recoveryBuffer2.position(pos);
            recoveryBuffer2.putShort((short)signalValue);
            recoveryBuffer2.putInt(agent.getNodeId());
//            logger.debug("Recovery block after update: {}", recoveryBlock.toFullString());

            for (RamcastEndpoint endpoint : groupEndpoints) {
                if (endpoint.getNode().getNodeId() == agent.getNode().getNodeId()) {
                    // skip, local buffer is written above
                } else { // remote buffers
                    long address = endpoint.getRemoteHeronRecoveryBlock().getAddress() +
                            (groupId*nodePerGroup + nodeId)*6;

                    if (endpoint == null) continue; // endpoint could be null due to failed process
                    if (endpoint.getRemoteHeronRecoveryBlock() == null)
                        throw new Exception("remote heron recovery block is null");

                    if (RamcastConfig.LOG_ENABLED)
                        logger.trace("writing recovery signal from {} to {} on addr {}, lkey {}",
                                agent.getNode(),
                                endpoint.getNode(),
                                address,
                                endpoint.getRemoteHeronRecoveryBlock().getLkey());
                    endpoint.writeSignal(
                            address,
                            endpoint.getRemoteHeronRecoveryBlock().getLkey(),
                            Short.TYPE,
                            (short) signalValue,
                            Integer.TYPE,
                            agent.getNodeId());
                }
            }
        } catch (Exception ex) {
            logger.error("recovery - node {} caused exception", agent.getNode());
            ex.printStackTrace();
        }
    }

    public void doStateTransfer1Row(int replicaId) {
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("state transfer requested for replica {}", replicaId);

//        try {
//            Thread.sleep(6000);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        try {
            StockTable s = remoteTables[agent.getGroupId()+1][replicaId].stockTable;
            long rowAddr = s.getAddress() + 1 * Row.MODEL.STOCK.getRowSize();
            if (RamcastConfig.LOG_ENABLED)
                logger.debug("going to write stock data remotely {}", tables.stockTable.get(2));
            ByteBuffer buffer = tables.stockTable.getByteBufferPair(2);
//            endpointGroup.writeRecoveryData(replicaId, rowAddr, s.getLkey(), buffer);
            if (RamcastConfig.LOG_ENABLED)
                logger.trace("issued remote write on Stock table in replica {}, address {}", replicaId, rowAddr);

        } catch (Exception e) {
            logger.debug("node {} problem in state transfer", agent.getNode());
            e.printStackTrace();
        }
    }

    ByteBuffer[] batchBuffer = new ByteBuffer[4];
//    byte[] zeroArray;
    int lkey;

    public void allocateBatchBuffer() {
        int payloadSize = RamcastConfig.getInstance().getPayloadSize();
        ByteBuffer buffer = ByteBuffer.allocateDirect(payloadSize*4);
        try {
            IbvMr mr = agent.getServerEp().registerMemory(buffer).execute().free().getMr();
            lkey = mr.getLkey();
        } catch (Exception e) {
            e.printStackTrace();
        }
        batchBuffer[0] = buffer.clear().position(0).limit(payloadSize).slice();
        batchBuffer[1] = buffer.clear().position(payloadSize).limit(payloadSize*2).slice();
        batchBuffer[2] = buffer.clear().position(payloadSize*2).limit(payloadSize*3).slice();
        batchBuffer[3] = buffer.clear().position(payloadSize*3).limit(payloadSize*4).slice();
//        zeroArray = new byte[payloadSize];
    }

//    public void eraseBatchBuffers() {
//        batchBuffer[0].clear().put(zeroArray);
//        batchBuffer[1].clear().put(zeroArray);
//    }

    public void doStateTransferMultiple(int replicaId) {
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("state transfer requested for replica {}", replicaId);

        int payloadSize = RamcastConfig.getInstance().getPayloadSize();
        int numRowsInEachWrite = payloadSize/Row.MODEL.STOCK.getRowSize();
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("number of rows in each write: {}, payloadSize {}, rowSize {}", numRowsInEachWrite, payloadSize, Row.MODEL.STOCK.getRowSize());
//        ByteBuffer buffer = ByteBuffer.allocateDirect(payloadSize);
//        buffer.clear();

        StockTable s = remoteTables[agent.getGroupId()+1][replicaId].stockTable;
        long rowAddr = 0;

        int writeCount = 0;  // // number of remote writes

        int numRows = 0;  // for each remote write specifies number of rows in that write

        boolean startNewWrite = true;
        SVCPostSend postSend = null;
        int index = 0;

        int capacity = 0;

        int numTotalRowsToWrite = 100000;
        try {
            for (int i = 1; i <= numTotalRowsToWrite; i++) {
//                try {
//                    Thread.sleep(5000);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }

                if (startNewWrite) {
                    // find out in which buffer to write
                    postSend = endpointGroup.getSVCPostSend(replicaId);
                    index = (int) postSend.getWrMod(0).getWr_id();
                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug("SVCpostsend index: {}", index);

                    // calculate number of rows to write in this batch
                    int remainingRowsToWrite = numTotalRowsToWrite - i + 1;
                    numRows = Math.min(remainingRowsToWrite, numRowsInEachWrite);
//                    batchBuffer[index].clear().putInt(numRows);

                    rowAddr = s.getAddress() + i * Row.MODEL.STOCK.getRowSize();

                    startNewWrite = false;
                }

                ByteBuffer rowPair = tables.stockTable.getByteBufferPair(i);
                rowPair.clear();
                capacity += rowPair.capacity();
                batchBuffer[index].put(rowPair);
                numRows--;

                if (numRows == 0) {
                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug("going to write buffer size {}, stock data remotely {}",
                                batchBuffer[index].capacity(),
                                tables.stockTable.get(i));
                    endpointGroup.writeRecoveryData(
                            postSend, replicaId, rowAddr, lkey, s.getLkey(), batchBuffer[index], capacity);
                    if (RamcastConfig.LOG_ENABLED)
                        logger.trace("issued remote write on Stock table in replica {}, address {}", replicaId, rowAddr);
                    writeCount++;
                    startNewWrite = true;
                    batchBuffer[index].clear();
                    capacity = 0;
                }
            }

            if (flag) {
                logger.info("# of remote writes in one run: {}", writeCount);
                flag = false;
            }
        } catch (Exception e) {
            logger.debug("node {} problem in state transfer", agent.getNode());
            e.printStackTrace();
        }
    }

    boolean flag = true;
    public void doStateTransferNonSerialized(int replicaId) {
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("state transfer requested for replica {}", replicaId);

        int payloadSize = RamcastConfig.getInstance().getPayloadSize();
        // for each write 4 bytes reserved for num of items, 4 bytes reserved for flag at the end of buffer
        int numRowsInEachWrite = (payloadSize-8)/Row.MODEL.ITEM.getRowSize();
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("number of rows in each write: {}, payloadSize {}, rowSize {}", numRowsInEachWrite, payloadSize, Row.MODEL.ITEM.getRowSize());
//        ByteBuffer buffer = ByteBuffer.allocateDirect(payloadSize);
//        buffer.clear();

        ItemsTable itemsTable = remoteTables[agent.getGroupId()+1][replicaId].itemsTable;

        long rowAddr = 0;  // address of remote writes
        int writeCount = 0;  // number of remote writes, to specify rowAddr

        int numRows = 0;  // for each remote write specifies number of rows in that write

        boolean startNewWrite = true;
        SVCPostSend postSend = null;
        int index = 0;

        int numTotalRowsToWrite = 7160;  // experiment's number of total rows to be written
        try {
            for (int i = 1; i <= numTotalRowsToWrite; i++) {
//                try {
//                    Thread.sleep(5000);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }

                // at the beginning of each remote write ...
                if (startNewWrite) {
                    // find out in which buffer to write
                    postSend = endpointGroup.getSVCPostSend(replicaId);
                    index = (int) postSend.getWrMod(0).getWr_id();
                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug("SVCpostsend index: {}", index);

                    // calculate number of rows to write in this batch
                    int remainingRowsToWrite = numTotalRowsToWrite - i + 1;
                    numRows = Math.min(remainingRowsToWrite, numRowsInEachWrite);
                    batchBuffer[index].putInt(numRows);

                    rowAddr = itemsTable.getAddress() + writeCount * payloadSize;

                    startNewWrite = false;
                }

                ByteBuffer row = tables.itemsTable.getByte(i);
                row.clear();
                batchBuffer[index].put(row);
                numRows--;

                if (numRows == 0) {
                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug("going to write buffer size {}, loop {}", payloadSize, i);

                    // add 1 in the last byte so the recipient knows all data is written
                    batchBuffer[index].position(batchBuffer[index].capacity()-4).putInt(100);
//                    endpointGroup.writeRecoveryData(replicaId, rowAddr, itemsTable.getLkey(), buffer);
                    endpointGroup.writeRecoveryData(
                            postSend, replicaId, rowAddr, lkey, itemsTable.getLkey(), batchBuffer[index], payloadSize);
                    writeMonitor.incrementCount();
                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug("issued remote write on Item table in replica {}, address {}", replicaId, rowAddr);
                    writeCount++;
                    startNewWrite = true;
                    batchBuffer[index].clear();
                }
            }

            // find out in which buffer to write
            postSend = endpointGroup.getSVCPostSend(replicaId);
            index = (int) postSend.getWrMod(0).getWr_id();

            // last write to signal remote node that state transfer is done
            batchBuffer[index].position(0).putInt(-100);
            batchBuffer[index].position(batchBuffer[index].capacity()-4).putInt(100);
            rowAddr = itemsTable.getAddress() + writeCount * payloadSize;
//            endpointGroup.writeRecoveryData(replicaId, rowAddr, itemsTable.getLkey(), buffer);
            endpointGroup.writeRecoveryData(
                    postSend, replicaId, rowAddr, lkey, itemsTable.getLkey(), batchBuffer[index], payloadSize);
            writeMonitor.incrementCount();
            writeCount++;
            if (flag) {
                logger.info("# of remote writes in one run: {}", writeCount);
                flag = false;
            }
            if (RamcastConfig.LOG_ENABLED)
                logger.debug("issued last remote write on Item table in replica {}, address {}", replicaId, rowAddr);

        } catch (Exception e) {
            logger.debug("node {} problem in state transfer", agent.getNode());
            e.printStackTrace();
        }
    }
}
