package ch.usi.dslab.lel.ramcast.heron;

import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.dynastar.tpcc.tables.Tables;
import ch.usi.dslab.lel.ramcast.MessageDeliveredCallback;
import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpointGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import com.ibm.disni.RdmaServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class HeronAgent {
    // ask for callback function to be called when the message is allowed to execute? yes, I did this way
    // message class to include the message + coordination buffer + read dict (read from remote processes)?


    private static final Logger logger = LoggerFactory.getLogger(HeronAgent.class);

    RamcastAgent agent;
    ByteBuffer sharedHeronBuffer;
    HeronMemoryBlock sharedHeronBlock;
    ByteBuffer sharedHeronRecoveryBuffer;
    HeronRecoveryMemoryBlock sharedHeronRecoveryBlock;
    Map<Integer, List<RamcastEndpoint>> groupEndpointsMap;
    MessageDeliveredCallback onExecute;
    boolean coordination = true;
//    boolean coordination = false;
//    HeronEndpoint heronEndpoint;

    public HeronRecovery heronRecovery;

    int majority;

//    LatencyPassiveMonitor delayLatencyMonitor;
//    ThroughputPassiveMonitor multiRequestsMonitor;
    boolean logDelayStarted = false;
    int delayGroup = 0;
    long delayStart;
    int requestNum = 0;
    int delayLatencyFreq = 1;

    boolean execute = true;
//    boolean execute = false;

    // server uses to execute a message multicasted by a client
    MessageDeliveredCallback onDeliverAmcast = new MessageDeliveredCallback() {
        @Override
        public void call(Object data) {
            if (!coordination) {
                if (execute)
                    onExecute.call(data);
                return;
            }

            // needed for frequency of delayed logging
            requestNum++;

            RamcastMessage message = (RamcastMessage) data;

            // message timestamp
            int finalTs = message.getFinalTs();

            if (RamcastConfig.LOG_ENABLED)
                logger.debug("Heron delivered message {} with timestamp {}, prepare for exchanging coordination",
                        message.getId(),
                        finalTs);

            // groups
            int[] groups = new int[message.getGroupCount()];
            for (int i = 0; i < message.getGroupCount(); i++)
                groups[i] = message.getGroup(i);

            if (groups.length == 1) {
                if (RamcastConfig.LOG_ENABLED)
                    logger.debug("only one group in destination, request is ready to execute");
                if (execute)
                    onExecute.call(data);
                return;
            }

//            multiRequestsMonitor.incrementCount();

            if (RamcastConfig.LOG_ENABLED)
                logger.debug("executing remote writes (phase 2) on groups {} ...",
                        Arrays.toString(groups));
            writeHeronCoord(finalTs, (short) 1, groups);
            if (RamcastConfig.LOG_ENABLED)
                logger.debug("writing remote write (phase 2) for message {}, finalTs {} done",
                        message.getId(),
                        finalTs);

            if (RamcastConfig.LOG_ENABLED)
                logger.debug("read local buffer for incoming coordination messages (phase 2) ...");
            int count = 0;
            while (true) {
                if (isCoordFulfilled(finalTs, (short) 1, groups))
                    break;
                count++;
                if (count % 300000000 == 0)
                    logger.error("isCoordFulfilled phase 2 cycles: {}, heron buffer {}", count, sharedHeronBlock.toFullString());
            }
            if (RamcastConfig.LOG_ENABLED)
                logger.debug("coordination fulfilled (phase 2) for message {}, finalTs {}",
                        message.getId(),
                        finalTs);

            // read remote values (phase 3)
            // todo: to be done

            if (RamcastConfig.LOG_ENABLED)
                logger.debug("request is ready to execute, execute application callback method");
            if (execute)
                onExecute.call(data);

            if (RamcastConfig.LOG_ENABLED)
                logger.debug("executing remote writes (phase 4) for message");
            writeHeronCoord(finalTs, (short) 2, groups);
            if (RamcastConfig.LOG_ENABLED)
                logger.trace("writing remote write (phase 4) for message {}, finalTs {} done",
                        message.getId(),
                        finalTs);

            if (RamcastConfig.LOG_ENABLED)
                logger.debug("read local buffer for incoming coordination messages (phase 4) ...");
            count = 0;
            while (true) {
                if (isCoordFulfilled(finalTs, (short) 2, groups))
                    break;
                count++;
                if (count % 100000000 == 0)
                    logger.error("isCoordFulfilled phase 4 cycles: {}, heron buffer {}", count, sharedHeronBlock.toFullString());
            }
            if (RamcastConfig.LOG_ENABLED)
                logger.debug("coordination fulfilled (phase 4) for message {}, finalTs {}",
                        message.getId(),
                        finalTs);
        }
    };

    void writeHeronCoord(int finalTs, short status, int[] groups) {
        try {
            for (int groupId : groups) {
                for (RamcastEndpoint endpoint : groupEndpointsMap.get(groupId)) {
                    // write coord on local buffer
                    if (endpoint.getNode().getNodeId() == agent.getNode().getNodeId() &&
                            endpoint.getNode().getGroupId() == agent.getNode().getGroupId()) {
                        sharedHeronBuffer.clear();
                        int pos = agent.getGroupId() * RamcastConfig.getInstance().getNodePerGroup() * 6
                                + agent.getNodeId() * 6;
                        logger.trace("writing coord {}/{} at pos {} to local addr {}", finalTs, status, pos, sharedHeronBlock.toFullString());
                        sharedHeronBuffer.putInt(pos, finalTs);
                        sharedHeronBuffer.putShort(pos+4, status);
                    }
                    else { // write coord on remote buffers
                        if (endpoint == null) continue; // endpoint could be null due to failed process
                        if (endpoint.getRemoteHeronBlock() == null)
                            throw new Exception("remote heron block is null");
                        logger.trace("writing coord to {} on addr {}, lkey {}",
                                endpoint.getNode(),
                                endpoint.getRemoteHeronBlock().getAddress(),
                                endpoint.getRemoteHeronBlock().getLkey());
                        endpoint.writeSignal(
                                endpoint.getRemoteHeronBlock().getAddress(),
                                endpoint.getRemoteHeronBlock().getLkey(),
                                Integer.TYPE,
                                finalTs,
                                Short.TYPE,
                                status);
                    }
                }
            }
        } catch (Exception ex) {
            logger.error("node {} that caused exception", agent.getNode());
            ex.printStackTrace();
        }
    }

    boolean isCoordFulfilled(int finalTs, int status, int[] groups) {
        for (int groupId : groups) {
            // set buffer to correct position for each group
            sharedHeronBuffer.position(groupId * groupByteLength);

//            logger.info("set heron buffer position, for msg {}, phase {}: group id {}, position {}",
//                    finalTs, status, groupId, sharedHeronBuffer.position());

            int count = 0;
            for (int i = 0; i < nodesPerGroup; i++) {
                int ts = sharedHeronBuffer.getInt();
                int st = sharedHeronBuffer.getShort();
                if (ts > finalTs || ts == finalTs && st >= status)
                    count++;
            }
            if (count < nodesPerGroup) {
//                if (requestNum % delayLatencyFreq == 0 &&
//                        count >= majority &&
//                        status == 1 &&
//                        groupId == 0 &&
//                        !logDelayStarted) {
//                    delayGroup = groupId;
//                    delayStart = System.nanoTime();
//                    logDelayStarted = true;
//                }

                return false;

            } else {
//                if (logDelayStarted && delayGroup == groupId) {
//                    logDelayStarted = false;
//                    long delayEnd = System.nanoTime();
//                    delayLatencyMonitor.logLatency(delayStart, delayEnd);
//                }
            }
        }
        return true;
    }


    public HeronAgent(int groupId, int nodeId, MessageDeliveredCallback onExecute) {
        // nodeId and groupId of RamcastAgent
        // todo: any different ones needed for Heron?
        this.onExecute = onExecute;
        try {
            agent = new RamcastAgent(groupId, nodeId, onDeliverAmcast);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        establishRamcastAgent();

        groupEndpointsMap = agent.getEndpointGroup().getGroupEndpointsMap();
        if (groupEndpointsMap == null)
            System.out.println("groupEndpointsMap is null");
//        else if (nodeId == 0 && groupId == 0) {
//            logger.debug("node {}, groupEndpointsMap:", agent.getNode());
//            for (RamcastEndpoint e: groupEndpointsMap.get(0))
//                logger.debug("endpoint {}", e.getNode());
////            for (RamcastEndpoint e: groupEndpointsMap.get(1))
////                logger.debug("endpoint {}", e.getNode());
//        }
        sharedHeronBuffer = agent.getEndpointGroup().getSharedHeronBuffer();
        sharedHeronBlock = agent.getEndpointGroup().getSharedHeronBlock();
    }

    int groupByteLength;
    int nodesPerGroup;

    public HeronAgent(int groupId, int nodeId, MessageDeliveredCallback onExecute,
                      Tables tables, Tables[][] remoteTables) {
        // nodeId and groupId of RamcastAgent
        // todo: any different ones needed for Heron?
        this.onExecute = onExecute;
        try {
            agent = new RamcastAgent(groupId, nodeId, onDeliverAmcast);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        establishRamcastAgent(tables, remoteTables);

        groupEndpointsMap = agent.getEndpointGroup().getGroupEndpointsMap();
        if (groupEndpointsMap == null)
            System.out.println("groupEndpointsMap is null");
//        else if (nodeId == 0 && groupId == 0) {
//            logger.debug("node {}, groupEndpointsMap:", agent.getNode());
//            for (RamcastEndpoint e: groupEndpointsMap.get(0))
//                logger.debug("endpoint {}", e.getNode());
////            for (RamcastEndpoint e: groupEndpointsMap.get(1))
////                logger.debug("endpoint {}", e.getNode());
//        }
        sharedHeronBuffer = agent.getEndpointGroup().getSharedHeronBuffer();
        sharedHeronBlock = agent.getEndpointGroup().getSharedHeronBlock();

        sharedHeronRecoveryBuffer = agent.getEndpointGroup().getSharedHeronRecoveryBuffer();
        sharedHeronRecoveryBlock = agent.getEndpointGroup().getSharedHeronRecoveryBlock();

        heronRecovery = new HeronRecovery(agent, sharedHeronRecoveryBlock, tables, remoteTables);
        heronRecovery.start();

//        if (agent.getGroupId() <= 1) {
//            delayLatencyMonitor = new LatencyPassiveMonitor(
//                    RamcastConfig.getInstance().getNodePerGroup() * agent.getGroupId() + agent.getNodeId(), "majority_delay");
//            multiRequestsMonitor = new ThroughputPassiveMonitor(
//                    RamcastConfig.getInstance().getNodePerGroup() * agent.getGroupId() + agent.getNodeId(), "multi_requests");
//        }

        majority = RamcastConfig.getInstance().getNodePerGroup()/2 + 1;
        nodesPerGroup = RamcastConfig.getInstance().getNodePerGroup();
        // 6: int + short for each heron buffer slice
        groupByteLength = nodesPerGroup * 6;

    }

    void establishRamcastAgent() {
        try {
            this.agent.bind();
            this.agent.establishHeronBlock(); // heron
            Thread.sleep(10000);
            this.agent.establishConnections();
            logger.error("NODE READY");
            Thread.sleep(10000);
        } catch (Exception e) {
            logger.error("node [{}/{}]", agent.getNode().getGroupId(), agent.getNode().getNodeId());
            System.out.println("node " + agent.getNode().getGroupId() + "/" + agent.getNode().getNodeId() + "bind error");
            e.printStackTrace();
        }
    }

    void establishRamcastAgent(Tables tables, Tables[][] remoteTables) {
        try {
            this.agent.bind();
            this.agent.establishHeronBlock(); // heron
            establishTpccTables(tables, remoteTables);
            Thread.sleep(5000);
            this.agent.establishConnections();
            logger.error("NODE READY");
            Thread.sleep(5000);
        } catch (Exception e) {
//            logger.error("node [{}/{}]", agent.getNode().getGroupId(), agent.getNode().getNodeId());
//            System.out.println("node " + agent.getNode().getGroupId() + "/" + agent.getNode().getNodeId() + "bind error");
            e.printStackTrace();
        }
    }

    public void establishTpccTables(Tables tables, Tables[][] remoteTables) {
        agent.getEndpointGroup().setTpccTables(tables);
        agent.getEndpointGroup().setRemoteTpccTables(remoteTables);
        tables.createRdmaTables(getServerEp());
    }

    public RdmaServerEndpoint<RamcastEndpoint> getServerEp() {
        return agent.getServerEp();
    }

    public RamcastEndpointGroup getEndpointGroup() {
        return agent.getEndpointGroup();
    }

    // client uses to execute a command
    public void execute(int msgId, ByteBuffer buffer, List <RamcastGroup> destinations) throws IOException {
        RamcastMessage message = agent.createMessage(msgId, buffer, destinations);
        this.agent.setSlot(message, destinations);
        logger.debug("client {}/{} to multicast message {} to {}", agent.getNode().getGroupId(), agent.getNode().getNodeId(), message.getId(), destinations);
        agent.multicastSync(message, destinations);
        logger.trace("client {}/{} done multicasting message {} to {}", agent.getNode().getGroupId(), agent.getNode().getNodeId(), message.getId(), destinations);
    }
}
