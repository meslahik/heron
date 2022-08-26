package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.lel.dynastar.tpcc.tables.Tables;
import ch.usi.dslab.lel.ramcast.MessageDeliveredCallback;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.heron.HeronAgent;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class TpccAgent {
    public static final Logger logger = LoggerFactory.getLogger(TpccAgent.class);

    // local state used in onExecute method
    TpccServer server;
    // local state used in run method
    TpccClient client;

    // local state used in onExecute method
    boolean isClient;
    int pid;
    HeronAgent heronAgent;

    // run by servers after delivery, a server is also a client to measure latency
    MessageDeliveredCallback onExecute = (data) -> {
        int msgId = ((RamcastMessage) data).getFinalTs();
        int finalTs = ((RamcastMessage) data).getFinalTs();
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("message with finalTs {} delivered, going to execute it", finalTs);
        ByteBuffer buffer = ((RamcastMessage) data).getMessage();
        server.executeCommand(buffer, msgId, finalTs);
    };

//    void run(int wNewOrder, int wPayment, int wDelivery, int wOrderStatus, int wStockLevel, String terminalDistribution) {
    void run(int wNewOrder, int wPayment, int wDelivery, int wOrderStatus, int wStockLevel, int clientWarehouseId, int clientDistrictId) {
        client.run(wNewOrder, wPayment, wDelivery, wOrderStatus, wStockLevel, clientWarehouseId, clientDistrictId);
    }

    Tables tables;
    Tables[][] remoteTables;

//    public TpccAgent(int nodeId, int groupId, int pid, String configFile, int payloadSize, TpccProcedure appProcedure, boolean isClient,
//                     String dataFile, int terminalNum, int transactionNum, int warehouseCount) {
    public TpccAgent(int nodeId, int groupId, int pid, String configFile, int payloadSize, boolean isClient,
        String dataFile, int terminalNum, int transactionNum, int warehouseCount, int clientIssueReq, int numPrcoesses) {
        this.pid = pid;

        RamcastConfig.SIZE_MESSAGE = payloadSize;
        RamcastConfig config = RamcastConfig.getInstance();
        config.loadConfig(configFile);
        config.setPayloadSize(payloadSize);

        tables = new Tables();
        // create remote tables
        // each 'Tables' only declares address, lkey, capacity of remote tables
        // ignore table with index 0 to make it easier to refer to tables with warehouseId
        // refer to nodes per group from 0 to RamcastConfig.getInstance().getNodePerGroup()-1
        remoteTables = new Tables[RamcastConfig.getInstance().getGroupCount() + 1][RamcastConfig.getInstance().getNodePerGroup()];
        for (int i = 1; i < RamcastConfig.getInstance().getGroupCount() + 1; i++)
            for (int j = 0; j < RamcastConfig.getInstance().getNodePerGroup(); j++)
                remoteTables[i][j] = new Tables();
        heronAgent = new HeronAgent(groupId, nodeId, onExecute, tables, remoteTables);
//        server = new TpccServer(nodeId, groupId, pid, agent, appProcedure);
        server = new TpccServer(nodeId, groupId, pid, dataFile, heronAgent, tables, remoteTables, isClient);
//        appProcedure.init("SERVER", server.secondaryIndex, logger, server.pId);

        this.isClient = isClient;
        if (isClient && clientIssueReq == 1)
            client = new TpccClient(
                    nodeId, groupId, pid, heronAgent,
                    dataFile, terminalNum, transactionNum, warehouseCount, numPrcoesses);
    }

    public static void main(String[] args) {
        if (RamcastConfig.LOG_ENABLED)
            logger.info("Entering main()");

        if (args.length != 11 && args.length != 22) {
            System.err.println("usage: " +
                    "nodeId | " +
                    "groupId | " +
                    "pid | " +
                    "configFile | " +
                    "payloadSize | " +
                    "dataFile | " +
                    "gathererHost | " +
                    "gathererPort | " +
                    "gathererLogDir | " +
                    "gathererDuration | " +
                    "gathererWarmup" +
                    "[terminalNumber | " +
                    "transactionNum | " +
                    "wNewOrder | " +
                    "wPayment | " +
                    "wDelivery | " +
                    "wOrderStatus | " +
                    "wStockLevel | " +
                    "clientWarehouseId | " +
                    "clientDistrictId | " +
                    "clientIssueReq | " +
                    "numProcesses]");
            System.exit(1);
        }
        int argIndex = 0;
        int nodeId = Integer.parseInt(args[argIndex++]);
        int groupId = Integer.parseInt(args[argIndex++]);
        int pId = Integer.parseInt(args[argIndex++]);
        String configFile = args[argIndex++];
        int payloadSize = Integer.parseInt(args[argIndex++]);
        String dataFile = args[argIndex++];
        String gathererHost = args[argIndex++];
        int gathererPort = Integer.parseInt(args[argIndex++]);
        String gathererDir = args[argIndex++];
        int gathererDuration = Integer.parseInt(args[argIndex++]);
        int gathererWarmup = Integer.parseInt(args[argIndex++]);
        boolean isClient = false;
        int terminalNumber = 0;
        int transactionNum = 0;
        int wNewOrder = 0;
        int wPayment = 0;
        int wDelivery = 0;
        int wOrderStatus = 0;
        int wStockLevel = 0;
//        String terminalDistribution = "";
        int clientWarehouseId = 0;
        int clientDistrictId = 0;
        int clientIssueReq = 0;
        int numProcesses = 0;
        if (args.length == 22) { // client
            isClient = true;
            terminalNumber = Integer.parseInt(args[argIndex++]);
            transactionNum = Integer.parseInt(args[argIndex++]);
            wNewOrder = Integer.parseInt(args[argIndex++]);
            wPayment = Integer.parseInt(args[argIndex++]);
            wOrderStatus = Integer.parseInt(args[argIndex++]);
            wDelivery = Integer.parseInt(args[argIndex++]);
            wStockLevel = Integer.parseInt(args[argIndex++]);
            // indicate number of terminals in the format of w=x:d=y, district part is just ignored for now
//            terminalDistribution = args[argIndex++];
            clientWarehouseId = Integer.parseInt(args[argIndex++]);
            clientDistrictId = Integer.parseInt(args[argIndex++]);
            clientIssueReq = Integer.parseInt(args[argIndex++]);
            numProcesses = Integer.parseInt(args[argIndex++]);
        }

        DataGatherer.configure(gathererDuration, gathererDir, gathererHost, gathererPort, gathererWarmup);

//        TpccProcedure appProcedure = new TpccProcedure();

        String[] dataFileParts = dataFile.split("/");
        int warehouseCount = Integer.parseInt(dataFileParts[dataFileParts.length - 1].split("_")[1]);

//        TpccAgent agent = new TpccAgent(nodeId, groupId, pId, configFile, payloadSize, appProcedure, isClient,
//                dataFile, terminalNumber, transactionNum, warehouseCount);
        TpccAgent tpccAgent =
                new TpccAgent(nodeId, groupId, pId, configFile, payloadSize, isClient, dataFile, terminalNumber, transactionNum, warehouseCount, clientIssueReq, numProcesses);
//        server.preLoadData(dataFile, gathererHost);

        tpccAgent.heronAgent.heronRecovery.allocateBatchBuffer();

        try {
            Thread.sleep(5000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // performance experiments
//        if (isClient && clientIssueReq == 1)
//            agent.run(wNewOrder, wPayment, wDelivery, wOrderStatus, wStockLevel, clientWarehouseId, clientDistrictId);

        // recovery experiments
        if (nodeId == 2) {
            for (int i = 0; i < 1000000000; i++) {
//                logger.info("StockTable first entry before recovery: {}", tpccAgent.tables.stockTable.get(1));

//                tpccAgent.heronAgent.heronRecovery.tables.itemsTable.eraseBuffer();

                // serialized data
//                tpccAgent.heronAgent.heronRecovery.initRecovery();
//                tpccAgent.heronAgent.heronRecovery.isRecovered();

                // non-serialized data
                tpccAgent.heronAgent.heronRecovery.initRecovery();
                tpccAgent.heronAgent.heronRecovery.isRecoveredNonSerialized();
                tpccAgent.heronAgent.heronRecovery.isRecovered();

                if (i % 500000 == 0)
                    logger.info("Recovery done: {}", i);

//                logger.info("StockTable first entry after recovery: {}", tpccAgent.tables.stockTable.get(1));

//                try {
//                    Thread.sleep(2000);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
        }
    }
}
