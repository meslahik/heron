package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.dynastar.tpcc.benchmark.BenchContext;
import ch.usi.dslab.lel.dynastar.tpcc.objects.ObjId;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccConfig;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccRandom;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.heron.HeronAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import redis.clients.jedis.BinaryJedis;
//import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.*;
import java.util.function.Consumer;

import static ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccConfig.*;

public class TpccClient {
    private static final Logger logger = LoggerFactory.getLogger(TpccClient.class);

    int nodeId;
    int groupId;
    private int pid;
    HeronAgent agent;

//    private Map<String, Set<ObjId>> secondaryIndex = new HashMap<>();
//    private Client clientProxy;
    private Random gen;
    private TpccTerminal[] terminals;
    private String[] terminalNames;
    private boolean terminalsBlockingExit = false;
    private long terminalsStarted = 0, sessionCount = 0, transactionCount = 0;
    private long sessionEndTargetTime = -1, fastNewOrderCounter, recentTpmC = 0, recentTpmTotal = 0;
    private long newOrderCounter = 0, sessionStartTimestamp, sessionEndTimestamp, sessionNextTimestamp = 0, sessionNextKounter = 0;

    private ThroughputPassiveMonitor transactionCountMonitor;
    private ThroughputPassiveMonitor newOrderCountMonitor;
    private ThroughputPassiveMonitor globalTransCountMonitor;
    private ThroughputPassiveMonitor localTransCountMonitor;
    private boolean signalTerminalsRequestEndSent = false, databaseDriverLoaded = false;
    private Object counterLock = new Object();

    private int transactionNum;
    private int terminalNum;

    private TpccRandom rnd;
    private int warehouseCount;

    int numProcesses;

    public TpccClient(int nodeId, int groupId, int pid, HeronAgent agent,
                      String dataFile, int terminalNum, int transactionNum, int warehouseCount, int numProcesses) {
        this.nodeId = nodeId;
        this.groupId = groupId;
        this.pid = pid;
        this.agent = agent;

        this.numProcesses = numProcesses;

        // todo: does procedure needed in client?
//        TpccProcedure appProcedure = new TpccProcedure();
//        this.clientId = clientId;
        // todo: to be replaced by Heron Client
//        clientProxy = new Client(clientId, systemConfigFile, partitioningFile, appProcedure);
//        this.preLoadData(dataFile);
        // todo: how cache and secondary index relates to dynastar clients??
//        appProcedure.init("CLIENT", secondaryIndex, logger, 0);
        gen = new Random(System.nanoTime());
        this.terminalNum = terminalNum;
        this.transactionNum = transactionNum;
        this.warehouseCount = warehouseCount;
        String[] fileNameParts = dataFile.replace(".data", "").replace(".oracle", "").split("_");
        TpccConfig.configCustPerDist = Integer.parseInt(fileNameParts[5]);
        TpccConfig.configItemCount = Integer.parseInt(fileNameParts[7]);
    }

//    void run(int wNewOrder, int wPayment, int wDelivery, int wOrderStatus, int wStockLevel, String terminalDistribution) {
    void run(int wNewOrder, int wPayment, int wDelivery, int wOrderStatus, int wStockLevel, int clientWarehouseId, int clientDistrictId) {
        // interactive terminal
        if (terminalNum == 0) {
            terminals = new TpccTerminal[1];
            terminals[0] = new TpccTerminal(agent, "Interactive", 100, warehouseCount,
                    1, 1, 100,
                    45, 43, 4, 4, 4,
                    -1, this,
                    new BenchContext.CallbackHandler(RamcastConfig.getInstance().getNodePerGroup() * groupId + nodeId),
                    pid,
                    numProcesses);
            terminals[0].setLogger(logger);
            this.runInteractive();
        } else {
            // for testing
//            this.runAuto(wNewOrder, wPayment, wDelivery, wOrderStatus, wStockLevel, "w=1:d=1_w=2:d=1_w=1:d=2_w=2:d=2_w=1:d=3_w=2:d=3_w=1:d=4_w=2:d=4_w=1:d=5_w=2:d=5");
//            this.runAuto(agent, wNewOrder, wPayment, wDelivery, wOrderStatus, wStockLevel, terminalDistribution);
            this.runAuto(agent, wNewOrder, wPayment, wDelivery, wOrderStatus, wStockLevel, clientWarehouseId, clientDistrictId);
        }
    }

//    public static void main(String[] args) {
//        if (args.length != 12 && args.length != 18) {
//            System.out.println("USAGE: AppClient | " +
//                    "groupId | " +
//                    "nodeId | " +
//                    "configFile | " +
//                    "payloadSize | " +
//                    "dataFile | " +
//                    "terminalNumber(0 for interactive) | " +
//                    "transaction count | " +
//                    "wNewOrder | " +
//                    "wPayment | " +
//                    "wOrderStatus | " +
//                    "wDelivery | " +
//                    "wStockLevel");
//            System.exit(1);
//        }
//        int index = 0;
////        int clientId = Integer.parseInt(args[index++]);
//        int groupId = Integer.parseInt(args[index++]);
//        int nodeId = Integer.parseInt(args[index++]);
//        String configFile = args[index++];
//        int payloadSize = Integer.parseInt(args[index++]);
//        String dataFile = args[index++];
//        int terminalNumber = Integer.parseInt(args[index++]);
//        int transactionNum = Integer.parseInt(args[index++]);
//        int wNewOrder = Integer.parseInt(args[index++]);
//        int wPayment = Integer.parseInt(args[index++]);
//        int wDelivery = Integer.parseInt(args[index++]);
//        int wOrderStatus = Integer.parseInt(args[index++]);
//        int wStockLevel = Integer.parseInt(args[index++]);
//        String terminalDistribution = null;
//        if (args.length == 18) {
//            String gathererNode = args[index++];
//            int gathererPort = Integer.parseInt(args[index++]);
//            String fileDirectory = args[index++];
//            int experimentDuration = Integer.parseInt(args[index++]);
//            int warmUpTime = Integer.parseInt(args[index++]);
//            DataGatherer.configure(experimentDuration, fileDirectory, gathererNode, gathererPort, warmUpTime);
//            // todo: what is terminalDistribution?
//            // indicate number of terminals in the format of w=x:d=y, one terminal per district
//            terminalDistribution = args[index++];
//        }
//        System.out.println(groupId + "/" + nodeId + " - " + terminalDistribution);
//        // todo: what is dataFileParts?
//        String[] dataFileParts = dataFile.split("/");
//        int warehouseCount = Integer.parseInt(dataFileParts[dataFileParts.length - 1].split("_")[1]);
//        TpccClient appcli = new TpccClient(
//                nodeId,
//                groupId,
//                dataFile,
//                terminalNumber,
//                transactionNum,
//                warehouseCount);
//        appcli.run(
//                wNewOrder,
//                wPayment,
//                wDelivery,
//                wOrderStatus,
//                wStockLevel,
//                terminalDistribution);
//    }

//    private void preLoadData(String file) {
//        String hostName = null, redisHost;
//        try {
//            hostName = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        if (hostName.indexOf("node") == 0) {
//            redisHost = "192.168.3.45";
//        } else if (hostName.indexOf("Long") == 0) {
//            redisHost = "127.0.0.1";
//        } else {
//            redisHost = "172.31.42.68";
//        }
//        boolean cacheLoaded = false;
//        String[] fileNameParts = file.split("/");
//        String fileName = fileNameParts[fileNameParts.length - 1];
//        Integer partitionCount = Partition.getPartitionsCount();
//        System.out.println("[CLIENT-" + this.clientProxy.getId() + "] Creating connection to redis host " + redisHost);
//        BinaryJedis jedis = new BinaryJedis(redisHost);
//        Codec codec = new CodecUncompressedKryo();
//        long start = System.currentTimeMillis();
//        boolean redisAvailabled = true;
//        byte[] keyObjectGraph = (fileName + "_p_" + partitionCount + "_CLIENT_" + this.clientProxy.getId() + "_objectGraph").getBytes();
//        byte[] keySecondaryIndex = (fileName + "_p_" + partitionCount + "_CLIENT_" + this.clientProxy.getId() + "_secondaryIndex").getBytes();
//        byte[] keyDataLoaded = (fileName + "_p_" + partitionCount + "_CLIENT_" + this.clientProxy.getId() + "_data_loaded").getBytes();
//
//        try {
//            byte[] cached = jedis.get(keyDataLoaded);
//            if (cached != null && new String(cached).equals("OK")) {
//                System.out.println("[CLIENT-" + this.clientProxy.getId() + "] loading sample data from cache...");
//                this.clientProxy.setCache((PRObjectGraph) codec.createObjectFromBytes(jedis.get(keyObjectGraph)));
//                this.clientProxy.setSecondaryIndex((HashMap<String, Set<ObjId>>) codec.createObjectFromBytes(jedis.get(keySecondaryIndex)));
//                cacheLoaded = true;
//            }
//        } catch (JedisConnectionException e) {
//            System.out.println("[CLIENT" + this.clientProxy.getId() + "] Redis Cache is not available. Loading from file");
//            redisAvailabled = false;
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(-1);
//        }
//        if (!cacheLoaded) {
//            System.out.println("[CLIENT-" + this.clientProxy.getId() + "] loading sample data from file...");
//            TpccUtil.loadDataToCache(file, this.clientProxy.getCache(), this.clientProxy.getSecondaryIndex(), (objId, obj) -> {
//                int dest = TpccUtil.mapIdToPartition(objId);
//                PRObjectNode node = new PRObjectNode(objId, dest);
//                this.clientProxy.getCache().addNode(node);
//            });
//            if (redisAvailabled) {
//                jedis.set(keyObjectGraph, codec.getBytes(this.clientProxy.getCache()));
//                jedis.set(keySecondaryIndex, codec.getBytes(this.clientProxy.getSecondaryIndex()));
//                jedis.set(keyDataLoaded, new String("OK").getBytes());
//            }
//        }
//        System.out.println("[CLIENT-" + this.clientProxy.getId() + "] Data loaded, takes " + (System.currentTimeMillis() - start));
//    }


    private void execute(int transaction) throws Exception{
        transactionCount++;
        long now = System.currentTimeMillis();
        Consumer then = o -> System.out.println("Time: " + (System.currentTimeMillis() - now) + " - " + o);
        switch (transaction) {
            case NEW_ORDER: {
//                terminals[0].doNewOrder(then);
                terminals[0].doNewOrder();
                break;
            }
            case PAYMENT: {
                terminals[0].doPayment();
                break;
            }
            case ORDER_STATUS: {
                terminals[0].doOrderStatus();
                break;
            }
            case DELIVERY: {
                terminals[0].doDelivery();
                break;
            }
            case STOCK_LEVEL: {
                terminals[0].doStockLevel();
                break;
            }

        }
    }


//    public void runAuto(int iNewOrderWeight, int iPaymentWeight, int iDeliveryWeight, int iOrderStatusWeight, int iStockLevelWeight) {
//        terminals = new TpccTerminal[this.terminalNum];
//        terminalNames = new String[this.terminalNum];
//        terminalsStarted = this.terminalNum;
//        rnd = new TpccRandom(1);
//        int[][] usedTerminals = new int[this.warehouseCount][TpccConfig.configDistPerWhse];
//        for (int i = 0; i < this.warehouseCount; i++)
//            for (int j = 0; j < TpccConfig.configDistPerWhse; j++)
//                usedTerminals[i][j] = 0;
//
//        BenchContext.CallbackHandler callbackHandler = new BenchContext.CallbackHandler(this.getId());
//
//        for (int i = 0; i < this.terminalNum; i++) {
//            int terminalWarehouseID;
//            int terminalDistrictID;
//            do {
//                terminalWarehouseID = rnd.nextInt(1, this.warehouseCount);
//                terminalDistrictID = rnd.nextInt(1, TpccConfig.configDistPerWhse);
//            }
//            while (usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] == 1);
//            usedTerminals[terminalWarehouseID - 1][terminalDistrictID - 1] = 1;
//
//            String terminalName = this.getId() + "/Term-" + (i >= 9 ? "" + (i + 1) : "0" + (i + 1));
//
//            TpccTerminal terminal = new TpccTerminal(this.clientProxy, terminalName, i, this.warehouseCount,
//                    terminalWarehouseID, terminalDistrictID, transactionNum, iNewOrderWeight, iPaymentWeight, iDeliveryWeight, iOrderStatusWeight, iStockLevelWeight,
//                    -1, this, callbackHandler, runningMode);
////            System.out.println("Client " + this.clientProxy.getId() + " starting terminal for warehouse/district " + terminalWarehouseID + "/" + terminalDistrictID);
//            terminal.setLogger(logger);
//            terminals[i] = terminal;
//            terminalNames[i] = terminalName;
//            logger.debug(terminalName + "\t" + terminalWarehouseID);
//        }
////        sessionEndTargetTime = executionTimeMillis;
//        signalTerminalsRequestEndSent = false;
//
//        synchronized (terminals) {
//            logger.debug("Starting all terminals... Terminal count: " + terminals.length);
//            transactionCount = 1;
//            for (int i = 0; i < terminals.length; i++)
//                (new Thread(terminals[i])).start();
//
//        }
//
//    }

//    public void runAuto(HeronAgent agent, int iNewOrderWeight, int iPaymentWeight, int iDeliveryWeight, int iOrderStatusWeight, int iStockLevelWeight, String terminalDistribution) {
    public void runAuto(HeronAgent agent,
                        int iNewOrderWeight, int iPaymentWeight, int iDeliveryWeight, int iOrderStatusWeight, int iStockLevelWeight,
                        int clientWarehouseId, int clientDistrictId) {

        try {
            BenchContext.CallbackHandler callbackHandler =
                    new BenchContext.CallbackHandler(RamcastConfig.getInstance().getGroupCount() * groupId + nodeId);

//            String[] terminalParts = terminalDistribution.split("_");
////             create a number of terminals for this client
//            terminals = new TpccTerminal[terminalParts.length];
//            terminalNames = new String[terminalParts.length];
//            terminalsStarted = terminalParts.length;
//            for (int i = 0; i < terminalParts.length; i++) {
//                String terminalName = this.nodeId + "/" + this.groupId + " Term-" + terminalParts[i];
//                String[] terminalDetail = terminalParts[i].split(":");
//                int terminalWarehouseID = Integer.parseInt(terminalDetail[0].split("=")[1]);
//                int terminalDistrictID = Integer.parseInt(terminalDetail[1].split("=")[1]);
//                TpccTerminal terminal = new TpccTerminal(agent,
//                        terminalName, i, this.warehouseCount,
//                        terminalWarehouseID, terminalDistrictID, transactionNum,
//                        iNewOrderWeight, iPaymentWeight, iDeliveryWeight, iOrderStatusWeight, iStockLevelWeight,
//                        -1, this, callbackHandler);
//                if (RamcastConfig.LOG_ENABLED)
//                    logger.info(
//                            "Client " + this.nodeId + "/" + this.groupId + " starting terminal for warehouse/district "
//                                    + terminalWarehouseID + "/" + terminalDistrictID + " and tx pattern NewOrder:{} Payment:{} Delivery:{} OrderStatus:{} StockLevel:{}",
//                            iNewOrderWeight, iPaymentWeight, iDeliveryWeight, iOrderStatusWeight, iStockLevelWeight);
//                terminal.setLogger(logger);
//                terminals[i] = terminal;
//                terminalNames[i] = terminalName;
//                logger.debug(terminalName + "\t" + terminalWarehouseID);
//            }

            String terminalName = this.nodeId + "/" + this.groupId + " Term-" + clientWarehouseId + "/" + clientDistrictId;
            TpccTerminal terminal = new TpccTerminal(agent,
                    terminalName, 0, this.warehouseCount,
                    clientWarehouseId, clientDistrictId, transactionNum,
                    iNewOrderWeight, iPaymentWeight, iDeliveryWeight, iOrderStatusWeight, iStockLevelWeight,
                    -1, this, callbackHandler, pid, numProcesses);
            if (RamcastConfig.LOG_ENABLED)
                logger.info(
                        "Client " + this.nodeId + "/" + this.groupId + " starting terminal for warehouse/district "
                                + clientWarehouseId + "/" + clientDistrictId + " and tx pattern NewOrder:{} Payment:{} Delivery:{} OrderStatus:{} StockLevel:{}",
                        iNewOrderWeight, iPaymentWeight, iDeliveryWeight, iOrderStatusWeight, iStockLevelWeight);
            terminal.setLogger(logger);
            terminals = new TpccTerminal[1];
            terminals[0] = terminal;
            terminalNames = new String[1];
            terminalNames[0] = terminalName;
            logger.debug(terminalName + "\t" + clientWarehouseId);

            signalTerminalsRequestEndSent = false;

            // todo: is this important? I could not found in DynaStar Client.
//        clientProxy.setInvalidCacheCallback(() -> {
//            for (int i = 0; i < terminals.length; i++) {
//                terminals[i].onCacheInvalidated();
//            }
//        });

            synchronized (terminals) {
                if (RamcastConfig.LOG_ENABLED)
                    logger.debug("Starting all terminals... Terminal count: " + terminals.length);
                transactionCount = 1;
                for (int i = 0; i < terminals.length; i++)
                    (new Thread(terminals[i])).start();

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public void runInteractive() {
        System.out.println("input format: r(ead)/n(ew order)/p(ayment)/order (s)tatus/d(elivery)/stock (l)evel/a(uto)/(or end to finish)");
        Scanner scan = new Scanner(System.in);
        String input;
        input = scan.nextLine();
        try {
            while (!input.equalsIgnoreCase("end")) {
                String[] params = input.split(" ");
                String opStr = params[0];
                if (opStr.equalsIgnoreCase("n")) {
                    execute(NEW_ORDER);
                } else if (opStr.equalsIgnoreCase("p")) {
                    execute(PAYMENT);
                } else if (opStr.equalsIgnoreCase("s")) {
                    execute(ORDER_STATUS);
                } else if (opStr.equalsIgnoreCase("d")) {
                    execute(DELIVERY);
                } else if (opStr.equalsIgnoreCase("l")) {
                    execute(STOCK_LEVEL);
                } else if (opStr.equalsIgnoreCase("r")) {
                    // todo: possible to have read command?
//                    //r District:d_w_id=2:d_id=1
//                    //r District:d_w_id=1:d_id=1
//                    //
//                    Map<String, Object> payload = new HashMap<>();
//                    payload.put("key", params[1]);
//                    Command cmd = new Command(TpccCommandType.READ, payload);
//                    CompletableFuture<Message> cmdEx = clientProxy.executeCommand(cmd);
//                    Message ret = cmdEx.get();
//                    System.out.println(ret);
                } else if (opStr.equalsIgnoreCase("a")) {
                    for (int i = 0; i < 5000; i++) {
                        execute(NEW_ORDER);
                    }
                }
                input = scan.nextLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        scan.close();

    }

    public void signalTerminalEnded(TpccTerminal tpccTerminal, int newOrderCounter) {
    }


    public void signalTerminalEndedTransaction(int transactionNum, String terminalName, String transactionType, long executionTime, String comment, int newOrder) {
        transactionCount++;
        fastNewOrderCounter += newOrder;
        if (transactionNum == 1) {
            sessionStartTimestamp = System.currentTimeMillis();
        }
        if (transactionNum >= 1) {
            if (sessionEndTargetTime != -1 && System.currentTimeMillis() > sessionEndTargetTime) {
                signalTerminalsRequestEnd(true);
            }
//            updateStatusLine();
        }
    }


    private void signalTerminalsRequestEnd(boolean timeTriggered) {
        synchronized (terminals) {
            if (!signalTerminalsRequestEndSent) {
                if (timeTriggered)
                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug("The time limit has been reached.");
                if (RamcastConfig.LOG_ENABLED)
                    logger.debug("Signalling all terminals to stop...");
                signalTerminalsRequestEndSent = true;

                for (int i = 0; i < terminals.length; i++)
                    if (terminals[i] != null)
                        terminals[i].stopRunningWhenPossible();
                if (RamcastConfig.LOG_ENABLED)
                    logger.debug("Waiting for all active transactions to end...");
            }
        }
    }


    synchronized private void updateStatusLine() {
        updateStatusLine(this.nodeId + "/" + this.groupId + " Term-00");
    }

    synchronized private void updateStatusLine(String terminalName) {
        long currTimeMillis = System.currentTimeMillis();

        if (currTimeMillis > sessionNextTimestamp) {
            StringBuilder informativeText = new StringBuilder("");
            Formatter fmt = new Formatter(informativeText);
            double tpmC = (6000000 * fastNewOrderCounter / (currTimeMillis - sessionStartTimestamp)) / 100.0;
            double tpmTotal = (6000000 * transactionCount / (currTimeMillis - sessionStartTimestamp)) / 100.0;

            sessionNextTimestamp += 1000;  /* update this every seconds */

            fmt.format(terminalName + ", Running Average tpmTOTAL: %.2f", tpmTotal);

            /* XXX What is the meaning of these numbers? */
            recentTpmC = (fastNewOrderCounter - sessionNextKounter) * 12;
            recentTpmTotal = (transactionCount - sessionNextKounter) * 12;
            sessionNextKounter = fastNewOrderCounter;
            fmt.format("    Current tpmTOTAL: %d", recentTpmTotal);

            long freeMem = Runtime.getRuntime().freeMemory() / (1024 * 1024);
            long totalMem = Runtime.getRuntime().totalMemory() / (1024 * 1024);
            fmt.format("    Memory Usage: %dMB / %dMB          ", (totalMem - freeMem), totalMem);

            if (RamcastConfig.LOG_ENABLED)
                logger.info(informativeText.toString());
//            for (int count = 0; count < 1 + informativeText.length(); count++)
//                System.out.print("\b");
        }
    }
}

