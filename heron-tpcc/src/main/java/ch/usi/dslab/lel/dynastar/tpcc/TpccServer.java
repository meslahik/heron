package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.codecs.Codec;
import ch.usi.dslab.bezerra.netwrapper.codecs.CodecUncompressedKryo;
import ch.usi.dslab.lel.dynastar.tpcc.command.Command;
import ch.usi.dslab.lel.dynastar.tpcc.rows.*;
import ch.usi.dslab.lel.dynastar.tpcc.tables.CustomerTable;
import ch.usi.dslab.lel.dynastar.tpcc.tables.StockTable;
import ch.usi.dslab.lel.dynastar.tpcc.tables.Tables;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccDataGeneratorV3;
import ch.usi.dslab.lel.ramcast.MessageDeliveredCallback;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.heron.HeronAgent;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TpccServer { // extends PartitionStateMachine {
    public static final Logger logger = LoggerFactory.getLogger(TpccServer.class);

    // todo: partitionId assigned correctly?
    int groupId;
    int nodeId;
    int pId;
    int warehouseId;
//    HeronAgent agent;
    Map<Integer, List<RamcastEndpoint>> warehouseEndpointsMap;

//    TpccProcedure appProcedure;
//    public PRObjectMap objectMap;
//    protected Map<String, Set<ObjId>> secondaryIndex = new ConcurrentHashMap<>();

    Tables tables;
    Tables[][] remoteTables;

//    protected String gathererHost = null;
//    protected int gathererPort;
//    protected String fileDirectory;
//    protected int gathererDuration;
//    protected int warmupTime;

//    private ThroughputPassiveMonitor tpMonitor;
//    private LatencyPassiveMonitor latMonitor;

    // todo: what is this? how ids used?
//    private AtomicLong nextObjId = new AtomicLong(0);
//    private AtomicInteger nextObjId = new AtomicInteger(0);
    private AtomicInteger exceptionCount = new AtomicInteger(0);

    // run by servers after delivery, a server is also a client to measure latency
    MessageDeliveredCallback onExecute = (data) -> {
        ByteBuffer buffer = ((RamcastMessage) data).getMessage();
        Message msg = Message.createFromByteBufferWithLengthHeader(buffer);
        Command cmd = (Command) msg.getNext();
        executeCommand(cmd, 0,0);
//        if (isClient && ((RamcastMessage) data).getMessage().getInt(0) == clientId) {
//            long time = System.nanoTime();
//            long startTime = ((RamcastMessage) data).getMessage().getLong(4);
//            latMonitor.logLatency(startTime, time);
//            cdfMonitor.logLatencyForDistribution(startTime, time);
//        }
//
//        RamcastMessage message = (RamcastMessage) data;
//        if (message.getId() % 50000 == 0)
//            logger.info("sent messages: {}", message.getId());
////            releasePermit();
    };

//    public TpccServer(int nodeId, int groupId, int pid, HeronAgent agent, TpccProcedure appProcedure) {
//    public TpccServer(int nodeId, int groupId, int pid, String dataFile, HeronAgent agent) {
//        // todo: needed?
////        super(replicaId, systemConfig, partitionsConfig, appProcedure);
//        this.groupId = groupId;
//        this.nodeId = nodeId;
//        this.pId = pid;
//        this.warehouseId = groupId + 1;
////        this.agent = agent;
//        warehouseEndpointsMap = agent.getEndpointGroup().getGroupEndpointsMap();
//
//        // todo: needed
////        this.appProcedure = appProcedure;
//        tables = new Tables(agent.getServerEp());
//        TpccDataGeneratorV3.loadCSVData(dataFile, tables, warehouseId);
//
////        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.warehouseTable.get(1));
////        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.districtTable.get(2));
////        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.itemsTable.get(2));
////        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.customerTables[1].get(1));
////        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.customerTables[2].get(2));
////        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.stockTable.get(2));
////        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.historyTable.get(2));
////        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.orderTables[1].get(2));
////        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.orderTables[2].get(2));
//    }

    public TpccServer(int nodeId, int groupId, int pid, String dataFile, HeronAgent agent,
                      Tables tables, Tables[][] remoteTables, boolean isClient) {
        // todo: needed?
//        super(replicaId, systemConfig, partitionsConfig, appProcedure);
        this.groupId = groupId;
        this.nodeId = nodeId;
        this.pId = pid;
        this.warehouseId = groupId + 1;
//        this.agent = agent;
        warehouseEndpointsMap = agent.getEndpointGroup().getGroupEndpointsMap();
        // todo: needed
//        this.appProcedure = appProcedure;
        this.tables = tables;
        this.remoteTables = remoteTables;

        preLoadData(dataFile, isClient);
//        logger.info("started loading data: {}", dataFile);
//        TpccDataGeneratorV3.loadCSVData(dataFile, tables, warehouseId);
//        logger.info("successfully loaded data");

//        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.warehouseTable.get(1));
//        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.districtTable.get(2));
//        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.itemsTable.get(2));
//        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.customerTables[1].get(1));
//        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.customerTables[2].get(2));
//        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.stockTable.get(2));
//        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.historyTable.get(2));
//        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.orderTables[1].get(2));
//        System.out.println("Warehouse: " + (groupId+1) + ", " + tables.orderTables[2].get(2));
    }

//    public static void main(String[] args) {
//        logger.debug("Entering main().");
//
//        if (args.length != 10) {
//            System.err.println("usage: " +
//                    "groupId | " +
//                    "nodeId | " +
//                    "configFile | " +
//                    "payloadSize | " +
//                    "database | " +
//                    "gathererHost | " +
//                    "gathererPort | " +
//                    "monitorLogDir | " +
//                    "gathererDuration | " +
//                    "gathererWarmup");
//            System.exit(1);
//        }
//        int argIndex = 0;
//        int groupId = Integer.parseInt(args[argIndex++]);
//        int nodeId = Integer.parseInt(args[argIndex++]);
//        String configFile = args[argIndex++];
//        int payloadSize = Integer.parseInt(args[argIndex++]);
//        String database = args[argIndex++];
//        String gathererHost = args[argIndex++];
//        int gathererPort = Integer.parseInt(args[argIndex++]);
//        String gathererDir = args[argIndex++];
//        int gathererDuration = Integer.parseInt(args[argIndex++]);
//        int gathererWarmup = Integer.parseInt(args[argIndex++]);
//        TpccProcedure appProcedure = new TpccProcedure();
//        TpccServer server = new TpccServer(nodeId, groupId, pid, agent, configFile, payloadSize, appProcedure);
//        server.preLoadData(database, gathererHost);
//        appProcedure.init("SERVER", server.secondaryIndex, logger, server.groupId);
//        server.setupMonitoring(gathererHost, gathererPort, gathererDir, gathererDuration, gathererWarmup);
//    }

//    public void setupMonitoring(String gathererHost, int gathererPort, String fileDirectory, int gathererDuration, int warmupTime) {
//        this.gathererHost = gathererHost;
//        this.gathererPort = gathererPort;
//        this.fileDirectory = fileDirectory;
//        this.warmupTime = warmupTime;
//        this.gathererDuration = gathererDuration;
//        DataGatherer.configure(gathererDuration, fileDirectory, gathererHost, gathererPort, warmupTime);
////        cpuMoniter = new CPUEmbededMonitorJavaMXBean(replicaId, isOracle() ? "ORACLE" : "PARTITION");
////        cpuMoniter.startLogging();
////        this.tpMonitor = new ThroughputPassiveMonitor(
////                RamcastConfig.SIZE_MSG_GROUP_COUNT * groupId + nodeId, "client_overall", true);
//    }

    // data is stored in redis for simplicity to be loaded
    // if not available, loaded from files
//    public static void preLoadData(String file) {
//        String redisHost;
//        String hostName = null;
//        try {
//            hostName = InetAddress.getLocalHost().getHostName();
//            logger.debug("hostname: {}", hostName);
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        if (hostName.indexOf("node") == 0) {
//            redisHost = "192.168.3.10";
//        } else if (hostName.indexOf("meslahik") == 0) {
//            redisHost = "127.0.0.1";
//        } else {
//            redisHost = "172.31.42.68";
//        }
//
//        boolean cacheLoaded = false;
//        long start = System.currentTimeMillis();
//        String[] fileNameParts = file.split("/");
//        String fileName = fileNameParts[fileNameParts.length - 1];
//        int warehouseCount = Integer.parseInt(fileName.split("_")[1]);
//
//        boolean redisAvailabled = true;
//        logger.debug("creating connection to redis host {}" + redisHost);
//        BinaryJedis jedis = new BinaryJedis(redisHost, 6379, 600000);
//        logger.debug("connected to redis!");
//
//        Codec codec = new CodecUncompressedKryo();
//        byte[] keyObjectGraph = (fileName + "_p_" + warehouseCount + "_SERVER_" + this.partitionId + "_objectGraph").getBytes();
//        byte[] keySecondaryIndex = (fileName + "_p_" + warehouseCount + "_SERVER_" + this.partitionId + "_secondaryIndex").getBytes();
//        byte[] keyDataLoaded = (fileName + "_p_" + warehouseCount + "_SERVER_" + this.partitionId + "_data_loaded").getBytes();
//        try {
//            byte[] cached = jedis.get(keyDataLoaded);
//            if (cached != null && new String(cached).equals("OK")) {
//                logger.info("[SERVER" + this.partitionId + "] loading sample data from cache..." + System.currentTimeMillis());
//                byte[] objectGraph = jedis.get(keyObjectGraph);
//                byte[] secondaryIndex = jedis.get(keySecondaryIndex);
//                logger.info("[SERVER" + this.partitionId + "] reading data length: objectGraph=" + objectGraph.length + " - secondaryIndex=" + secondaryIndex.length);
//                this.objectGraph = (PRObjectGraph) codec.createObjectFromBytes(objectGraph);
//                this.secondaryIndex = (ConcurrentHashMap<String, Set<ObjId>>) codec.createObjectFromBytes(secondaryIndex);
//                this.objectGraph.setLogger(this.logger);
//                cacheLoaded = true;
//            }
//        } catch (JedisConnectionException e) {
//            logger.info("[SERVER" + this.partitionId + "] Redis Cache is not available. Loading from file");
//            redisAvailabled = false;
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(-1);
//        }
//
//        if (!cacheLoaded) {
//            logger.info("[SERVER" + this.partitionId + "] loading sample data from file..." + System.currentTimeMillis());
//            TpccUtil.loadDataToCache(file, this.objectGraph, this.secondaryIndex, (objId, obj) -> {
//                int dest = TpccUtil.mapIdToPartition(objId);
//                if (nextObjId.get() <= objId.value) nextObjId.set(objId.value + 1);
////                if (obj.get("model").equals("Customer")) {
//////                    System.out.println("[SERVER" + this.partitionId + "] indexing " + objId);
////                }
//                if (this.objectGraph.getNode(objId) != null) {
//                    System.out.println("Duplicated ID:" + objId);
//                    System.exit(1);
//                }
//                if (this.partitionId == dest || obj.get("model").equals("Item")) {
//                    TpccCommandPayload payload = new TpccCommandPayload(obj);
//                    createObject(objId, payload);
////                    System.out    .println("[SERVER" + this.partitionId + "] indexing " + objId);
//                } else {
//                    PRObjectNode node = indexObject(objId, dest);
////                    logger.info("[SERVER" + this.partitionId + "] indexing only " + objId);
//                    node.setPartitionId(dest);
//                }
//            });
//            byte[] objectGraph = codec.getBytes(this.objectGraph);
//            byte[] secondaryIndex = codec.getBytes(this.secondaryIndex);
//            logger.info("[SERVER" + this.partitionId + "] writing data length: objectGraph=" + objectGraph.length + " - secondaryIndex=" + secondaryIndex.length);
//            if (redisAvailabled) {
//                jedis.set(keyObjectGraph, objectGraph);
//                jedis.set(keySecondaryIndex, codec.getBytes(this.secondaryIndex));
//                jedis.set(keyDataLoaded, new String("OK").getBytes());
//            }
//        }
//
//        logger.info("[SERVER" + this.partitionId + "] Data loaded, takes " + (System.currentTimeMillis() - start));
//    }

//    public TpccServer() {
//    }

//    public static void main(String[] args) {
//        Tables tables = new Tables();
//        TpccDataGeneratorV3.loadCSVData(
//                "/Users/meslahik/MyMac/PhD/Code/SharedMemorySSMR/dynastar-tpcc/bin-heron/databases/w_2_d_10_c_3000_i_100000.data",
//                tables, 1);
//        System.out.println(tables.warehouseTable.get(1));
//        System.out.println(tables.districtTable.get(1));
//        System.out.println(tables.itemsTable.get(1));
//        logger.info("Data load complete");
//    }

    public void preLoadData(String dataFile, boolean isClient) {
//        String hostName = null;
//        try {
//            hostName = InetAddress.getLocalHost().getHostName();
////            logger.info("hostname: {}" + hostName);
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        if (hostName.indexOf("node") == 0) {
//
//        } else if (hostName.indexOf("Meslahik") == 0) {
//            redisHost = "127.0.0.1";
//        } else {
//            redisHost = "172.31.42.68";
//        }
        String redisHost = "10.10.1.1";
//        redisHost = "192.168.3.10";

        boolean cacheLoaded = false;
        boolean redisAvailabled = true;

        String[] fileNameParts = dataFile.split("/");
        String fileName = fileNameParts[fileNameParts.length - 1];
        int warehouseCount = Integer.parseInt(fileName.split("_")[1]);;

//        logger.info("creating connection to redis host {}" + redisHost);
        BinaryJedis jedis = new BinaryJedis(redisHost, 56278, 600000);
//        BinaryJedis jedis = new BinaryJedis(redisHost, 6379, 600000);
            logger.info("connected to redis at " + redisHost);

        Codec codec = new CodecUncompressedKryo();
//        byte[] keyTables = (fileName + "_p_" + warehouseCount + "_SERVER_" + (warehouseId-1) + "_warehouse").getBytes();
//        byte[] keyDataLoaded = (fileName + "_p_" + warehouseCount + "_SERVER_" + (warehouseId-1) + "_data_loaded").getBytes();

        String keyTablesName;
        String keyDataLoadedName;
        if (warehouseId <= warehouseCount) {
            keyTablesName = fileName + "_p_" + warehouseCount + "_SERVER_" + (warehouseId) + "_warehouse";
            keyDataLoadedName = fileName + "_p_" + warehouseCount + "_SERVER_" + (warehouseId) + "_data_loaded";
        }
        else {
            keyTablesName = fileName + "_p_" + warehouseCount + "_SERVER_" + 0 + "_warehouse";
            keyDataLoadedName = fileName + "_p_" + warehouseCount + "_SERVER_" + 0 + "_data_loaded";
        }

        byte[] keyTables = (keyTablesName).getBytes();
        byte[] keyDataLoaded = (keyDataLoadedName).getBytes();

//        jedis.del(keyTables);
//        jedis.del(keyDataLoaded);

        long start = System.currentTimeMillis();
        try {
            byte[] cached = jedis.get(keyDataLoaded);
            if (cached != null && new String(cached).equals("OK")) {
                    logger.info("loading data from Redis: {}", keyTablesName);
                byte[] tablesBytes = jedis.get(keyTables);
//                byte[] secondaryIndex = jedis.get(keySecondaryIndex);
//                logger.info("read data length: " + tablesBytes.length + " bytes");
                Tables tablesRedis = (Tables) codec.createObjectFromBytes(tablesBytes);
                tables.copyFrom(tablesRedis);
//                this.secondaryIndex = (ConcurrentHashMap<String, Set<ObjId>>) codec.createObjectFromBytes(secondaryIndex);
                logger.info("successfully loaded data from Redis, length " + tablesBytes.length + ", took " + (System.currentTimeMillis() - start) + " ms. storing data in Redis...");
                cacheLoaded = true;
            }
        } catch (JedisConnectionException e) {
            logger.info("Redis Cache is not available. Loading from file");
            redisAvailabled = false;
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        if (!cacheLoaded) {
            logger.info("Redis does not have the data. loading data from file for key {}", keyTablesName);
            TpccDataGeneratorV3.loadCSVData(dataFile, tables, warehouseId);

            tables.stockTable.setBufferArray();
            for (int i=1; i<11; i++)
                tables.customerTables[i].setBufferArray();
            byte[] tablesBytes = codec.getBytes(tables);
            logger.info("successfully loaded data from file, took " + (System.currentTimeMillis() - start) + " ms. storing data size {} in Redis...", tablesBytes.length);
//            byte[] secondaryIndex = codec.getBytes(this.secondaryIndex);
            if (redisAvailabled) {
                jedis.set(keyTables, tablesBytes);
//                jedis.set(keySecondaryIndex, codec.getBytes(this.secondaryIndex));
                jedis.set(keyDataLoaded, new String("OK").getBytes());
                logger.info("stored data {} in Redis, length: {} bytes", keyTablesName, tablesBytes.length);
            }
        }
//        logger.info("{}", tables.itemsTable.get(1));
//        logger.info("{}", tables.warehouseTable.get(1));
//        logger.info("{}", tables.districtTable.get(1));
//        logger.info("{}", tables.stockTable.get(1));
        logger.info("Data load complete");
    }

    Stock readRemoteStockRow(int warehouseId, int row, int reqTs) {
        try {
            // todo: make node selection random
            // endpoints map index start from 0
            RamcastEndpoint endpoint = warehouseEndpointsMap.get(warehouseId - 1).get(0);
            StockTable s = remoteTables[warehouseId][0].stockTable;
            long rowAddr = s.getAddress() + row * Row.MODEL.STOCK.getRowSize();
            if (RamcastConfig.LOG_ENABLED)
                logger.trace("to issue remote read on Stock table in warehouse {}, node {}, address {}",
                    warehouseId,
                    endpoint.getNode(),
                    rowAddr);
            StockTable.remoteReadBuffer.clear();

            // reads remote stock row (duplicates) into StockTable.stockReadBuffer
            endpoint.readRemoteMemory(
                    rowAddr,
                    s.getLkey(),
                    StockTable.remoteReadBuffer,
                    StockTable.remoteReadBuffer_lkey,
                    StockTable.remoteReadBuffer.capacity());

            // choose the correct entry and change bytes to Stock object
            // enters recovery mode in case node is lagging behind
            Stock stock = StockTable.getRemoteStock(reqTs);
            if (stock == null) {
                logger.error("remote stock is empty, go to recovery mode, this should never happen in wait-for-all mode");
                // enter recovery mode
                System.exit(-1);
            }

            if (RamcastConfig.LOG_ENABLED)
                logger.debug("successful remote read from warehouse {}, node {}, address {}, remote stock row {}",
                    warehouseId,
                    endpoint.getNode(),
                    rowAddr,
                    stock);
            return stock;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    Customer readRemoteCustomerRow(int warehouseId, int districtId, int row, int reqTs) {
        try {
            // todo: make node selection random
            // endpoints map index start from 0
            RamcastEndpoint endpoint = warehouseEndpointsMap.get(warehouseId - 1).get(1);
            CustomerTable c = remoteTables[warehouseId][1].customerTables[districtId];
            long rowAddr = c.getAddress() + row * Row.MODEL.CUSTOMER.getRowSize();
            if (RamcastConfig.LOG_ENABLED)
                logger.trace("to issue remote read on Customer table in warehouse {}, district {}, node {}, address {}",
                        warehouseId,
                        districtId,
                        endpoint.getNode(),
                        rowAddr);
            CustomerTable.remoteReadBuffer.clear();

            // reads remote customer row (duplicates) into CustomerTable.remoteReadBuffer
            endpoint.readRemoteMemory(
                    rowAddr,
                    c.getLkey(),
                    CustomerTable.remoteReadBuffer,
                    CustomerTable.remoteReadBuffer_lkey,
                    CustomerTable.remoteReadBuffer.capacity());

            // choose the correct entry and change bytes to Stock object
            // enters recovery mode in case node is lagging behind
            Customer customer = CustomerTable.getRemoteCustomer(reqTs);
            if (customer == null) {
                logger.error("remote stock is empty, go to recovery mode, this should never happen in wait-for-all mode");
                // enter recovery mode
                System.exit(-1);
            }

            if (RamcastConfig.LOG_ENABLED)
                logger.debug("successful remote read from warehouse {}, node {}, address {}, remote customer row {}",
                        warehouseId,
                        endpoint.getNode(),
                        rowAddr,
                        customer);
            return customer;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Message executeCommand(Command command, int msgId, int finalTs) {
        return new Message();
    }

    public void executeCommand(ByteBuffer serializedBuffer, int msgId, int finalTs) {
//        if (command.isInvalid()) {
//            logger.debug("cmd {} is invalid, causes transaction arboted");
//            return new Message("TRANSACTION_ARBOTED");
//        }
//        command.rewind();
//        // Command format : | byte op | ObjId o1 | int value |
////        logger.debug("cmd {} AppServer: Processing command {}-{}", command.getId(), command);
//        TpccCommandType cmdType = (TpccCommandType) command.getItem(0);
//        Map<String, Object> params = (Map<String, Object>) command.getItem(1);
////        Set<ObjId> extractedObjId = appProcedure.extractObjectId(command);
////        logger.debug("cmd {} Extracted id: {}", command, extractedObjId);
        logger.debug("executing {}", finalTs);
        serializedBuffer.clear();
        TpccCommandType cmdType = null;
        int type = serializedBuffer.getInt();
        if (type == 1)
            cmdType = TpccCommandType.NEW_ORDER;
        else if (type == 2)
            cmdType = TpccCommandType.PAYMENT;
        else if (type == 3)
            cmdType = TpccCommandType.ORDER_STATUS;
        else if (type == 4)
            cmdType = TpccCommandType.DELIVERY;
        else if (type == 5)
            cmdType = TpccCommandType.STOCK_LEVEL;

        try {
            switch (cmdType) {
                case READ: {
//                    String key = (String) params.get("key");
//                    ObjId objId = secondaryIndex.get(key).iterator().next();
//                    PRObjectNode node = this.objectGraph.getNode(objId);
                    // added PRObject instead of PRObjectNode
//                    PRObject object = this.objectMap.getPRObject(objId);
//                    logger.debug("READ: {}", object);
//                    logger.debug("READ-Dependencies: {}", object.getDependencyIds());
//                    return new Message(object);
//                    return new Message();
                }
                case NEW_ORDER: {
//                    ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
//                    ObjId districtObjId = (ObjId) params.get("d_obj_id");
//                    ObjId customerObjId = (ObjId) params.get("c_obj_id");
//                    int numItems = (int) params.get("ol_o_cnt");
//                    int allLocal = (int) params.get("o_all_local");
//                    Set<ObjId> itemObjIds = (HashSet) params.get("itemObjIds");
//                    Set<ObjId> supplierWarehouseObjIds = (Set<ObjId>) params.get("supplierWarehouseObjIds");
//                    Set<ObjId> stockObjIds = (Set<ObjId>) params.get("stockIds");

//                    int w_id = (int) params.get("w_id");
//                    int d_id = (int) params.get("d_id");
//                    int c_id = (int) params.get("c_id");
//                    int o_ol_cnt = (int) params.get("ol_o_cnt");
//                    int o_all_local = (int) params.get("o_all_local");
//                    int[] itemIDs = (int[]) params.get("itemIds");
//                    int[] supplierWarehouseIds = (int[]) params.get("supplierWarehouseIds");
//                    int[] orderQuantities = (int[]) params.get("orderQuantities");
//                    ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", w_id)).iterator().next();
//                    ObjId districtObjId = secondaryIndex.get(Row.genSId("District", w_id, d_id)).iterator().next();
                    //TODO testing
//                    ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", w_id, d_id, c_id)).iterator().next();
//                    ObjId customerObjId = new ObjId(Row.genSId("Customer", w_id, d_id, c_id));

                    int w_id = serializedBuffer.getInt();
                    int d_id = serializedBuffer.getInt();
                    int c_id = serializedBuffer.getInt();
                    int o_ol_cnt = serializedBuffer.getInt();
                    int o_all_local = serializedBuffer.getInt();
                    int[] itemIDs = new int[o_ol_cnt];
                    for (int i=0; i < o_ol_cnt; i++)
                        itemIDs[i] = serializedBuffer.getInt();
                    int[] supplierWarehouseIds = new int[o_ol_cnt];
                    for (int i=0; i < o_ol_cnt; i++)
                        supplierWarehouseIds[i] = serializedBuffer.getInt();
                    int[] orderQuantities = new int[o_ol_cnt];
                    for (int i=0; i < o_ol_cnt; i++)
                        orderQuantities[i] = serializedBuffer.getInt();

                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug(
                            "server executing cmd {} NewOrder: w_id={} d_id={} c_id={} o_ol_cnt={} o_all_local={} " +
                            "itemIds={} supplierWarehouseIds={} orderQuantities={}",
                            finalTs,
                            w_id,
                            d_id,
                            c_id,
                            o_ol_cnt,
                            o_all_local,
                            itemIDs,
                            supplierWarehouseIds,
                            orderQuantities);

                    // todo: added here for early exception, correct?
                    for (int index = 1; index <= o_ol_cnt; index++) {
                        int ol_number = index;
                        int ol_i_id = itemIDs[ol_number - 1];
                        // If I_ID has an unused value (see Clause 2.4.1.5), a "not-found" condition is signaled,
                        // resulting in a rollback of the database transaction (see Clause 2.4.2.3).
                        if (ol_i_id == -12345) {
                            // an expected condition generated 1% of the time in the test data...
                            // we throw an illegal access exception and the transaction gets rolled back later on
                            exceptionCount.getAndIncrement();
                            // todo: added here from the next if statement (commented), correct?
//                            return new Message("TRANSACTION_ARBOTED");
                        }
                    }

                    if (w_id == warehouseId) {
                        final List<Double> itemPrices = Arrays.asList(new Double[o_ol_cnt + 1]);
                        final List<String> itemNames = Arrays.asList(new String[o_ol_cnt + 1]);
                        final List<Double> orderLineAmounts = Arrays.asList(new Double[o_ol_cnt + 1]);
                        final List<Integer> stockQuantities = Arrays.asList(new Integer[o_ol_cnt + 1]);
                        final List<Character> brandGeneric = Arrays.asList(new Character[o_ol_cnt + 1]);

                        // The row in the WAREHOUSE table with matching W_ID is selected and W_TAX, the warehouse
                        // tax rate, is retrieved.
//                    Warehouse warehouse = (Warehouse) getObject(warehouseObjId);
                        Warehouse warehouse = tables.warehouseTable.get(w_id);
                        assert warehouse != null;

                        // The row in the DISTRICT table with matching D_W_ID and D_ ID is selected, D_TAX, the
                        // district tax rate, is retrieved, and D_NEXT_O_ID, the next available order number for
                        // the district, is retrieved and incremented by one.
//                    District district = (District) getObject(districtObjId);
                        District district = tables.districtTable.get(d_id);
                        assert district != null;
//                    PRObjectNode districtNode = this.objectGraph.getNode(districtObjId);
//                    assert districtNode != null;
                        district.d_next_o_id += 1;

                        // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected
                        // and C_DISCOUNT, the customer's discount rate, C_LAST, the customer's last name,
                        // and C_CREDIT, the customer's credit status, are retrieved.
//                    Customer customer = (Customer) getObject(customerObjId);
                        // todo: never used in tx??
                        Customer customer = tables.customerTables[d_id].get(c_id);
                        assert customer != null;

                        // A new row is inserted into both the NEW-ORDER table and the ORDER table to reflect the
                        // creation of the new order. O_CARRIER_ID is set to a null value. If the order includes
                        // only home order-lines, then O_ALL_LOCAL is set to 1, otherwise O_ALL_LOCAL is set to 0.
                        NewOrder newOrder = new NewOrder(w_id, d_id, district.d_next_o_id);
                        tables.newOrderTables[d_id].put(newOrder);
                        // todo: where does this id is used? is that the primary key of the new order entry?
//                    newOrder.setId(new ObjId(nextObjId.getAndIncrement()));
//                    PRObjectNode newOrderNode = indexObject(newOrder, this.partitionId);
//                    newOrderNode.setPartitionId(this.partitionId);
//                    districtNode.addDependencyIds(newOrderNode);
//                    List<String> keys = Row.genStrObjId(newOrder.toHashMap());
//                    System.out.println("NewOrder Keys:" + newOrder.getId() + " - " + keys);
//                    for (String key : keys) {
//                        appProcedure.addToSecondaryIndex(this.secondaryIndex, key, newOrder.getId(), null);
//                    }

                        Order order = new Order(w_id, d_id, c_id, district.d_next_o_id);
                        tables.orderTables[d_id].put(order);
//                    order.setId(new ObjId(nextObjId.getAndIncrement()));
                        order.o_entry_d = System.currentTimeMillis();
                        // The number of items, O_OL_CNT, is computed to match ol_cnt
                        order.o_ol_cnt = o_ol_cnt;
                        order.o_all_local = o_all_local;
//                    PRObjectNode orderNode = indexObject(order, this.partitionId);
//                    orderNode.setPartitionId(this.partitionId);
//                    districtNode.addDependencyIds(orderNode);
//                    keys = Row.genStrObjId(order.toHashMap());
//                    for (String key : keys) {
//                        addToSecondaryIndex(key, order.getId());
//                        appProcedure.addToSecondaryIndex(this.secondaryIndex, key, order.getId(), null);
//                    }

                        // add order id to customer's c_last_req
                        customer.c_last_order = order.o_id;
                        tables.customerTables[d_id].put(customer);

                        // in case of repetitive items in the same tx, must prevent reading remote value multiple times as it may end up wrong value.
                        // so, it's necessary to keep a local updated state for reading correct values
                        // therefore, need to keep stock out of items loop
                        Set<Stock> remoteStockRowsAlreadyRead = new HashSet<>();

                        // For each O_OL_CNT item on the order:
                        for (int index = 1; index <= o_ol_cnt; index++) {
                            int ol_number = index;
                            // todo: anything to be done for this arrays?
                            int ol_supply_w_id = supplierWarehouseIds[ol_number - 1];
                            int ol_i_id = itemIDs[ol_number - 1];
                            int ol_quantity = orderQuantities[ol_number - 1];

                            // todo: moved this if above for early exception. correct?
//                        // If I_ID has an unused value (see Clause 2.4.1.5), a "not-found" condition is signaled,
//                        // resulting in a rollback of the database transaction (see Clause 2.4.2.3).
//                        if (ol_i_id == -12345) {
//                            // an expected condition generated 1% of the time in the test data...
//                            // we throw an illegal access exception and the transaction gets rolled back later on
//                            exceptionCount.getAndIncrement();
//                            // added here from the next if statement (commented), todo: correct?
//                            return new Message("TRANSACTION_ARBOTED");
//                        }

                            // The row in the ITEM table with matching I_ID (equals OL_I_ID) is selected
                            // and I_PRICE, the price of the item, I_NAME, the name of the item, and I_DATA are retrieved.
//                        if (secondaryIndex.get(Row.genSId("Item", ol_i_id)) == null) {
//                        if (tables.itemsTable.get(ol_i_id)) == null) {
//                            return new Message("TRANSACTION_ARBOTED");
//                        }
//                        ObjId itemObjId = secondaryIndex.get(Row.genSId("Item", ol_i_id)).iterator().next();
//                        Item item = (Item) getObject(itemObjId);
                            Item item = tables.itemsTable.get(ol_i_id);
                            assert item != null;

                            itemPrices.set(ol_number - 1, item.i_price);
                            itemNames.set(ol_number - 1, item.i_name);

                            //TODO testing
//                        ObjId stockObjId = secondaryIndex.get(Row.genSId("Stock", ol_supply_w_id, ol_i_id)).iterator().next();
//                        ObjId stockObjId = new ObjId(Row.genSId("Stock", ol_supply_w_id, ol_i_id));
//                        Stock stock = (Stock) getObject(stockObjId);

                            Stock stock = null;
                            if (ol_supply_w_id == warehouseId) {
                                stock = tables.stockTable.get(ol_i_id);

//                    logger.debug("cmd {} retriving stock w_id={} i_id={} stock {}", command.getId(),ol_supply_w_id, ol_i_id, stock);
                                stockQuantities.set(ol_number - 1, stock.s_quantity);
                                if (stock.s_quantity - ol_quantity >= 10) {
                                    stock.s_quantity -= ol_quantity;
                                } else {
                                    stock.s_quantity += -ol_quantity + 91;
                                }

                                int s_remote_cnt_increment;
                                if (ol_supply_w_id == w_id) {
                                    s_remote_cnt_increment = 0;
                                } else {
                                    s_remote_cnt_increment = 1;
                                }
                                stock.s_ytd += ol_quantity;
                                stock.s_remote_cnt += s_remote_cnt_increment;

                                // update finalTs of the stock and write into the buffer
                                stock.setReqId(finalTs);
                                tables.stockTable.put(stock);
                            } else {
                                // must read remotely since stock values needed for updating some fields
                                boolean isRemoteStockAvailable = false;
                                Iterator<Stock> it = remoteStockRowsAlreadyRead.iterator();
                                while(it.hasNext()) {
                                    Stock s = it.next();
                                    if (s.s_w_id == ol_supply_w_id && s.s_i_id == ol_i_id) {
                                        stock = s;
                                        isRemoteStockAvailable = true;
                                    }
                                }

                                if (!isRemoteStockAvailable) {
                                    stock = readRemoteStockRow(ol_supply_w_id, ol_i_id, finalTs);
                                    remoteStockRowsAlreadyRead.add(stock);
                                }

                                stockQuantities.set(ol_number - 1, stock.s_quantity);
                                if (stock.s_quantity - ol_quantity >= 10) {
                                    stock.s_quantity -= ol_quantity;
                                } else {
                                    stock.s_quantity += -ol_quantity + 91;
                                }

                                int s_remote_cnt_increment;
                                if (ol_supply_w_id == w_id) {
                                    s_remote_cnt_increment = 0;
                                } else {
                                    s_remote_cnt_increment = 1;
                                }
                                stock.s_ytd += ol_quantity;
                                stock.s_remote_cnt += s_remote_cnt_increment;
                            }

                            double ol_amount = ol_quantity * item.i_price;
                            orderLineAmounts.set(ol_number - 1, ol_amount);

                            if (item.i_data.contains("GENERIC") && StandardCharsets.ISO_8859_1.decode(stock.s_data).toString().contains("GENERIC")) {
                                brandGeneric.set(ol_number - 1, 'B');
                            } else {
                                brandGeneric.set(ol_number - 1, 'G');
                            }
                            ByteBuffer ol_dist_info = null;
                            switch (district.d_id) {
                                case 1:
                                    ol_dist_info = stock.s_dist_01;
                                    break;
                                case 2:
                                    ol_dist_info = stock.s_dist_02;
                                    break;
                                case 3:
                                    ol_dist_info = stock.s_dist_03;
                                    break;
                                case 4:
                                    ol_dist_info = stock.s_dist_04;
                                    break;
                                case 5:
                                    ol_dist_info = stock.s_dist_05;
                                    break;
                                case 6:
                                    ol_dist_info = stock.s_dist_06;
                                    break;
                                case 7:
                                    ol_dist_info = stock.s_dist_07;
                                    break;
                                case 8:
                                    ol_dist_info = stock.s_dist_08;
                                    break;
                                case 9:
                                    ol_dist_info = stock.s_dist_09;
                                    break;
                                case 10:
                                    ol_dist_info = stock.s_dist_10;
                                    break;
                            }

                            OrderLine orderLine = new OrderLine(w_id, d_id, district.d_next_o_id, ol_number);
//                        orderLine.setId(new ObjId(nextObjId.getAndIncrement()));
//                        PRObjectNode orderLineNode = indexObject(orderLine, this.partitionId);
//                        orderLineNode.setPartitionId(this.partitionId);
//                        districtNode.addDependencyIds(orderLineNode);
//                        keys = Row.genStrObjId(orderLine.toHashMap());
//                        for (String key : keys) {
//                            addToSecondaryIndex(key, orderLine.getId());
//                            appProcedure.addToSecondaryIndex(this.secondaryIndex, key, orderLine.getId(), null);
//                        }
                            orderLine.ol_i_id = ol_i_id;
                            orderLine.ol_supply_w_id = ol_supply_w_id;
                            orderLine.ol_quantity = ol_quantity;
                            orderLine.ol_amount = ol_amount;
                            orderLine.ol_dist_info = StandardCharsets.ISO_8859_1.decode(ol_dist_info).toString();

//                        (appProcedure).storeOrderLine(orderLine.getId());
                            tables.orderTables[d_id].addOrderLine(orderLine);
                        }

//                return new Message(warehouse, district, customer, order, newOrder, itemPrices, itemNames, orderLineAmounts, stockQuantities, brandGeneric);
                    } else {
                        final List<Integer> stockQuantities = Arrays.asList(new Integer[o_ol_cnt + 1]);

                        for (int index = 1; index <= o_ol_cnt; index++) {
                            int ol_number = index;
                            // todo: anything to be done for this arrays?
                            int ol_supply_w_id = supplierWarehouseIds[ol_number - 1];
                            int ol_i_id = itemIDs[ol_number - 1];
                            int ol_quantity = orderQuantities[ol_number - 1];


                            if (ol_supply_w_id == warehouseId) {
                                Stock stock = tables.stockTable.get(ol_i_id);

                                stockQuantities.set(ol_number - 1, stock.s_quantity);
                                if (stock.s_quantity - ol_quantity >= 10) {
                                    stock.s_quantity -= ol_quantity;
                                } else {
                                    stock.s_quantity += -ol_quantity + 91;
                                }

                                int s_remote_cnt_increment;
                                if (ol_supply_w_id == w_id) {
                                    s_remote_cnt_increment = 0;
                                } else {
                                    s_remote_cnt_increment = 1;
                                }
                                stock.s_ytd += ol_quantity;
                                stock.s_remote_cnt += s_remote_cnt_increment;

                                stock.setReqId(finalTs);
                                tables.stockTable.put(stock);
                            }
                        }
                    }

                    if (RamcastConfig.LOG_ENABLED)
                        logger.debug("Executed successfully cmd {} finalTs {}: NewOrder: w_id={} d_id={} c_id={} o_ol_cnt={} o_all_local={} " +
                                    "itemIds={} supplierWarehouseIds={} orderQuantities={}",
                            msgId,
                            finalTs,
                            w_id,
                            d_id,
                            c_id,
                            o_ol_cnt,
                            o_all_local,
                            itemIDs,
                            supplierWarehouseIds,
                            orderQuantities);
//                    return new Message("OK");
                    break;
                }

                case PAYMENT: {
//                    int terminalWarehouseID = (int) params.get("w_id");
//                    int districtID = (int) params.get("d_id");
//                    int customerWarehouseID = (int) params.get("c_w_id");
//                    int customerDistrictID = (int) params.get("c_d_id");
//                    float amount = (float) params.get("amount");

                    int w_id = serializedBuffer.getInt();
                    int d_id = serializedBuffer.getInt();
                    int c_id = serializedBuffer.getInt();
                    int c_w_id = serializedBuffer.getInt();
                    int c_d_id = serializedBuffer.getInt();
                    float amount = serializedBuffer.getFloat();

                    logger.debug("cmd {} PAYMENT: w_id={} d_id={} c_id={} c_w_id={} c_d_id={} amount={}",
                            finalTs,
                            w_id,
                            d_id,
                            c_id,
                            c_w_id,
                            c_d_id,
                            amount);

                    if (w_id == warehouseId) {
                        // The warehouse with matching w_id is selected. W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved
                        // and W_YTD, the warehouse's year-to-date balance, is increased by H_ AMOUNT.
                        Warehouse warehouse = tables.warehouseTable.get(w_id);
                        assert warehouse != null;
                        warehouse.w_ytd += amount;

                        // The district with matching D_W_ID and D_ID is selected. D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
                        // and D_YTD, the district's year-to-date balance, is increased by H_AMOUNT.
                        District district = tables.districtTable.get(d_id);
                        assert district != null;
                        district.d_ytd += amount;

                        // Retrieve customer
                        Customer customer = null;
                        if (c_w_id == warehouseId)
                            customer = tables.customerTables[d_id].get(c_id);
                        else
                            customer = readRemoteCustomerRow(c_w_id, c_d_id, c_id, finalTs);

                        String w_name = warehouse.w_name;
                        String d_name = district.d_name;
                        if (w_name.length() > 10) w_name = w_name.substring(0, 10);
                        if (d_name.length() > 10) d_name = d_name.substring(0, 10);
                        String h_data = w_name + "    " + d_name;
                        History history = new History(customer.c_id, customer.c_d_id, customer.c_w_id);
                        history.h_date = System.currentTimeMillis();
                        history.h_amount = amount;
                        history.h_data = h_data;
                        tables.historyTable.put(history);
//                    history.setId(new ObjId(nextObjId.getAndIncrement()));
//                    PRObjectNode historyNode = indexObject(history, this.partitionId);
//                    historyNode.setPartitionId(this.partitionId);
//                    districtNode.addDependencyIds(historyNode);
//                    return new Message("OK");
                    } else {
                        // Retrieve customer
                        Customer customer = tables.customerTables[c_d_id].get(c_id);

                        customer.c_balance -= amount;
                        customer.c_payment_cnt += 1;
                        customer.c_ytd_payment += amount;

                        // StandardCharsets.ISO_8859_1.decode(stock.s_data).toString();
                        String credit = StandardCharsets.ISO_8859_1.decode(customer.c_credit).toString();
                        if (credit.equals("BC")) {  // bad credit
//                        String c_data = customer.c_data;
                            String c_data = StandardCharsets.ISO_8859_1.decode(customer.c_data).toString();
                            String c_new_data = customer.c_id + " " + customer.c_d_id + " " + customer.c_w_id + " " + d_id + " " + w_id + " " + amount + " |";
                            if (c_data.length() > c_new_data.length()) {
                                c_new_data += c_data.substring(0, c_data.length() - c_new_data.length());
                            } else {
                                c_new_data += c_data;
                            }
                            if (c_new_data.length() > 500) c_new_data = c_new_data.substring(0, 500);
//                        customer.c_data = c_new_data;
                            customer.c_data.clear().put(c_new_data.getBytes(StandardCharsets.ISO_8859_1));
                        }

                        tables.customerTables[c_d_id].put(customer);
                    }
                    break;
                }

                case ORDER_STATUS: {
//                    int terminalWarehouseID = (int) params.get("w_id");
//                    int districtID = (int) params.get("d_id");
//                    boolean customerByName = (boolean) params.get("c_by_name");
//                    ObjId customerObjId;
//                    int customerID = -1;
//                    if (customerByName) customerObjId = (ObjId) params.get("c_objId");
//                    else {
//                        customerID = (int) params.get("c_id");
////                        customerObjId = new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID));
//                        customerObjId = secondaryIndex.get(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)).iterator().next();
//                    }
//                    ObjId orderObjId = (ObjId) params.get("orderObjId");
//                    Set<ObjId> orderLineObjIds = (Set<ObjId>) params.get("orderLineObjIds");

                    int w_id = serializedBuffer.getInt();
                    int d_id = serializedBuffer.getInt();
                    int c_id = serializedBuffer.getInt();

                    logger.debug("cmd {} ORDER_STATUS: w_id={} d_id={} c_id={}",
                            finalTs,
                            w_id,
                            d_id,
                            c_id);

                    // Retrieve customer
                    Customer customer = tables.customerTables[d_id].get(c_id);
                    assert customer != null;
                    int lastOrderId = customer.c_last_order;

                    if (lastOrderId == 0) {
//                        return new Message("Order is empty");
                        break;
                    }

                    Order order = tables.orderTables[d_id].get(lastOrderId);
                    assert order != null;

//                    Set<OrderLine> orderLines = new HashSet<>();
//                    for (ObjId objId : orderLineObjIds) {
//                        OrderLine orderLine = (OrderLine) getObject(objId);
//                        assert orderLine != null;
//                        orderLines.add(orderLine);
//                    }
                    tables.orderTables[d_id].getOrderLines(order);

//                    return new Message("OK");
                    break;
                }

                case DELIVERY: {
//                    int terminalWarehouseID = (int) params.get("w_id");
//                    final ObjId orderIDs[] = new ObjId[TpccConfig.configDistPerWhse];
//                    int o_carrier_id = (int) params.get("o_carrier_id");
//                    ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
//                    Set<ObjId> districtObjIds = (HashSet) params.get("d_obj_ids");

                    int w_id = serializedBuffer.getInt();
                    int o_carrier_id = serializedBuffer.getInt();

                    logger.debug("cmd {} Delivery: w_id={} o_carrier_id={}",
                            finalTs,
                            w_id,
                            o_carrier_id);

                    Warehouse warehouse = tables.warehouseTable.get(w_id);
                    assert warehouse != null;

                    for (int districtId=1; districtId < 11; districtId++) {
                        logger.debug("cmd {} DELIVERY: District={}",
                                finalTs,
                                districtId);

                        District district = tables.districtTable.get(districtId);
                        assert district != null;

                        NewOrder newOrder = tables.newOrderTables[districtId].get(district.oldest_new_order);
                        if (newOrder == null) {
                            logger.debug("cannot find NewOrder {}, district {}", district.oldest_new_order, districtId);
                            break;
                        }
                        tables.newOrderTables[districtId].remove(newOrder);
                        district.oldest_new_order++;

                        Order order = tables.orderTables[districtId].get(newOrder.no_o_id);
                        if (order == null)
                            logger.error("ERROR: cannot find Order {}, district {}", newOrder.no_o_id, districtId);
                        order.o_carrier_id = o_carrier_id;

                        double total = 0;
                        List<OrderLine> list = tables.orderTables[districtId].getOrderLines(order);
                        if (list == null)
                            logger.error("ERROR: cannot find OrderLine items in order {}, district {}", order.o_id, districtId);
                        for (OrderLine orderLine: list) {
                            orderLine.ol_delivery_d = System.currentTimeMillis();
                            total += orderLine.ol_amount;
                        }

                        Customer customer = tables.customerTables[districtId].get(order.o_c_id);
                        if (customer == null)
                            logger.error("ERROR: cannot find Customer {}, district {}", order.o_c_id, districtId);
                        customer.c_balance += total;
                        tables.customerTables[districtId].put(customer);
                    }

//                    return new Message("OK");
                    break;
                }

                case STOCK_LEVEL: {
//                    int terminalWarehouseID = (int) params.get("w_id");
//                    int districtID = (int) params.get("d_id");
//                    int threshold = (int) params.get("threshold");

                    int w_id = serializedBuffer.getInt();
                    int d_id = serializedBuffer.getInt();
                    int threshold = serializedBuffer.getInt();

                    logger.debug("cmd {} Delivery: w_id={} d_id={} threshold={}",
                            finalTs,
                            w_id,
                            d_id,
                            threshold);

                    District district = tables.districtTable.get(d_id);

                    Set<Integer> stockIds = new HashSet<>();
                    for (int i = district.d_next_o_id; i >= district.d_next_o_id - 20; i--) {
                        Order order = tables.orderTables[d_id].get(i);
                        if (order == null) {
//                            logger.error("ERROR: Can't find Order {}, district.d_next_o_id {}", i, district.d_next_o_id);
                            continue;
                        }

                        List<OrderLine> list = tables.orderTables[d_id].getOrderLines(order);
                        if (list == null)
                            logger.error("ERROR: Can't find OrderLine");

                        for (OrderLine ol: list) {
                            Stock stock = tables.stockTable.get(ol.ol_i_id);
                            if (stock.s_quantity < threshold) {
                                stockIds.add(stock.s_i_id);
                            }
                        }
                    }

                    int count = stockIds.size();
                    logger.debug("Stock-level count: {}", count);
//                    return new Message("OK");
                }

            }
        } catch (Exception e) {
            logger.error("cmd {} has error {}", finalTs, e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

//    public PRObject getObject(ObjId id) {
//        return this.objectMap.getPRObject(id);
//    }

//    public PRObjectNode indexObject(PRObject obj, int partitionId) {
//        PRObjectNode node = new PRObjectNode(obj, obj.getId(), partitionId);
//        this.objectGraph.addNode(node);
//        return node;
//    }
//
//    public PRObjectNode indexObject(ObjId objId, int partitionId) {
//        PRObjectNode node = new PRObjectNode(null, objId, partitionId);
//        this.objectGraph.addNode(node);
//        return node;
//    }

//    protected PRObject createObject(PRObject prObject) {
////        logger.debug("Creating user {}.", prObject.getId());
////        User newUser = new User(prObject.getId());
//        return prObject;
//    }

//    public PRObject createObject(ObjId objId, Object value) {
//        // todo: tpccCommandPaylaod ??
//        if (value instanceof TpccCommandPayload) {
//            TpccCommandPayload payload = (TpccCommandPayload) value;
//            String modelName = (String) payload.attributes.get("model");
//            Row.MODEL modelObj = Row.getModelFromName(modelName);
////            logger.debug("Creating object " + modelName + " with id=" + objId.value);
//            String model = "ch.usi.dslab.lel.dynastar.tpcc.tables." + modelName;
//
//            try {
//                // create instance of 'model' with constructor of type ObjId
//                Class<?>[] params = Row.getConstructorParams();
//                Object xyz = Class.forName(model).getConstructor(params).newInstance(objId);
//
//                // todo: what are the payload attributes?
//                for (String attr : payload.attributes.keySet()) {
//                    if (!attr.equals("ObjId") && !attr.equals("model")) {
//                        Row.set(xyz, attr, payload.attributes.get(attr));
//                    }
//                }
//                objId.setSId(((Row) xyz).getObjIdString());
//                ((PRObject) xyz).setId(objId);
////                PRObjectNode node = indexObject((PRObject) xyz, partitionId);
////                node.setPartitionId(partitionId);
//                if (!modelName.equals("District") && !modelName.equals("Warehouse") && !modelName.equals("Item")) {
////                    node.setTransient(true);
//                    int w_id = 0, d_id = 1;
//                    if (modelName.equals("Customer")) {
//                        w_id = ((Customer) xyz).c_w_id;
//                        d_id = ((Customer) xyz).c_d_id;
//                    } else if (modelName.equals("History")) {
//                        w_id = ((History) xyz).h_c_w_id;
//                        d_id = ((History) xyz).h_c_d_id;
//                    } else if (modelName.equals("NewOrder")) {
//                        w_id = ((NewOrder) xyz).no_w_id;
//                        d_id = ((NewOrder) xyz).no_d_id;
//                    } else if (modelName.equals("Order")) {
//                        w_id = ((Order) xyz).o_w_id;
//                        d_id = ((Order) xyz).o_d_id;
//                    } else if (modelName.equals("OrderLine")) {
//                        w_id = ((OrderLine) xyz).ol_w_id;
//                        d_id = ((OrderLine) xyz).ol_d_id;
//                    } else if (modelName.equals("Stock")) {
//                        w_id = ((Stock) xyz).s_w_id;
//                        d_id = TpccUtil.mapStockToDistrict(objId.getSId());
//                    }
//                    try {
//                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", w_id, d_id)).iterator().next();
////                        PRObjectNode districtNode = this.objectGraph.getNode(districtObjId);
////                        assert districtNode != null;
////                        logger.debug("Creating object {} with id={} sid={} belongs to {}", modelName, objId.value, objId.getSId(), districtObjId);
//////                        if (modelName.equals("Customer"))
//////                            System.out.println("[" + partitionId + "] Creating object " + modelName + " with id=" + objId.value + " sId=" + objId.getSId());
////                        districtNode.addDependencyIds(node);
//                    } catch (Exception e) {
//                        System.out.println("ERROR getting " + Row.genSId("District", w_id, d_id));
//                    }
//                } else if (modelName.equals("Item")) {
////                    node.setTransient(true);
////                    node.setReplicated(true);
//                } else {
//
//                }
//                return (PRObject) xyz;
//            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException | InvocationTargetException | NullPointerException e) {
//                e.printStackTrace();
//            }
//            return null;
//        } else {
//            objId.setSId(((Row) value).getObjIdString());
//            ((PRObject) value).setId(objId);
////            PRObjectNode node = indexObject((PRObject) value, partitionId);
////            node.setPartitionId(partitionId);
//            return (PRObject) value;
//        }
//    }
}
