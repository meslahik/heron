package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.lel.dynastar.tpcc.benchmark.BenchContext;
import ch.usi.dslab.lel.dynastar.tpcc.rows.Row;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccConfig;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccUtil;
import ch.usi.dslab.lel.dynastar.tpcc.command.Command;
import ch.usi.dslab.lel.dynastar.tpcc.objects.ObjId;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.heron.HeronAgent;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;

public class TpccTerminal implements Runnable {
    private static final int SELECT_CUSTOMER_BY_NAME_PERCENTAGE = 0;
    private Logger logger;

//    private Client clientProxy;
    private int terminalWarehouseID, terminalDistrictID, newOrderCounter;
    private int numTransactions, newOrderWeight, paymentWeight, deliveryWeight, orderStatusWeight, stockLevelWeight, limPerMin_Terminal;
    private Random gen;
    private TpccClient parent;
    private String terminalName;
    private int terminalId, warehouseCount;
    private boolean stopRunningSignal = false;

    private Semaphore sendPermits;
    private BenchContext.CallbackHandler callbackHandler;
    private boolean goodToGo = true;
    private boolean isSSMRMode = false;

    HeronAgent agent;
    int msgeId = 0;
    private int txCount = 0;

    int pid;
    int numProcesses;

    boolean sanityCheck = false;
//    boolean sanityCheck = true;

    boolean multiPartitionOnly = false;
//    boolean multiPartitionOnly = true;
    int multiPartition = 4;

    public TpccTerminal(HeronAgent agent,
                        String terminalName, int terminalId, int warehouseCount, int warehouseId, int districtId,
                        int numTransactions, int newOrderWeight, int paymentWeight, int deliveryWeight,
                        int orderStatusWeight, int stockLevelWeight, int limPerMin_Terminal, TpccClient parent,
                        BenchContext.CallbackHandler callbackHandler,
                        int pid, int numProcesses) {
        this.pid = pid;
        this.numProcesses = numProcesses;

        this.agent = agent;
        this.terminalId = terminalId;
        this.warehouseCount = warehouseCount;
        this.terminalWarehouseID = warehouseId;
        this.terminalDistrictID = districtId;
        this.newOrderCounter = 0;
        this.newOrderWeight = newOrderWeight;
        this.paymentWeight = paymentWeight;
        this.orderStatusWeight = orderStatusWeight;
        this.deliveryWeight = deliveryWeight;
        this.stockLevelWeight = stockLevelWeight;
        this.limPerMin_Terminal = limPerMin_Terminal;
        this.numTransactions = numTransactions;
//        this.clientProxy = clientProxy;
        this.terminalName = terminalName;
        this.parent = parent;
        this.gen = new Random(System.nanoTime());
        this.callbackHandler = callbackHandler;
//        if (runningMode.equals(PartitionStateMachine.RUNNING_MODE_SSMR)) this.isSSMRMode = true;
        sendPermits = new Semaphore(1);
        // clients processes do not deliver messages since they are not in the destination of any message, null for onExecute
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public void run() {
        try {
            gen = new Random(System.nanoTime());
            executeTransactions(numTransactions);
            parent.signalTerminalEnded(this, newOrderCounter);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void stopRunningWhenPossible() {
        stopRunningSignal = true;
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("Terminal received stop signal!");
        if (RamcastConfig.LOG_ENABLED)
            logger.debug("Finishing current transaction before exit...");
    }

//    public void doNewOrder(Consumer then) throws Exception {
    public void doNewOrder() throws Exception {
        int[] itemIDs, supplierWarehouseIds, orderQuantities;

        int districtID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);
        int customerID = TpccUtil.getCustomerID(gen);

        // todo: change back to random
        int numItems = TpccUtil.randomNumber(5, 15, gen);
//        int numItems = 2;
        itemIDs = new int[numItems];
//        Set<ObjId> itemObjIds = new HashSet<>();
        supplierWarehouseIds = new int[numItems];
//        Set<ObjId> supplierWarehouseObjIds = new HashSet<>();
//        Set<ObjId> stockObjIds = new HashSet<>();
        orderQuantities = new int[numItems];
        int allLocal = 1;
        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TpccUtil.getItemID(gen);
//            itemObjIds.add(new ObjId(Row.genSId("Item", itemIDs[i])));

            if (sanityCheck)
                supplierWarehouseIds[i] = terminalWarehouseID;
            else {
                if (TpccUtil.randomNumber(1, 100, gen) > 1) {
                    supplierWarehouseIds[i] = terminalWarehouseID;
                } else {
                    do {
                        supplierWarehouseIds[i] = TpccUtil.randomNumber(1, this.warehouseCount, gen);
                    }
                    while (supplierWarehouseIds[i] == terminalWarehouseID && this.warehouseCount > 1);
                    allLocal = 0;
                }
            }

//            supplierWarehouseObjIds.add(new ObjId(Row.genSId("Warehouse", supplierWarehouseIds[i])));
//            stockObjIds.add(new ObjId(Row.genSId("Stock", supplierWarehouseIds[i], itemIDs[i])));
            orderQuantities[i] = TpccUtil.randomNumber(1, 10, gen);
        }

        // ensure the request is multi-partition (for latency breakdown benchmark)
        if (multiPartitionOnly) {
            supplierWarehouseIds[0] = 1;
            supplierWarehouseIds[1] = 2;
            if (multiPartition > 2)
                supplierWarehouseIds[2] = 3;
            if (multiPartition > 3)
                supplierWarehouseIds[3] = 4;
        }

        // todo: changed to make no rollback
//        // we need to cause 1% of the new orders to be rolled back.
//        if (TpccUtil.randomNumber(1, 100, gen) == 1) {
//            itemIDs[numItems - 1] = -12345;
//            itemObjIds.add(new ObjId(Row.genSId("Item", itemIDs[numItems - 1])));
//        }

//        Map<String, Object> params = new HashMap<>();
//        params.put("w_id", terminalWarehouseID);
////        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
//        params.put("d_id", districtID);
////        params.put("d_obj_id", new ObjId(Row.genSId("District", terminalWarehouseID, districtID)));
//        params.put("c_id", customerID);
////        params.put("c_obj_id", new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)));
//        params.put("ol_o_cnt", numItems);
//        params.put("o_all_local", allLocal);
//        params.put("itemIds", itemIDs);
////        params.put("itemObjIds", itemObjIds);
////        params.put("stockIds", stockObjIds);
//        params.put("supplierWarehouseIds", supplierWarehouseIds);
////        params.put("supplierWarehouseObjIds", supplierWarehouseObjIds);
//        params.put("orderQuantities", orderQuantities);
////        Command cmd = new Command(TpccCommandType.NEW_ORDER, params);

        ByteBuffer serializedBuffer = ByteBuffer.allocateDirect(24 + 12*numItems);  // 4 + 4 + 4 + 4 + 4 + 4 + 4*numItems + 4*numItems + 4*numItems
        serializedBuffer.putInt(1);  // NEW_ORDER
        serializedBuffer.putInt(terminalWarehouseID);
        serializedBuffer.putInt(districtID);
        serializedBuffer.putInt(customerID);
        serializedBuffer.putInt(numItems);
        serializedBuffer.putInt(allLocal);
        for (int i: itemIDs)
            serializedBuffer.putInt(i);
        for (int s: supplierWarehouseIds)
            serializedBuffer.putInt(s);
        for (int oq: orderQuantities)
            serializedBuffer.putInt(oq);
        serializedBuffer.clear();

//        logger.debug("cmd {} Client: sending command {}-{}", cmd.getId(), cmd);
        if (RamcastConfig.LOG_ENABLED)
            logger.debug(
                "client {} Starting txn {} NewOrder: w_id={} d_id={} c_id={} o_ol_cnt={} o_all_local={} " +
                        "itemIds={} supplierWarehouseIds={} orderQuantities={}",
                agent.getEndpointGroup().getAgent().getNode(),
                msgeId+1,
                terminalId,
                districtID,
                customerID,
                numItems,
                allLocal,
                itemIDs,
                supplierWarehouseIds,
                orderQuantities);

        ArrayList<RamcastGroup> dest = new ArrayList<>();
        dest.add(RamcastGroup.getGroup(terminalWarehouseID - 1));  // Ramcast groups' index start from 0
        if (allLocal == 0) {
            for (int warehouseId : supplierWarehouseIds) {
                if (!dest.contains(RamcastGroup.getGroup(warehouseId - 1))) {
                    dest.add(RamcastGroup.getGroup(warehouseId - 1));
                }
            }
        }
        Collections.sort(dest);

//        ByteBuffer buffer = cmd.getByteBufferWithLengthHeader();
//        buffer.clear();
//        logger.debug("Tx bytebuffer size {} before multicast to {}", buffer.getInt(), Arrays.toString(dest.toArray()));
//        buffer.clear();

        int id = pid + (msgeId++ * numProcesses);
//        if (txCount % 20000 == 1)
            logger.debug("client {}/{} to mcast message ID: {}", parent.nodeId, parent.groupId, id);

        BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.NEW_ORDER);
        Consumer then = o -> callbackHandler.accept(o, context);
        agent.execute(id, serializedBuffer, dest);
        then.accept(null);
//        releasePermit();
    }

    public void doPayment() throws Exception{
        int districtID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);

        int x = TpccUtil.randomNumber(1, 100, gen);
        int customerDistrictID;
        int customerWarehouseID;

        if (sanityCheck) {
            customerDistrictID = districtID;
            customerWarehouseID = terminalWarehouseID;
        } else {
            if (x <= 85) {
                customerDistrictID = districtID;
                customerWarehouseID = terminalWarehouseID;
            } else {
                customerDistrictID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);
                do {
                    customerWarehouseID = TpccUtil.randomNumber(1, this.warehouseCount, gen);
                }
                while (customerWarehouseID == terminalWarehouseID && this.warehouseCount > 1);
            }
        }

//        int y = TpccUtil.randomNumber(1, 100, gen);
        int customerID = -1;
//        boolean customerByName;
//        String customerLastName = null;
//        if (y <= SELECT_CUSTOMER_BY_NAME_PERCENTAGE) {
//            // 60% lookups by last name
//            customerByName = true;
//            customerLastName = TpccUtil.getLastName(new Random(System.nanoTime()));
//        } else {
//            // 40% lookups by customer ID
//            customerByName = false;
            customerID = TpccUtil.getCustomerID(gen);
//        }

        float paymentAmount = (float) (TpccUtil.randomNumber(100, 500000, gen) / 100.0);

//        Map<String, Object> params = new HashMap<>();
//        params.put("w_id", terminalWarehouseID);
//        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
//        params.put("d_id", districtID);
//        params.put("d_obj_id", new ObjId(Row.genSId("District", terminalWarehouseID, districtID)));
//        params.put("c_id", customerID);
//        params.put("c_w_id", customerWarehouseID);
//        params.put("c_w_obj_id", new ObjId(Row.genSId("Warehouse", customerWarehouseID)));
//        params.put("c_d_id", customerDistrictID);
//        params.put("c_d_obj_id", new ObjId(Row.genSId("District", customerWarehouseID, customerDistrictID)));
//        params.put("c_obj_id", new ObjId(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerID)));
//        params.put("c_by_name", customerByName);
//        params.put("c_name", customerLastName);
//        params.put("amount", paymentAmount);

//        Command cmd = new Command(TpccCommandType.PAYMENT, params);
//        if (isSSMRMode)
//            clientProxy.executeSSMRCommand(cmd).thenAccept(then);
//        else
//            clientProxy.executeCommand(cmd).thenAccept(then);
//        agent.execute(++msgeId, buffer, dest);

        ByteBuffer serializedBuffer = ByteBuffer.allocateDirect(28);  // 4 + 4 + 4 + 4 + 4 + 4 + 4
        serializedBuffer.putInt(2);  // Payment
        serializedBuffer.putInt(terminalWarehouseID);
        serializedBuffer.putInt(districtID);
        serializedBuffer.putInt(customerID);
        serializedBuffer.putInt(customerWarehouseID);
        serializedBuffer.putInt(customerDistrictID);
        serializedBuffer.putFloat(paymentAmount);
        serializedBuffer.clear();

//        logger.debug("cmd {} Client: sending command {}-{}", cmd.getId(), cmd);
        if (RamcastConfig.LOG_ENABLED)
            logger.debug(
                    "client {} starting txn {} Payment: w_id={} d_id={} c_id={} c_w_id={} c_d_id={} amount={}",
                    agent.getEndpointGroup().getAgent().getNode(),
                    msgeId+1,
                    terminalWarehouseID,
                    districtID,
                    customerID,
                    customerWarehouseID,
                    customerDistrictID,
                    paymentAmount);

        ArrayList<RamcastGroup> dest = new ArrayList<>();
        dest.add(RamcastGroup.getGroup(terminalWarehouseID - 1));  // Ramcast groups' index start from 0
        if (!dest.contains(RamcastGroup.getGroup(customerWarehouseID - 1))) {
            dest.add(RamcastGroup.getGroup(customerWarehouseID - 1));
        }
        Collections.sort(dest);

        int id = pid + (msgeId++ * numProcesses);
//        if (txCount % 20000 == 1)
        logger.debug("client {}/{} to mcast message ID: {}", parent.nodeId, parent.groupId, id);

        BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.PAYMENT);
        Consumer then = o -> callbackHandler.accept(o, context);
        agent.execute(id, serializedBuffer, dest);
        then.accept(null);
    }

    public void doOrderStatus() throws Exception {
        int districtID = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);

//        int y = TpccUtil.randomNumber(1, 100, gen);
//        boolean customerByName = false;
//        String customerLastName = null;
        int customerID = -1;
//        if (y <= SELECT_CUSTOMER_BY_NAME_PERCENTAGE) {
//            // 60% lookups by last name
//            customerByName = true;
//            customerLastName = TpccUtil.getLastName(new Random(System.nanoTime()));
//        } else {
//            // 40% lookups by customer ID
//            customerByName = false;
            customerID = TpccUtil.getCustomerID(gen);
//        }

//        Map<String, Object> params = new HashMap<>();
//        params.put("w_id", terminalWarehouseID);
//        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
//        params.put("d_id", districtID);
//        ObjId districtObjId = new ObjId(Row.genSId("District", terminalWarehouseID, districtID));
//        params.put("d_obj_id", districtObjId);
//        params.put("c_id", customerID);
//        params.put("c_obj_id", new ObjId(Row.genSId("Customer", terminalWarehouseID, districtID, customerID)));
//        params.put("c_w_id", terminalWarehouseID);
//        params.put("c_d_id", districtID);
//        params.put("c_by_name", customerByName);
//        params.put("c_name", customerLastName);

        ByteBuffer serializedBuffer = ByteBuffer.allocateDirect(16);  // 4 + 4 + 4 + 4
        serializedBuffer.putInt(3);  // Order-Status
        serializedBuffer.putInt(terminalWarehouseID);
        serializedBuffer.putInt(districtID);
        serializedBuffer.putInt(customerID);
        serializedBuffer.clear();

//        logger.debug("cmd {} Client: sending command {}-{}", cmd.getId(), cmd);
        if (RamcastConfig.LOG_ENABLED)
            logger.debug(
                    "client {} starting txn {} OrderStatus: w_id={} d_id={} c_id={}",
                    agent.getEndpointGroup().getAgent().getNode(),
                    msgeId+1,
                    terminalWarehouseID,
                    districtID,
                    customerID);

        ArrayList<RamcastGroup> dest = new ArrayList<>();
        dest.add(RamcastGroup.getGroup(terminalWarehouseID - 1));  // Ramcast groups' index start from 0

//        Command cmd = new Command(TpccCommandType.ORDER_STATUS, params);
//        int preferredPartitionId = clientProxy.getCache().getNode(districtObjId) != null ? clientProxy.getCache().getNode(districtObjId).getPartitionId() : -1;
//        int preferredPartitionId = -1;
//        if (isSSMRMode)
//            clientProxy.executeSSMRCommand(cmd).thenAccept(then);
//        else
//            clientProxy.executeCommand(cmd, preferredPartitionId).thenAccept(then);
//        agent.execute(++msgeId, buffer, dest);

        int id = pid + (msgeId++ * numProcesses);
//        if (txCount % 20000 == 1)
        logger.debug("client {}/{} to mcast message ID: {}", parent.nodeId, parent.groupId, id);

        BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.ORDER_STATUS);
        Consumer then = o -> callbackHandler.accept(o, context);
        agent.execute(id, serializedBuffer, dest);
        then.accept(null);
    }

    public void doDelivery() throws Exception {
        int orderCarrierID = TpccUtil.randomNumber(1, 10, gen);
//        Map<String, Object> params = new HashMap<>();
//        params.put("w_id", terminalWarehouseID);
//        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
//        Set<ObjId> districtObjIds = new HashSet<>();
//        for (int districtID = 1; districtID <= TpccConfig.configDistPerWhse; districtID++) {
//            ObjId districtObjId = new ObjId(Row.genSId("District", terminalWarehouseID, districtID));
//            districtObjId.includeDependencies = true;
//            districtObjIds.add(districtObjId);
//        }
//        params.put("d_obj_ids", districtObjIds);
//        params.put("o_carrier_id", orderCarrierID);
//        Command cmd = new Command(TpccCommandType.DELIVERY, params);
//        if (isSSMRMode)
//            clientProxy.executeSSMRCommand(cmd).thenAccept(then);
//        else
//            clientProxy.executeCommand(cmd).thenAccept(then);
//        agent.execute(++msgeId, buffer, dest);

        ByteBuffer serializedBuffer = ByteBuffer.allocateDirect(16);  // 4 + 4 + 4
        serializedBuffer.putInt(4);  // Delivery
        serializedBuffer.putInt(terminalWarehouseID);
        serializedBuffer.putInt(orderCarrierID);
        serializedBuffer.clear();

//        logger.debug("cmd {} Client: sending command {}-{}", cmd.getId(), cmd);
        if (RamcastConfig.LOG_ENABLED)
            logger.debug(
                    "client {} starting txn {} OrderStatus: w_id={} o_carrier_id={}",
                    agent.getEndpointGroup().getAgent().getNode(),
                    msgeId,
                    terminalWarehouseID,
                    orderCarrierID);

        ArrayList<RamcastGroup> dest = new ArrayList<>();
        dest.add(RamcastGroup.getGroup(terminalWarehouseID - 1));  // Ramcast groups' index start from 0

        int id = pid + (msgeId++ * numProcesses);
//        if (txCount % 20000 == 1)
        logger.debug("client {}/{} to mcast message ID: {}", parent.nodeId, parent.groupId, id);

        BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.DELIVERY);
        Consumer then = o -> callbackHandler.accept(o, context);
        agent.execute(id, serializedBuffer, dest);
        then.accept(null);
    }

    public void doStockLevel() throws Exception{
        int threshold = TpccUtil.randomNumber(10, 20, gen);
        int districtId = TpccUtil.randomNumber(1, TpccConfig.configDistPerWhse, gen);

//        Map<String, Object> params = new HashMap<>();
//        params.put("w_id", terminalWarehouseID);
//        params.put("w_obj_id", new ObjId(Row.genSId("Warehouse", terminalWarehouseID)));
//        params.put("d_id", districtId);
//        ObjId districtObjId = new ObjId(Row.genSId("District", terminalWarehouseID, districtId));
//        districtObjId.includeDependencies = true;
//        params.put("d_obj_id", districtObjId);
//        params.put("threshold", threshold);
//        Set<ObjId> stockDistrictObjIds = TpccUtil.getStockDistrictId(terminalWarehouseID);
//        stockDistrictObjIds.forEach(objId -> objId.includeDependencies = true);
//        params.put("s_d_obj_ids", stockDistrictObjIds);
//        Command cmd = new Command(TpccCommandType.STOCK_LEVEL, params);
//        if (isSSMRMode)
//            clientProxy.executeSSMRCommand(cmd).thenAccept(then);
//        else
//            clientProxy.executeCommand(cmd).thenAccept(then);
//        agent.execute(++msgeId, buffer, dest);

        ByteBuffer serializedBuffer = ByteBuffer.allocateDirect(16);  // 4 + 4 + 4
        serializedBuffer.putInt(5);  // Stock-Level
        serializedBuffer.putInt(terminalWarehouseID);
        serializedBuffer.putInt(districtId);
        serializedBuffer.putInt(threshold);
        serializedBuffer.clear();

//        logger.debug("cmd {} Client: sending command {}-{}", cmd.getId(), cmd);
        if (RamcastConfig.LOG_ENABLED)
            logger.debug(
                    "client {} starting txn {} OrderStatus: w_id={} d_id={} threshold={}",
                    agent.getEndpointGroup().getAgent().getNode(),
                    msgeId,
                    terminalWarehouseID,
                    districtId,
                    threshold);

        ArrayList<RamcastGroup> dest = new ArrayList<>();
        dest.add(RamcastGroup.getGroup(terminalWarehouseID - 1));  // Ramcast groups' index start from 0

        int id = pid + (msgeId++ * numProcesses);
//        if (txCount % 20000 == 1)
        logger.debug("client {}/{} to mcast message ID: {}", parent.nodeId, parent.groupId, id);

        BenchContext.CallbackContext context = new BenchContext.CallbackContext(this, TpccCommandType.STOCK_LEVEL);
        Consumer then = o -> callbackHandler.accept(o, context);
        agent.execute(id, serializedBuffer, dest);
        then.accept(null);
    }

    void getPermit() {
        try {
            sendPermits.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void executeTransactions(int numTransactions) throws Exception{

        boolean stopRunning = false;

        if (numTransactions != -1)
            if (RamcastConfig.LOG_ENABLED)
                logger.error("Executing " + numTransactions + " transactions...");
        else
            if (RamcastConfig.LOG_ENABLED)
                logger.error("Executing for a limited time, client's warehouseId: {}, districtId: {}", terminalWarehouseID, terminalDistrictID);

        for (int i = 0; (i < numTransactions || numTransactions == -1) && !stopRunning; i++) {
            getPermit();
            long transactionType = TpccUtil.randomNumber(1, 100, gen);
            int skippedDeliveries = 0, newOrder = 0;
            String transactionTypeName = null;

            long transactionStart = System.currentTimeMillis();
            if (transactionType <= newOrderWeight) {
                transactionTypeName = "New-Order";
                doNewOrder();
                newOrderCounter++;
                newOrder = 1;
            } else if (transactionType <= newOrderWeight + paymentWeight) {
                transactionTypeName = "Payment";
                doPayment();
            } else if (transactionType <= newOrderWeight + paymentWeight + orderStatusWeight) {
                transactionTypeName = "Order-Status";
                doOrderStatus();
            } else if (transactionType <= newOrderWeight + paymentWeight + orderStatusWeight + deliveryWeight) {
                transactionTypeName = "Delivery";
                doDelivery();
            } else if (transactionType <= newOrderWeight + paymentWeight + orderStatusWeight + deliveryWeight + stockLevelWeight) {
                transactionTypeName = "Stock-Level";
                doStockLevel();
            }
            long transactionEnd = System.currentTimeMillis();

            if (!transactionTypeName.equals("Delivery")) {
                parent.signalTerminalEndedTransaction(i, this.terminalName, transactionTypeName, transactionEnd - transactionStart, null, newOrder);
            } else {
                parent.signalTerminalEndedTransaction(i, this.terminalName, transactionTypeName, transactionEnd - transactionStart, (skippedDeliveries == 0 ? "None" : "" + skippedDeliveries + " delivery(ies) skipped."), newOrder);
            }

            if (limPerMin_Terminal > 0) {
                long elapse = transactionEnd - transactionStart;
                long timePerTx = 60000 / limPerMin_Terminal;

                if (elapse < timePerTx) {
                    try {
                        long sleepTime = timePerTx - elapse;
                        Thread.sleep((sleepTime));
                    } catch (Exception e) {
                    }
                }
            }

            txCount++;
            if (txCount % 100000 == 1)
                logger.info("sent messages: {}", txCount);

            if (stopRunningSignal) stopRunning = true;

            if (RamcastConfig.DELAY)
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }

    public void releasePermit() {
        sendPermits.release();
    }

    public void onCacheInvalidated() {
        this.goodToGo = true;
    }
}
