package ch.usi.dslab.mojtaba.heron.tpcc;

import java.util.*;

public class TpccClient {
    private final Random gen;

    private static final int NUM_TX = 1000000;
    private static final int warehouseCount = 10;  // number of warehouses
    private static final int warehouseID = 1;  // client's main warehouse

    // Tpcc workload
    private static final int newOrderWeight = 45;
    private static final int paymentWeight = 43;
    private static final int orderStatusWeight = 4;
    private static final int deliveryWeight = 4;
    private static final int stockLevelWeight = 4;

    // enable for local-only workload
    boolean sanityCheck = false;

    double numNewOrderTx = 0;
    double numPaymentTx = 0;
    double numOrderStatusTx = 0;
    double numDeliveryTx = 0;
    double numStockLevelTx = 0;

    double multiPartitionTx = 0;
    double partitionsAccessedMultiPartitionTxs = 0;
    double partitionsAccessedAllTxs = 0;

    double numItemsAccessesNewOrder;

    int dest2NewOrder = 0;
    int dest2Payment = 0;
    int dest3 = 0;
    int dest4 = 0;
    int dest5 = 0;
    int dest6 = 0;
    int dest7 = 0;
    int dest8 = 0;
    int dest9 = 0;
    int dest10 = 0;

    public TpccClient() {
        this.gen = new Random(System.nanoTime());
    }

    public static void main(String[] args) {
        TpccClient t = new TpccClient();
        t.run();
    }

    public void run() {
        executeTransactions(NUM_TX);

        System.out.println();
        System.out.println("Ratio of NewOrder Txs: " + numNewOrderTx / NUM_TX * 100 + "%");
        System.out.println("Ratio of Payment Txs: " + numPaymentTx / NUM_TX * 100 + "%");
        System.out.println("Ratio of OrderStatus Txs: " + numOrderStatusTx / NUM_TX * 100 + "%");
        System.out.println("Ratio of Delivery Txs: " + numDeliveryTx / NUM_TX * 100 + "%");
        System.out.println("Ratio of StockLevel Txs: " + numStockLevelTx / NUM_TX * 100 + "%");
        System.out.println();

        System.out.println("Multi-partition Txs: " + multiPartitionTx);
        System.out.println("# of partitions accessed (multi-partition Txs): " + partitionsAccessedMultiPartitionTxs);
        System.out.println("# of partitions accessed (all Txs): " + partitionsAccessedAllTxs);
        System.out.println();

        System.out.println("Ratio of multi-partition Txs: " + multiPartitionTx / NUM_TX * 100 + "%");
        System.out.println("Average # of partitions accessed (in multi-partition Txs): " + partitionsAccessedMultiPartitionTxs / multiPartitionTx);
        System.out.println("Average # of partitions accessed (in all Txs): " + partitionsAccessedAllTxs / NUM_TX);
        System.out.println();

        System.out.println("Average # of items accessed in NewOrder Txs: " + numItemsAccessesNewOrder / numNewOrderTx);
        System.out.println();

        System.out.println("# of Txs that targets 2 WH: " + (dest2NewOrder+dest2Payment) + " (New Order: " + dest2NewOrder + ", Payment: " + dest2Payment + ")");
        System.out.println("# of Txs that targets 3 WH: " + dest3);
        System.out.println("# of Txs that targets 4 WH: " + dest4);
        System.out.println("# of Txs that targets 5 WH: " + dest5);
        System.out.println("# of Txs that targets 6 WH: " + dest6);
        System.out.println("# of Txs that targets 7 WH: " + dest7);
        System.out.println("# of Txs that targets 8 WH: " + dest8);
        System.out.println("# of Txs that targets 9 WH: " + dest9);
        System.out.println("# of Txs that targets 10 WH: " + dest10);
    }

    private void executeTransactions(int numTransactions) {
        if (numTransactions != -1)
            System.out.println("Creating " + numTransactions + " transactions for " + warehouseCount + " warehouses");
        else
            System.out.println("Creating Txs for a limited time for " + warehouseCount + " warehouses");

        for (int i = 0; (i < numTransactions || numTransactions == -1) ; i++) {
            String transactionTypeName;
            long transactionType = randomNumber(1, 100, gen);

            if (transactionType <= newOrderWeight) {
                transactionTypeName = "New-Order";
                doNewOrder();
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
        }
    }

    public void doNewOrder() {
        int numItems = randomNumber(5, 15, gen);
        int[] supplierWarehouseIds = new int[numItems];

        for (int i = 0; i < numItems; i++) {
            if (sanityCheck)
                supplierWarehouseIds[i] = warehouseID;
            else {
                if (randomNumber(1, 100, gen) > 1) {
                    supplierWarehouseIds[i] = warehouseID;
                } else {
                    do {
                        supplierWarehouseIds[i] = randomNumber(1, warehouseCount, gen);
                    }
                    while (supplierWarehouseIds[i] == warehouseID && warehouseCount > 1);
                }
            }
        }

        ArrayList<Integer> dest = new ArrayList<>();
        dest.add(warehouseID);  // Ramcast groups' index start from 0
        for (int warehouseId : supplierWarehouseIds) {
            if (!dest.contains(warehouseId)) {
                dest.add(warehouseId);
            }
        }

        if (dest.size() == 2)
            dest2NewOrder++;
        if (dest.size() == 3)
            dest3++;
        if (dest.size() == 4)
            dest4++;
        if (dest.size() == 5)
            dest5++;
        if (dest.size() == 6)
            dest6++;
        if (dest.size() == 7)
            dest7++;
        if (dest.size() == 8)
            dest8++;
        if (dest.size() == 9)
            dest9++;
        if (dest.size() == 10)
            dest10++;

        numItemsAccessesNewOrder += numItems;
        numNewOrderTx++;
        if (dest.size() > 1) {
            multiPartitionTx++;
            partitionsAccessedMultiPartitionTxs += dest.size();
        }
        partitionsAccessedAllTxs += dest.size();
    }

    public void doPayment() {
        int x = randomNumber(1, 100, gen);
        int customerWarehouseID;

        if (sanityCheck) {
            customerWarehouseID = warehouseID;
        } else {
            if (x <= 85) {
                customerWarehouseID = warehouseID;
            } else {
                do {
                    customerWarehouseID = randomNumber(1, this.warehouseCount, gen);
                }
                while (customerWarehouseID == warehouseID && this.warehouseCount > 1);
            }
        }

        ArrayList<Integer> dest = new ArrayList<>();
        dest.add(warehouseID);
        if (!dest.contains(customerWarehouseID)) {
            dest.add(customerWarehouseID);
        }

        if (dest.size() == 2)
            dest2Payment++;

        numPaymentTx++;
        if (dest.size() > 1) {
            multiPartitionTx++;
            partitionsAccessedMultiPartitionTxs += dest.size();
        }
        partitionsAccessedAllTxs += dest.size();
    }

    public void doOrderStatus() {
        numOrderStatusTx++;
        partitionsAccessedAllTxs += 1;
    }

    public void doDelivery() {
        numDeliveryTx++;
        partitionsAccessedAllTxs += 1;
    }

    public void doStockLevel() {
        numStockLevelTx++;
        partitionsAccessedAllTxs += 1;
    }

    public static int randomNumber(int min, int max, Random r) {
        return (int) (r.nextDouble() * (max - min + 1) + min);
    }
}

