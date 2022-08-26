package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastar.tpcc.rows.Row;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvMr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class Tables{
    public static final Logger logger = LoggerFactory.getLogger(Tables.class);

//    public int lkey;

    public WarehouseTable warehouseTable;
    public DistrictTable districtTable;
    public ItemsTable itemsTable;
    public StockTable stockTable;
    public HistoryTable historyTable;
    // skip index 0 to handle indexes easier
    public CustomerTable[] customerTables = new CustomerTable[11];
    public OrderTable[] orderTables = new OrderTable[11];
    public NewOrderTable[] newOrderTables = new NewOrderTable[11];

//    public static transient boolean stockRead = false;
//    public static transient ByteBuffer stockReadBuffer;

    public int customerTableSize = 5000000; // bytes
    public int stockTableSize = 65000000; // bytes
    public int itemTableSize = 100000000; // bytes

    public static transient Boolean isRemoteReadComplete = false;

//    RdmaEndpoint endpoint;
//    RdmaServerEndpoint<RamcastEndpoint> serverEndpoint;

//    public Tables(RdmaEndpoint endpoint) {
//        this.endpoint = endpoint;
//        createTables();
//    }

//    public void setBufferArray() {
//        stockReadBuffer.clear();
//        stockReadBufferArray = new byte[stockReadBuffer.remaining()];
//        stockReadBuffer.get(stockReadBufferArray);
//    }

//    public void setBuffers() {
//        stockReadBuffer.clear().put(stockReadBufferArray);
//    }

    public Tables(RdmaServerEndpoint<RamcastEndpoint> serverEndpoint) {
        createTables();
        createRdmaTables(serverEndpoint);
    }

    // test tpcc data load
    public Tables() {
        createTables();
    }

    public void createTables() {
        warehouseTable = new WarehouseTable();
        districtTable = new DistrictTable();
        itemsTable = new ItemsTable();
        historyTable = new HistoryTable();
        for (int i=1; i < 11; i++) {
            orderTables[i] = new OrderTable();
            newOrderTables[i] = new NewOrderTable();
        }
    }

    public void createRdmaTables(RdmaServerEndpoint<RamcastEndpoint> serverEndpoint) {
        try {
            for (int i=1; i < 11; i++) {
                ByteBuffer buffer1 = ByteBuffer.allocateDirect(customerTableSize);
//                logger.debug("register buffer {}", buffer1);
                IbvMr mr1 = serverEndpoint.registerMemory(buffer1).execute().free().getMr();
                customerTables[i] = new CustomerTable(Row.MODEL.CUSTOMER, mr1.getAddr(), mr1.getLkey(), mr1.getLength(), buffer1);
            }

            ByteBuffer buffer2 = ByteBuffer.allocateDirect(stockTableSize);
            IbvMr mr2 = serverEndpoint.registerMemory(buffer2).execute().free().getMr();
//            int access = IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
//            IbvMr mr2 = serverEndpoint.getPd().regMr(buffer2, access).execute().free().getMr();
            stockTable = new StockTable(Row.MODEL.STOCK, mr2.getAddr(), mr2.getLkey(), mr2.getLength(), buffer2);

            StockTable.remoteReadBuffer = ByteBuffer.allocateDirect(Row.MODEL.STOCK.getRowSize());
            IbvMr mr3 = serverEndpoint.registerMemory(StockTable.remoteReadBuffer).execute().free().getMr();
            StockTable.remoteReadBuffer_lkey = mr3.getLkey();

            CustomerTable.remoteReadBuffer = ByteBuffer.allocateDirect(Row.MODEL.CUSTOMER.getRowSize());
            IbvMr mr4 = serverEndpoint.registerMemory(CustomerTable.remoteReadBuffer).execute().free().getMr();
            CustomerTable.remoteReadBuffer_lkey = mr4.getLkey();

            ByteBuffer buffer3 = ByteBuffer.allocateDirect(itemTableSize);
            IbvMr mr5 = serverEndpoint.registerMemory(buffer3).execute().free().getMr();
            itemsTable.setTempBuffer(buffer3);
            itemsTable.setAddress(mr5.getAddr());
            itemsTable.setLkey(mr5.getLkey());
            itemsTable.setCapacity(mr5.getLength());

            logger.info("successfully registered rdma tables, Customer table size {}MB, Stock table size {}MB, temp buffer (item table) size {}MB",
                    customerTableSize/1000000,
                    stockTableSize/1000000,
                    itemTableSize/1000000);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(0);
        }
    }

    public void copyFrom(Tables tables) {
        warehouseTable = tables.warehouseTable;
        districtTable = tables.districtTable;
        historyTable = tables.historyTable;
        orderTables = tables.orderTables;
        newOrderTables = tables.newOrderTables;

        itemsTable.map = tables.itemsTable.map;

//        logger.info("ItemsTable map size: {}", itemsTable.map.size());

//        stockTable = tables.stockTable;
//        customerTables = new CustomerTable[11];

        stockTable.bufferArray = tables.stockTable.bufferArray;
        stockTable.setBuffer();

        for (int i=1; i<11; i++) {
            customerTables[i].bufferArray = tables.customerTables[i].bufferArray;
            customerTables[i].setBuffer();
        }
    }
}
