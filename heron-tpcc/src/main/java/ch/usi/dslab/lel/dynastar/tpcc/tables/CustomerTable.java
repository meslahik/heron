package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastar.tpcc.TpccAgent;
import ch.usi.dslab.lel.dynastar.tpcc.rows.Customer;
import ch.usi.dslab.lel.dynastar.tpcc.rows.Row;
import ch.usi.dslab.lel.dynastar.tpcc.rows.Stock;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class CustomerTable {
    public static final Logger logger = LoggerFactory.getLogger(CustomerTable.class);

    public Row.MODEL model;
    public long address;
    public int lkey;
    public int capacity;
    public transient ByteBuffer buffer;
    public byte[] bufferArray;  // intends to fulfill Kryo requirement

    public static transient int remoteReadBuffer_lkey;
    public static transient ByteBuffer remoteReadBuffer;
    public static transient Customer customer = new Customer();  // reusable customer object to prevent creating new object each time reading buffer

    public CustomerTable() {

    }

    public CustomerTable(Row.MODEL model, long address, int lkey, int capacity, ByteBuffer buffer) {
        this.model = model;
        this.address = address;
        this.lkey = lkey;
        this.capacity = capacity;
        this.buffer = buffer;  // full customer table; in case of remote tables, this is empty
    }

    public void putInit(Customer customer) {
        int id = customer.c_id;

        customer.c_req_id = -1;
        buffer.clear().position(id * model.getRowSize()).limit(id * model.getRowSize() + model.getRowSize() / 2);
        customer.toBuffer(buffer);

        customer.c_req_id = 0;
        buffer.clear().position(id * model.getRowSize() + model.getRowSize() / 2).limit((id + 1) * model.getRowSize());
        customer.toBuffer(buffer);
    }

    public void put(Customer customer) {
//        int id = customer.c_id;
//        buffer.clear().position(id * model.getRowSize()).limit((id+1) * model.getRowSize());
//        customer.toBuffer(buffer);

        int id = customer.c_id;
        buffer.clear().position(id * model.getRowSize());
        int pos1 = buffer.position();
        int reqId1 = buffer.getInt();
        buffer.clear().position(id * model.getRowSize() + model.getRowSize()/2);
        int pos2 = buffer.position();
        int reqId2 = buffer.getInt();

        if (RamcastConfig.LOG_ENABLED)
            logger.trace("put customer {} in reqId: {}, current customer entries reqId1: {} (pos: {}), reqId2: {} (pos: {})",
                    customer.c_id,
                    customer.c_req_id,
                    reqId1,
                    pos1,
                    reqId2,
                    pos2);

        // correct slot for put is one with lower ts or one with equal reqId (the case to update item in same tx more than once)
        if (customer.c_req_id == reqId1)
            buffer.clear().position(id * model.getRowSize()).limit(id * model.getRowSize() + model.getRowSize() / 2);
        else if (customer.c_req_id == reqId2)
            buffer.clear().position(id * model.getRowSize() + model.getRowSize() / 2).limit((id + 1) * model.getRowSize());
        else if (reqId1 < reqId2)
            buffer.clear().position(id * model.getRowSize()).limit(id * model.getRowSize() + model.getRowSize() / 2);
        else
            buffer.clear().position(id * model.getRowSize() + model.getRowSize() / 2).limit((id + 1) * model.getRowSize());

        customer.toBuffer(buffer);
    }

    public Customer get(int id) {
//        buffer.clear().position(id * model.getRowSize()).limit((id+1) * model.getRowSize());
//        customer.fromBuffer(buffer);
//        return customer;

        buffer.clear().position(id * model.getRowSize());
        int reqId1 = buffer.getInt();
        buffer.clear().position(id * model.getRowSize() + model.getRowSize()/2);
        int reqId2 = buffer.getInt();

        // correct slot for get is one with higher ts
        if (reqId1 > reqId2)
            buffer.clear().position(id * model.getRowSize()).limit(id * model.getRowSize() + model.getRowSize() / 2);
        else
            buffer.clear().position(id * model.getRowSize() + model.getRowSize() / 2).limit((id + 1) * model.getRowSize());

        customer.fromBuffer(buffer);
        return customer;
    }

    // get method for remote Customer table
    // sends back null in case the node is lagging behind
    public static Customer getRemoteCustomer(int reqId) {
//        remoteReadBuffer.clear();
//        customer.fromBuffer(remoteReadBuffer);
//        return customer;

        boolean result = setBufferForRemoteGet(reqId);
        if (result) {
            customer.fromBuffer(remoteReadBuffer);
            return customer;
        } else
            return null;
    }

    // in case of return false, recovery is needed, the required value is gone
    static boolean setBufferForRemoteGet(int reqId) {
        int stockRowSize = Row.MODEL.STOCK.getRowSize();
        remoteReadBuffer.clear();
//        logger.error("remoteReadBuffer reqId1: {}", remoteReadBuffer.position());
        int reqId1 = remoteReadBuffer.getInt();
        remoteReadBuffer.clear().position(stockRowSize/2);
//        logger.error("remoteReadBuffer reqId1: {}", remoteReadBuffer.position());
        int reqId2 = remoteReadBuffer.getInt();

        int slot;
        if (reqId1 < reqId && reqId2 < reqId && reqId1 < reqId2)
            slot = 1;
        else if (reqId1 < reqId && reqId2 < reqId && reqId2 < reqId1)
            slot = 2;
        else if (reqId1 < reqId)
            slot = 1;
        else if (reqId2 < reqId)
            slot = 2;
        else {
            // the node is lagging behind, recovery needed
            if (RamcastConfig.LOG_ENABLED)
                logger.error("node lag behind, reqId: {}, remote stock entires: reqId1: {}, reqId2: {}", reqId, reqId1, reqId2);
            return false;
        }

        if (slot == 1)
            remoteReadBuffer.clear();
        else
            remoteReadBuffer.clear().position(stockRowSize/2);
        return true;
    }

    public void setBufferArray() {
        buffer.clear();
        bufferArray = new byte[buffer.remaining()];
        buffer.get(bufferArray);
    }

    public void setBuffer() {
        buffer.clear().put(bufferArray);
    }

    public long getAddress() {
        return address;
    }

    public int getLkey() {
        return lkey;
    }

    public int getCapacity() {
        return capacity;
    }

    @Override
    public String toString() {
        return "CustomerTable{" +
                "address=" + address +
                ", lkey=" + lkey +
                ", capacity=" + capacity +
                '}';
    }
}
