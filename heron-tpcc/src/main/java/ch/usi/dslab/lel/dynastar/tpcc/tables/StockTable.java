package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastar.tpcc.rows.Row;
import ch.usi.dslab.lel.dynastar.tpcc.rows.Stock;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class StockTable {
    public static final Logger logger = LoggerFactory.getLogger(StockTable.class);

    public Row.MODEL model;
    public long address;
    public int lkey;
    public int capacity;
    public transient ByteBuffer buffer; // full stock table; in case of remote tables, this is empty
    public byte[] bufferArray;  // intends to fulfill Kryo requirement

    public static transient int remoteReadBuffer_lkey;
    public static transient ByteBuffer remoteReadBuffer;
    public static transient Stock stock = new Stock();  // reusable stock object to prevent creating new object each time reading buffer

    public StockTable() {

    }

    public StockTable(Row.MODEL model, long address, int lkey, int capacity, ByteBuffer buffer) {
        this.model = model;
        this.address = address;
        this.lkey = lkey;
        this.capacity = capacity;
        this.buffer = buffer;
    }

    public void putInit(Stock stock) {
        int id = stock.s_i_id;

        stock.s_req_id = -1;
        buffer.clear().position(id * model.getRowSize()).limit(id * model.getRowSize() + model.getRowSize() / 2);
        stock.toBuffer(buffer);
//        if (stock.s_i_id == 5 && !printed)
//            logger.error("stock item id 5: {}", stock);

        stock.s_req_id = 0;
        buffer.clear().position(id * model.getRowSize() + model.getRowSize() / 2).limit((id + 1) * model.getRowSize());
        stock.toBuffer(buffer);
//        if (stock.s_i_id == 5 && !printed) {
//            logger.error("stock item id 5: {}", stock);
//            printed = true;
//        }
    }

    public void put(Stock stock) {
//        int id = stock.s_i_id;
//        buffer.clear().position(id * model.getRowSize()).limit((id+1) * model.getRowSize());
//        stock.toBuffer(buffer);

        int id = stock.s_i_id;
        buffer.clear().position(id * model.getRowSize());
        int pos1 = buffer.position();
        int reqId1 = buffer.getInt();
        buffer.clear().position(id * model.getRowSize() + model.getRowSize()/2);
        int pos2 = buffer.position();
        int reqId2 = buffer.getInt();

        if (RamcastConfig.LOG_ENABLED)
            logger.debug("put stock {} in reqId: {}, current stock entries reqId1: {} at position {}, reqId2: {} at position {}", stock.s_i_id, stock.s_req_id, reqId1, pos1, reqId2, pos2);

//        if (stock.req_id == reqId1 || stock.req_id == reqId2)
//            logger.info("error in put reqId: {}, reqId1: {}, reqId2: {}", stock.req_id, reqId1, reqId2);

        // correct slot for put is one with lower ts or one with equal reqId (the case to update item in same tx more than once)
        if (stock.s_req_id == reqId1)
            buffer.clear().position(id * model.getRowSize()).limit(id * model.getRowSize() + model.getRowSize() / 2);
        else if (stock.s_req_id == reqId2)
            buffer.clear().position(id * model.getRowSize() + model.getRowSize() / 2).limit((id + 1) * model.getRowSize());
        else if (reqId1 < reqId2)
            buffer.clear().position(id * model.getRowSize()).limit(id * model.getRowSize() + model.getRowSize() / 2);
        else
            buffer.clear().position(id * model.getRowSize() + model.getRowSize() / 2).limit((id + 1) * model.getRowSize());

        stock.toBuffer(buffer);
    }

    // get method for local Stock table
    public Stock get(int id) {
        buffer.clear().position(id * model.getRowSize());
        int reqId1 = buffer.getInt();
        buffer.clear().position(id * model.getRowSize() + model.getRowSize()/2);
        int reqId2 = buffer.getInt();

        // correct slot for get is one with higher ts
        if (reqId1 > reqId2)
            buffer.clear().position(id * model.getRowSize()).limit(id * model.getRowSize() + model.getRowSize() / 2);
        else
            buffer.clear().position(id * model.getRowSize() + model.getRowSize() / 2).limit((id + 1) * model.getRowSize());

        stock.fromBuffer(buffer);
        return stock;
    }

    public ByteBuffer getByteBufferPair(int id) {
        buffer.clear().position(id * model.getRowSize()).limit((id + 1) * model.getRowSize());
        return buffer.slice();
    }

    // get method for remote Stock table
    // sends back null in case the node is lagging behind
    public static Stock getRemoteStock(int reqId) {
        boolean result = setBufferForRemoteGet(reqId);
        if (result) {
            stock.fromBuffer(remoteReadBuffer);
            return stock;
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
        logger.debug("buffer: " + buffer + ", bufferArray " + bufferArray);
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
        return "StockTable{" +
                "address=" + address +
                ", lkey=" + lkey +
                ", capacity=" + capacity +
                '}';
    }
}
