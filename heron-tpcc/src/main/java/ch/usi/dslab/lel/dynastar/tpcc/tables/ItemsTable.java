package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastar.tpcc.rows.Item;
import ch.usi.dslab.lel.dynastar.tpcc.rows.Row;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class ItemsTable {
    public static final Logger logger = LoggerFactory.getLogger(ItemsTable.class);

    public HashMap<Integer, Item> map = new HashMap<>();

    public Item get(int itemId) {
        return map.get(itemId);
    }

    public void put(Item item) {
        map.put(item.i_id, item);
    }


    // temporary buffer for state transfer of non-serialized data
    public transient ByteBuffer tempBuffer;
    public long address;
    public int lkey;
    public int capacity;
    public Row.MODEL model;

    public ItemsTable() {

    }

    public ItemsTable(Row.MODEL model, long address, int lkey, int capacity) {
        this.model = model;
        this.address = address;
        this.lkey = lkey;
        this.capacity = capacity;
    }

    public void eraseBuffer() {
        // this method is expensive; instead going to zero just last byte of each row
//        tempBuffer.position(0).put(zeroArray);

        int tempBufferRealSize = 10000000;  // real data size is less than 10MB
        int counter = 0;
        for (int i = 1; counter < tempBufferRealSize; i++) {
            counter = i*32768 - 4;
            tempBuffer.position(counter).putInt(0);
        }
    }

    public ByteBuffer getByte(int itemId) {
        Item item = map.get(itemId);
//        logger.info("item id: {}, item {}", itemId, item);
        ByteBuffer buff = item.toBuffer();
        buff.clear();
        return buff;
    }

    public void setByte(ByteBuffer buffer) {
        buffer.clear();
        int itemId = buffer.getInt();
        map.get(itemId).fromBuffer(buffer);
    }

    public void setTempBuffer(ByteBuffer buffer) {
        this.tempBuffer = buffer;
    }

    public ByteBuffer getTempBuffer() {
        return tempBuffer;
    }

    public void setAddress(long address) {
        this.address = address;
    }

    public long getAddress() {
        return address;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setLkey(int lkey) {
        this.lkey = lkey;
    }

    public int getLkey() {
        return lkey;
    }
}
