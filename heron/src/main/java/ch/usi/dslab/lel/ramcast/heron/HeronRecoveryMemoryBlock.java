package ch.usi.dslab.lel.ramcast.heron;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class HeronRecoveryMemoryBlock {
    //  |          g1     |           g2      |
    //  | n1  |  n2 |  n3 |  n1  |  n2  |  n3 |
    //  |s, id|
    //  s: status (0: no signal, 1: recovery request, 2: done), id: replica id

    protected static final Logger logger = LoggerFactory.getLogger(HeronRecoveryMemoryBlock.class);
    private long address;
    private int lkey;
    private int rkey;
    private int capacity;
    private ByteBuffer buffer;

    public HeronRecoveryMemoryBlock(long address, int lkey, int capacity, ByteBuffer buffer) {
        this.address = address;
        this.lkey = lkey;
        this.capacity = capacity;
        this.buffer = buffer;
    }
    public HeronRecoveryMemoryBlock(long address, int lkey, int rkey, int capacity, ByteBuffer buffer) {
        this.address = address;
        this.lkey = lkey;
        this.rkey = rkey;
        this.capacity = capacity;
        this.buffer = buffer;
    }

    ByteBuffer getSignal (int group, int node) throws Exception{
        int startPos = group * HeronConfig.getInstance().getNodePerGroup() * HeronConfig.SIZE_COORD
                + node * HeronConfig.SIZE_COORD;
        int endPos = group * HeronConfig.getInstance().getNodePerGroup() * HeronConfig.SIZE_COORD
                + (node+1) * HeronConfig.SIZE_COORD;
        return buffer.position(startPos).limit(endPos).slice();
    }

    @Override
    public String toString() {
        return "Mem{"
                + ", address="
                + address
                + ", lkey="
                + lkey
                + ", capacity="
                + capacity
                + ", buffer="
                + buffer
                + "}";
    }

    // todo: HeronConfig null, hard coded COORD_SIZE
    public String toFullString(){
        StringBuilder ret =
                new StringBuilder(
                        "Mem{"
                                + "address="
                                + address
                                + ", lkey="
                                + lkey
                                + ", capacity="
                                + capacity
                                + ", buffer=");
        for (int i = 0; i < RamcastConfig.getInstance().getGroupCount(); i++) {
            for (int j = 0; j < RamcastConfig.getInstance().getNodePerGroup(); j++) {
                buffer.clear();
                HeronRecoveryMessage msg =
                        new HeronRecoveryMessage((buffer
                                .position(i * RamcastConfig.getInstance().getNodePerGroup() * 6
                                        + j * 6)
                                .limit(i * RamcastConfig.getInstance().getNodePerGroup() * 6
                                        + (j+1) * 6))
                                .slice(),
                                this);
                ret.append(msg).append("|");
            }
        }

        return ret.append("}").toString();
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public long getAddress() {
        return address;
    }

    public int getLkey() {
        return lkey;
    }

    public int getRkey() {return rkey;}

    public int getCapacity() {
        return capacity;
    }
}
