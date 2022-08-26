package ch.usi.dslab.lel.ramcast.heron;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class HeronMemoryBlock {
    //  |          g1     |           g2      |
    //  | n1  |  n2 |  n3 |  n1  |  n2  |  n3 |
    //  |id, s|
    //  id: request id, s: status(1:delivered, 2: executed)

    protected static final Logger logger = LoggerFactory.getLogger(HeronMemoryBlock.class);
    private long address;
    private int lkey;
    private int rkey;
    private int capacity;
    private ByteBuffer buffer;

    public HeronMemoryBlock(long address, int lkey, int capacity, ByteBuffer buffer) {
        this.address = address;
        this.lkey = lkey;
        this.capacity = capacity;
        this.buffer = buffer;
    }

    public HeronMemoryBlock(long address, int lkey, int rkey, int capacity, ByteBuffer buffer) {
        this.address = address;
        this.lkey = lkey;
        this.rkey = rkey;
        this.capacity = capacity;
        this.buffer = buffer;
    }

    HeronMessage getMessage (int group, int node) throws Exception{
        ByteBuffer buff = getCoord(group, node);
        return new HeronMessage(buff, this);
    }

    ByteBuffer getCoord (int group, int node) throws Exception{
        int startPos = group * HeronConfig.getInstance().getNodePerGroup() * HeronConfig.SIZE_COORD
                + node * HeronConfig.SIZE_COORD;
        int endPos = group * HeronConfig.getInstance().getNodePerGroup() * HeronConfig.SIZE_COORD
                + (node+1) * HeronConfig.SIZE_COORD;
        return buffer.position(startPos).limit(endPos).slice();
    }

    // groups are array of group ids to which the message is forwarded
    public boolean isFulfilled(int[] groups, int lastReqExecuted) throws Exception {
        int quroumSize = HeronConfig.getInstance().getNodePerGroup() / 2 + 1;
        int nodesFullfilled = 0;
        for (int i = 0; i < groups.length; i++) {
            for (int nodeIndex = 0; nodeIndex < HeronConfig.getInstance().getNodePerGroup(); nodeIndex++) {
                ByteBuffer buff = getCoord(groups[i], nodeIndex);
                int reqId = buff.getInt();
                int status = buff.getShort();
                if (reqId > lastReqExecuted || (reqId == lastReqExecuted && status == 2 ))
                    nodesFullfilled++;
            }
            if (nodesFullfilled < quroumSize)
                return false;
            nodesFullfilled = 0;
        }
        return true;
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
                                + ", address="
                                + address
                                + ", lkey="
                                + lkey
                                + ", capacity="
                                + capacity
                                + ", buffer="
                                + buffer
                                + "}\n");
        for (int i = 0; i < RamcastConfig.getInstance().getGroupCount(); i++) {
            for (int j = 0; j < RamcastConfig.getInstance().getNodePerGroup(); j++) {
                buffer.clear();
                HeronMessage msg =
                        new HeronMessage((buffer
                                .position(i * RamcastConfig.getInstance().getNodePerGroup() * 6
                                        + j * 6)
                                .limit(i * RamcastConfig.getInstance().getNodePerGroup() * 6
                                        + (j+1) * 6))
                                .slice(),
                                this);
                ret.append("slot=").append(i).append("/").append(j).append("::").append(msg).append("\n");
            }
        }

        return ret.toString();
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

    public int getCapacity() {
        return capacity;
    }

    public int getRkey() { return rkey; }

    public void setRkey(int rkey) {
        this.rkey = rkey;
    }
}
