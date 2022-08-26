package ch.usi.dslab.lel.ramcast.heron;

import java.nio.ByteBuffer;

public class HeronMessage {
    int reqId;
    short status;
    HeronMemoryBlock block;

    public HeronMessage(int reqId, short status) {
        this.reqId = reqId;
        this.status = status;
    }

    public HeronMessage(ByteBuffer buffer, HeronMemoryBlock block) {
        buffer.clear();
        this.reqId = buffer.getInt();
        this.status = buffer.getShort();
        this.block = block;
    }

    @Override
    public String toString() {
        return reqId + "|" + status;
    }
}
