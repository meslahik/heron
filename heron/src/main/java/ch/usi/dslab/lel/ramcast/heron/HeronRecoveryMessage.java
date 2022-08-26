package ch.usi.dslab.lel.ramcast.heron;

import java.nio.ByteBuffer;

public class HeronRecoveryMessage {
    short status;
    int replicaId;
    HeronRecoveryMemoryBlock block;

    public HeronRecoveryMessage(short status, int replicaId) {
        this.status = status;
        this.replicaId = replicaId;
    }

    public HeronRecoveryMessage(ByteBuffer buffer, HeronRecoveryMemoryBlock block) {
        buffer.clear();
        this.status = buffer.getShort();
        this.replicaId = buffer.getInt();
        this.block = block;
    }

    @Override
    public String toString() {
        return status + "," + replicaId;
    }
}
