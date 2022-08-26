package ch.usi.dslab.lel.ramcast.heron;

import java.io.IOException;

public class HeronConfig {
    public static final int SIZE_COORD = 6;

    private static HeronConfig instance;

    private int payloadSize = 32;
    private int groupCount;
    private int nodePerGroup = 1;

    public HeronConfig (int groupCount, int nodePerGroup, int payloadSize) {
        this.groupCount = groupCount;
        this.nodePerGroup = nodePerGroup;
        this.payloadSize = payloadSize;
    }

    public static HeronConfig getInstance() throws IOException{
        if (instance == null) {
            throw new IOException("HeronConfig is null");
        }
        return instance;
    }

    public void setPayloadSize(int payloadSize) {
        this.payloadSize = payloadSize;
    }

    public void setGroupCount(int groupCount) {
        this.groupCount = groupCount;
    }

    public void setNodePerGroup(int nodePerGroup) {
        this.nodePerGroup = nodePerGroup;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public int getGroupCount() {
        return groupCount;
    }

    public int getNodePerGroup() {
        return nodePerGroup;
    }
}
