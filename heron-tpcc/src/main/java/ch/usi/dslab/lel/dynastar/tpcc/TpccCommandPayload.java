package ch.usi.dslab.lel.dynastar.tpcc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TpccCommandPayload {
    public Map<String, Object> attributes = new ConcurrentHashMap<>();

    public TpccCommandPayload() {

    }

    public TpccCommandPayload(Map attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        StringBuffer str = new StringBuffer("[");
        this.attributes.keySet().stream().forEach(key -> str.append(key + ":" + this.attributes.get(key) + ", "));
        str.append("]");
        return str.toString();
    }
}
