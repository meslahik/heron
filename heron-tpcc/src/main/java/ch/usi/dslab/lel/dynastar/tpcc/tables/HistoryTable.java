package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastar.tpcc.rows.History;

import java.util.HashMap;

public class HistoryTable {
    public int index = 0;
    public HashMap<Integer, History> map = new HashMap<>();

    public History get(int historyId) {
        return map.get(historyId);
    }

    public void put(History history) {
        map.put(++index, history);
    }
}
