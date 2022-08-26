package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastar.tpcc.rows.Warehouse;

import java.util.HashMap;

public class WarehouseTable {
    public HashMap<Integer, Warehouse> map = new HashMap<>();

    public Warehouse get(int itemId) {
        return map.get(itemId);
    }

    public void put(Warehouse warehouse) {
        map.put(warehouse.w_id, warehouse);
    }
}