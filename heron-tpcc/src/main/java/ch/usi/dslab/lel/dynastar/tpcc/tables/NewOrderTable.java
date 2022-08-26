package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastar.tpcc.rows.NewOrder;

import java.util.HashMap;

public class NewOrderTable {
    public HashMap<Integer, NewOrder> map = new HashMap<>();

    public NewOrder get(int itemId) {
        return map.get(itemId);
    }

    public void put(NewOrder newOrder) {
        map.put(newOrder.no_o_id, newOrder);
    }

    public void remove(NewOrder newOrder) {
        map.remove(newOrder.no_o_id);
    }
}
