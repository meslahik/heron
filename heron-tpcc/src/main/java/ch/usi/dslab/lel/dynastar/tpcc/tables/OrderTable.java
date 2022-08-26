package ch.usi.dslab.lel.dynastar.tpcc.tables;

import ch.usi.dslab.lel.dynastar.tpcc.rows.Order;
import ch.usi.dslab.lel.dynastar.tpcc.rows.OrderLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OrderTable {
    public static final Logger logger = LoggerFactory.getLogger(OrderTable.class);

    public HashMap<Integer, Order> map = new HashMap<>();
    public HashMap<Integer, List<OrderLine>> orderLineMap = new HashMap<>();

    public Order get(int itemId) {
        return map.get(itemId);
    }

    public void put(Order order) {
        map.put(order.o_id, order);
    }

    public void addOrderLine(OrderLine orderLine) {
        List<OrderLine> list = orderLineMap.get(orderLine.ol_o_id);
        if (list == null) {
            List<OrderLine> newList = new ArrayList<>();
            newList.add(orderLine);
            orderLineMap.put(orderLine.ol_o_id, newList);
        } else
            list.add(orderLine);
    }

    public List<OrderLine> getOrderLines(Order order) {
//        logger.debug("order {}, is orderline map empty? {}", order.o_id, orderLineMap);
        return orderLineMap.get(order.o_id);
    }
}
