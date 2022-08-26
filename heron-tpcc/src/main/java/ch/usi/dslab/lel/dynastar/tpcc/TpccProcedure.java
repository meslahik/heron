package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.lel.dynastar.tpcc.command.Command;
import ch.usi.dslab.lel.dynastar.tpcc.rows.*;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccUtil;
//import ch.usi.dslab.lel.dynastarv2.AppProcedure;
//import ch.usi.dslab.lel.dynastarv2.StateMachine;
//import ch.usi.dslab.lel.dynastarv2.command.Command;
//import ch.usi.dslab.lel.dynastarv2.messages.CommandType;

import ch.usi.dslab.lel.dynastar.tpcc.objects.ObjId;
import ch.usi.dslab.lel.dynastar.tpcc.objects.PRObject;

//import com.google.common.base.Splitter;
import org.slf4j.Logger;

import java.util.*;

public class TpccProcedure { // implements AppProcedure {
    //    public TpccProcedure(PRObjectGraph graph, Map<String, Set<ObjId>> secondaryIndex){
//        setCache(graph, secondaryIndex);
//    }

//    private static Splitter splitterCollon = Splitter.on(':');
//    private static Splitter splitterEqual = Splitter.on('=');
    //    Queue<ObjId> mostRecentOrderdItem = EvictingQueue.create(15 * TpccConfig.configDistPerWhse);
//    Queue<ObjId> mostRecentOrderLine = EvictingQueue.create(15 * TpccConfig.configDistPerWhse);
    private Map<String, Set<ObjId>> secondaryIndex;
    private String role;
    private Logger logger;
    private int partitionId;

    static String safeKey(String model, String... parts) {
        StringBuilder ret = new StringBuilder(model);
        for (int i = 0; i < parts.length; i++) {
            ret.append(":");
            ret.append(parts[i]);
            ret.append("=");
            ret.append(parts[++i]);
        }
        return ret.toString();
    }

//    public boolean isTransient(PRObjectNode node) {
//        return isTransient(node.getId());
//    }

    public void init(String role, Map<String, Set<ObjId>> secondaryIndex, Logger logger, int partitionId) {
        this.role = role;
        this.secondaryIndex = secondaryIndex;
        this.logger = logger;
        this.partitionId = partitionId;
    }

//    public void storeOrderLine(ObjId id) {
//        mostRecentOrderLine.add(id);
//    }

//    public boolean shouldQuery(PRObjectMap cache, Command command, boolean forceQuery) {
//        if (command.getReservedObjects() != null) return true;
//        if (command.getItem(0) instanceof CommandType && command.getItem(0) == CommandType.CREATE) return true;
//        if (command.getItem(0) instanceof TpccCommandType) {
//            TpccCommandType cmdType = (TpccCommandType) command.getItem(0);
//            Map<String, Object> params = (Map<String, Object>) command.getItem(1);
//            switch (cmdType) {
//                case NEW_ORDER:
//                case PAYMENT:
//                case ORDER_STATUS: {
//                    ObjId terminalWarehouseID = (ObjId) params.get("w_obj_id");
//                    ObjId districtId = (ObjId) params.get("d_obj_id");
//                    if (graph.getNode(districtId) != null) return false;
//                    break;
//                }
//                case STOCK_LEVEL: {
//                    ObjId terminalWarehouseID = (ObjId) params.get("w_obj_id");
//                    ObjId districtId = (ObjId) params.get("d_obj_id");
//                    if (graph.getNode(districtId) != null) return false;
//                    if (graph.getNode(terminalWarehouseID) != null) return false;
//                    Set<ObjId> districtObjIds = (HashSet) params.get("s_d_obj_ids");
//                    for (ObjId objId : districtObjIds) {
//                        if (graph.getNode(objId) != null) return false;
//                    }
//                    break;
//                }
//                case DELIVERY: {
//                    ObjId terminalWarehouseID = (ObjId) params.get("w_obj_id");
//                    if (graph.getNode(terminalWarehouseID) != null) return false;
//                    Set<ObjId> districtObjIds = (HashSet) params.get("d_obj_ids");
//                    for (ObjId objId : districtObjIds) {
//                        if (graph.getNode(objId) != null) return false;
//                    }
//                    break;
//                }
//                default: {
//                    return true;
//                }
//            }
//        }
//        return true;
//    }

    public Map<Integer, Set<ObjId>> refineObjectId(Command command, Map<Integer, Set<ObjId>> srcPartMap, boolean includeAllPartition) {
        command.rewind();
        TpccCommandType cmdType = (TpccCommandType) command.getNext();
        Map<String, Object> params = (Map<String, Object>) command.getItem(1);
        switch (cmdType) {
            case ORDER_STATUS:
            case PAYMENT: {

                for (Map.Entry entry : srcPartMap.entrySet()) {
                    if (!includeAllPartition && (int) entry.getKey() != partitionId) continue;
                    boolean customerByName = (boolean) params.get("c_by_name");
                    if (!customerByName) {
                        break;
                    }
                    logger.debug("Need to refine customerid for Payment. Before: " + srcPartMap);
                    int customerWarehouseID = (int) params.get("c_w_id");
                    int customerDistrictID = (int) params.get("c_d_id");

                    String customerLastName = (String) params.get("c_name");
                    ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)) != null ?
                            secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)).iterator().next() : null;


                    Set<ObjId> objIds = (Set<ObjId>) entry.getValue();
                    for (ObjId objId : objIds) {
                        if (objId.sId.contains("Customer")) {

                            if (customerObjId == null) {
                                logger.debug("cmd {} can't find customer {}", command.getId(), Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName));
                                command.setInvalid(true);
                                return srcPartMap;
                            }
                            objId.setSId(customerObjId.getSId());
                        }
                    }
                }
            }
        }
        logger.debug("SrcMap after refining: " + srcPartMap);
        return srcPartMap;

    }

//    public void fillDependencies(PRObjectNode node, PRObjectGraph objectGraph, Command command) {
//        if (node.getDependencyIds() == null) return;
//        Set<PRObjectNode> dependencies = new HashSet<>();
//        node.getDependencyIds().stream().filter(objId -> {
//            return objId.sId != null && (objId.sId.split(":")[0].equals("Stock") || objId.sId.split(":")[0].equals("OrderLine") || objId.sId.split(":")[0].equals("Order") || objId.sId.split(":")[0].equals("Customer")) && objectGraph.getNode(objId) != null;
//        }).forEach(objId1 -> dependencies.add(objectGraph.getNode(objId1)));
//        node.setDependencies(dependencies);
//    }



    public Set<ObjId> extractObjectId(Command command) {

        command.rewind();
        if (command.getInvolvedObjects() != null && command.getInvolvedObjects().size() > 0) {
            return command.getInvolvedObjects();
        }
        Set<ObjId> ret = new HashSet<>();
        if (this.role.equals("ORACLE")) {
//            if (command.getItem(0) instanceof TpccCommandType) {
//                TpccCommandType cmdType = (TpccCommandType) command.getNext();
//                Map<String, Object> params = (Map<String, Object>) command.getItem(1);
//                switch (cmdType) {
//                    case NEW_ORDER: {
//                        int terminalWarehouseID = (int) params.get("w_id");
//                        int districtID = (int) params.get("d_id");
//                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
//                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
//                        ret.add(districtObjId);
//                        ret.add(warehouseObjId);
//                        Set<ObjId> stockDistrictObjIds = TpccUtil.getStockDistrictId(terminalWarehouseID);
//                        ret.addAll(stockDistrictObjIds);
//                        break;
//                    }
//                    case PAYMENT: {
//                        int terminalWarehouseID = (int) params.get("w_id");
//                        int districtID = (int) params.get("d_id");
//                        ObjId districtObjId = secondaryIndex.get(Row.genSId("District", terminalWarehouseID, districtID)).iterator().next();
//                        ObjId warehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", terminalWarehouseID)).iterator().next();
//                        ret.add(districtObjId);
//                        ret.add(warehouseObjId);
//                        int c_w_id = (int) params.get("c_w_id");
//                        int c_d_id = (int) params.get("c_d_id");
//                        ObjId customerDistrictObjId = secondaryIndex.get(Row.genSId("District", c_w_id, c_d_id)).iterator().next();
//                        ObjId customerWarehouseObjId = secondaryIndex.get(Row.genSId("Warehouse", c_w_id)).iterator().next();
//                        ret.add(customerDistrictObjId);
//                        ret.add(customerWarehouseObjId);
//                        break;
//                    }
//                    case ORDER_STATUS: {
//                        ObjId districtObjId = (ObjId) params.get("d_obj_id");
//                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
//                        ret.add(districtObjId);
//                        ret.add(warehouseObjId);
//                        break;
//                    }
//                    case DELIVERY: {
//                        int terminalWarehouseID = (int) params.get("w_id");
//                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
//                        ret.add(warehouseObjId);
//                        Set<ObjId> districtObjIds = (HashSet) params.get("d_obj_ids");
//                        ret.addAll(districtObjIds);
//                        break;
//                    }
//                    case STOCK_LEVEL: {
//                        ObjId districtObjId = (ObjId) params.get("d_obj_id");
//                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
//                        ret.add(districtObjId);
//                        ret.add(warehouseObjId);
//                        Set<ObjId> districtObjIds = (HashSet) params.get("s_d_obj_ids");
//                        ret.addAll(districtObjIds);
//                        break;
//                    }
//
//                }
//            }
        } else if (this.role.equals("CLIENT")) {
            if (command.getItem(0) instanceof TpccCommandType) {
                TpccCommandType cmdType = (TpccCommandType) command.getNext();
                Map<String, Object> params = (Map<String, Object>) command.getItem(1);
                switch (cmdType) {
                    case NEW_ORDER: {
                        ObjId districtObjId = (ObjId) params.get("d_obj_id");
                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
                        ObjId customerObjId = (ObjId) params.get("c_obj_id");
                        ret.add(districtObjId);
                        ret.add(warehouseObjId);
                        ret.add(customerObjId);
//                        int[] supplierWarehouseIds = (int[]) params.get("supplierWarehouseIds");
//                        for (int id : supplierWarehouseIds) {
//                            ret.addAll(TpccUtil.getStockDistrictId(id));
//                        }
                        Set<ObjId> supplierWarehouseObjIds = (Set<ObjId>) params.get("supplierWarehouseObjIds");
                        Set<ObjId> stockDistrictObjIds = TpccUtil.getStockDistrictId((int) params.get("w_id"));
                        Set<ObjId> stockIds = (HashSet) params.get("stockIds");
                        ret.addAll(stockDistrictObjIds);
                        if (supplierWarehouseObjIds.size() > 0) ret.addAll(supplierWarehouseObjIds);
                        ret.addAll(stockIds);
                        break;
                    }
                    case PAYMENT: {
                        ObjId districtObjId = (ObjId) params.get("d_obj_id");
                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
                        ObjId customerObjId = (ObjId) params.get("c_obj_id");
                        if (customerObjId != null) ret.add(customerObjId);
                        ret.add(districtObjId);
                        ret.add(warehouseObjId);
                        ObjId customerDistrictObjId = (ObjId) params.get("c_d_obj_id");
                        ObjId customerWarehouseObjId = (ObjId) params.get("c_w_obj_id");
                        ret.add(customerDistrictObjId);
                        ret.add(customerWarehouseObjId);
                        break;
                    }
                    case ORDER_STATUS: {
                        ObjId districtObjId = (ObjId) params.get("d_obj_id");
                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
                        ObjId customerObjId = (ObjId) params.get("c_obj_id");
                        if (customerObjId != null) ret.add(customerObjId);
                        ret.add(districtObjId);
                        ret.add(warehouseObjId);
                        break;
                    }
                    case DELIVERY: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
                        ret.add(warehouseObjId);
                        Set<ObjId> districtObjIds = (HashSet) params.get("d_obj_ids");
                        ret.addAll(districtObjIds);
                        break;
                    }
                    case STOCK_LEVEL: {
                        ObjId districtObjId = (ObjId) params.get("d_obj_id");
                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
                        ret.add(districtObjId);
                        ret.add(warehouseObjId);
                        Set<ObjId> districtObjIds = (HashSet) params.get("s_d_obj_ids");
                        ret.addAll(districtObjIds);
                        break;
                    }
                    case READ: {
                        ObjId objId = secondaryIndex.get(params.get("key")).iterator().next();
                        ret.add(objId);
                        break;
                    }
                }
            } else ret = _extractObjId(command);
        } else if (this.role.equals("SERVER") && command.getItem(0) instanceof TpccCommandType) {
//            System.out.println("Extracting objects of command "+command);
            try {
                TpccCommandType cmdType = (TpccCommandType) command.getNext();
                Map<String, Object> params = (Map<String, Object>) command.getItem(1);

                switch (cmdType) {
                    case READ: {
                        ObjId key = secondaryIndex.get(params.get("key")).iterator().next();
                        ret.add(key);
                        break;
                    }
                    case NEW_ORDER: {
                        ObjId terminalWarehouseID = (ObjId) params.get("w_obj_id");
                        ObjId districtID = (ObjId) params.get("d_obj_id");
                        ObjId customerID = (ObjId) params.get("c_obj_id");
                        Set<ObjId> itemObjIds = (HashSet) params.get("itemObjIds");
                        Set<ObjId> supplierWarehouseObjIds = (HashSet) params.get("supplierWarehouseObjIds");
                        Set<ObjId> stockObjIds = (Set<ObjId>) params.get("stockIds");
                        stockObjIds.forEach(objId -> objId.includeDependencies = true);
                        int[] orderQuantities = (int[]) params.get("orderQuantities");

                        ret.add(terminalWarehouseID);
                        ret.add(districtID);
                        ret.add(customerID);
                        ret.addAll(itemObjIds);
//                        ret.addAll(supplierWarehouseObjIds);
                        ret.addAll(stockObjIds);
                        break;
                    }
                    case PAYMENT: {
                        ObjId districtObjId = (ObjId) params.get("d_obj_id");
                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");

                        ret.add(districtObjId);
                        ret.add(warehouseObjId);
                        ObjId customerDistrictObjId = (ObjId) params.get("c_d_obj_id");
                        ObjId customerWarehouseObjId = (ObjId) params.get("c_w_obj_id");
//                        ret.add(customerDistrictObjId);
//                        ret.add(customerWarehouseObjId);


                        boolean customerByName = (boolean) params.get("c_by_name");
                        if (!customerByName) {
                            ObjId customerObjId = (ObjId) params.get("c_obj_id");
                            ret.add(customerObjId);
                        } else {
                            int customerWarehouseID = (int) params.get("c_w_id");
                            int customerDistrictID = (int) params.get("c_d_id");

                            String customerLastName = (String) params.get("c_name");
                            ObjId customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)) != null ?
                                    secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)).iterator().next() : null;
                            if (customerObjId != null) {
                                params.put("c_objId", customerObjId);
                                ret.add(customerObjId);
                            }

                        }
                        break;
                    }
                    case ORDER_STATUS: {

                        ObjId districtObjId = (ObjId) params.get("d_obj_id");
                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");

                        ret.add(districtObjId);
                        ret.add(warehouseObjId);

                        int terminalWarehouseID = (int) params.get("w_id");
                        int districtID = (int) params.get("d_id");

                        boolean customerByName = (boolean) params.get("c_by_name");

                        ObjId customerObjId;
                        if (!customerByName) {
                            customerObjId = (ObjId) params.get("c_obj_id");
                            ret.add(customerObjId);
                        } else {
                            int customerWarehouseID = (int) params.get("c_w_id");
                            int customerDistrictID = (int) params.get("c_d_id");

                            String customerLastName = (String) params.get("c_name");
                            customerObjId = secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)) != null ?
                                    secondaryIndex.get(Row.genSId("Customer", customerWarehouseID, customerDistrictID, customerLastName)).iterator().next() : null;
                            if (customerObjId != null) {
                                params.put("c_objId", customerObjId);
                                ret.add(customerObjId);
                            }

                        }

                        String ordersKey = Row.genSId("Order", terminalWarehouseID, districtID, Integer.parseInt(customerObjId.getSId().split(":")[3].split("=")[1]));

//                        System.out.println("customerObjId: " + customerObjId + " - " + customerObjId.getSId() + " - - " + customerObjId.getSId().split(":")[3].split("=")[1]);
//                        System.out.println("ordersKey: " + ordersKey);
                        if (secondaryIndex.get(ordersKey) != null) {
                            List<ObjId> orders = new ArrayList<>();
                            synchronized (secondaryIndex.get(ordersKey)) {
                                orders.addAll(secondaryIndex.get(ordersKey));
                            }
                            orders.addAll(secondaryIndex.get(ordersKey));
                            Collections.sort(orders, new OrderIdComparator());
//
//                            System.out.println("found orders" + orders);
                            ObjId orderObjId = orders.get(0);
//                            ObjId orderObjId = new ObjId(ordersKey);
//                            ObjId orderObjId = secondaryIndex.get(ordersKey).iterator().next();
                            params.put("orderObjId", orderObjId);
                            ret.add(orderObjId);
                            String orderLineKey = Row.genSId("OrderLine", terminalWarehouseID, districtID, orderObjId.getSId().split(":")[4].split("=")[1]);
//                            System.out.println("finding ol keys " + orderLineKey);
                            if (secondaryIndex.get(orderLineKey) != null) {
                                Set<ObjId> orderLines = secondaryIndex.get(orderLineKey);
//                                System.out.println("found orderLiness" + orderLines);
                                params.put("orderLineObjIds", orderLines);
                                ret.addAll(orderLines);
                            }
                        }
                        break;
                    }
                    case DELIVERY: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
                        ret.add(warehouseObjId);
                        Set<ObjId> districtObjIds = (HashSet) params.get("d_obj_ids");
                        ret.addAll(districtObjIds);

                        break;
                    }
                    case STOCK_LEVEL: {
                        int terminalWarehouseID = (int) params.get("w_id");
                        ObjId warehouseObjId = (ObjId) params.get("w_obj_id");
                        int districtID = (int) params.get("d_id");
                        ObjId districtObjId = (ObjId) params.get("d_obj_id");
                        districtObjId.includeDependencies = true;
                        ret.add(warehouseObjId);
                        ret.add(districtObjId);
                        Set<ObjId> stockDistrictObjIds = TpccUtil.getStockDistrictId(terminalWarehouseID);
                        stockDistrictObjIds.forEach(objId -> objId.includeDependencies = true);
                        ret.addAll(stockDistrictObjIds);
                        break;
                    }
                }
            } catch (Exception e) {
                System.out.println("P_" + this.partitionId + " ERROR: " + e.getMessage() + " command " + command.toFullString());
                e.printStackTrace();
//                throw e;
            }
        } else {
            ret = _extractObjId(command);
        }
        return ret;
    }

    private Set<ObjId> _extractObjId(Command command) {
        Set<ObjId> ret = new HashSet<>();
        if (command.getInvolvedObjects() != null && command.getInvolvedObjects().size() > 0)
            return command.getInvolvedObjects();
//        if (StateMachine.isCreateWithGather(command)) {
//            command.setInvolvedObjects((Set<ObjId>) command.getItem(2));
//            return (Set<ObjId>) command.getItem(2);
//        }
        command.rewind();
        while (command.hasNext()) {
            Object test = command.getNext();
            if (test instanceof ObjId) {
                ret.add((ObjId) test);
            } else if (test instanceof List) {
                for (Object objId : (List) test) {
                    if (objId instanceof ObjId) {
                        ret.add((ObjId) objId);
                    }
                }
            } else if (test instanceof Set) {
                for (Object objId : (Set) test) {
                    if (objId instanceof ObjId) {
                        ret.add((ObjId) objId);
                    }
                }
            } else if (test instanceof Map) {
                for (Object objId : ((Map) test).keySet()) {
                    if (objId instanceof ObjId) {
                        ret.add((ObjId) objId);
                    }
                }
                for (Object objId : ((Map) test).values()) {
                    if (objId instanceof ObjId) {
                        ret.add((ObjId) objId);
                    } else if (objId instanceof Set) {
                        for (Object o : (Set) objId) {
                            if (o instanceof ObjId) {
                                ret.add((ObjId) o);
                            }
                        }
                    }
                }
            } else if (test instanceof Command) {
                ((Command) test).rewind();
                ret.addAll(extractObjectId((Command) test));
            }
        }
//        Set<ObjId> tmp = extractRelatedObjects(command);
//        if (tmp != null) ret.addAll(tmp);
        return ret;
    }

    public ObjId genParentObjectId(ObjId objId) {
        String[] idParts = objId.getSId().split(":");
        StringBuilder id = new StringBuilder("District:d_w_id=");
        switch (idParts[0]) {
            case "Customer": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "History": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "Stock": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(TpccUtil.mapStockToDistrict(objId.getSId()));
                break;
            }
            case "Order": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "NewOrder": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }
            case "OrderLine": {
                id.append(idParts[1].split("=")[1]);
                id.append(":d_id=");
                id.append(idParts[2].split("=")[1]);
                break;
            }

        }
        return new ObjId(id.toString());
    }


//    public PRObjectNode getParentObject(PRObjectNode node) {
//        String[] idParts = node.getId().getSId().split(":");
//        StringBuilder id = new StringBuilder("District:d_w_id=");
//        switch (idParts[0]) {
//            case "Customer": {
//                id.append(idParts[1].split("=")[1]);
//                id.append(":d_id=");
//                id.append(idParts[2].split("=")[1]);
//                break;
//            }
//            case "History": {
//                id.append(idParts[1].split("=")[1]);
//                id.append(":d_id=");
//                id.append(idParts[2].split("=")[1]);
//                break;
//            }
//            case "Stock": {
//                id.append(idParts[1].split("=")[1]);
//                id.append(":d_id=");
//                id.append(TpccUtil.mapStockToDistrict(node.getId().getSId()));
//                break;
//            }
//            case "Order": {
//                id.append(idParts[1].split("=")[1]);
//                id.append(":d_id=");
//                id.append(idParts[2].split("=")[1]);
//                break;
//            }
//            case "NewOrder": {
//                id.append(idParts[1].split("=")[1]);
//                id.append(":d_id=");
//                id.append(idParts[2].split("=")[1]);
//                break;
//            }
//            case "OrderLine": {
//                id.append(idParts[1].split("=")[1]);
//                id.append(":d_id=");
//                id.append(idParts[2].split("=")[1]);
//                break;
//            }
//
//        }
//        ObjId parentId;
//        try {
//            parentId = this.secondaryIndex.get(id.toString()).iterator().next();
//        } catch (NullPointerException e) {
//            System.out.println("ERROR: can't find parent of " + node + " - generated id=" + id.toString());
//            throw e;
//        }
//        return this.graph.getNode(parentId);
//
//    }


//    public boolean isTransient(ObjId objId) {
//        if (objId.getSId() == null) return false;
//        String modelName = splitterCollon.split(objId.getSId()).iterator().next();
//        if (modelName == null) return false;
//        if (modelName.equals("Customer")
//                || modelName.equals("History")
//                || modelName.equals("NewOrder")
//                || modelName.equals("Order")
//                || modelName.equals("OrderLine")
//                || modelName.equals("Stock"))
//            return true;
//        return false;
//    }

//    public void addToSecondaryIndex(Map<String, Set<ObjId>> secondaryIndex, String objKey, ObjId objId, PRObject obj) {
//        List<String> keys = new ArrayList<>();
//        // todo: obj is always null?
//        if (obj != null) {
//            keys.add(objKey);
//            if (obj instanceof Warehouse) {
//                keys.add(safeKey("Warehouse",
//                        "w_id", String.valueOf(((Warehouse) obj).w_id)));
//            } else if (obj instanceof District) {
//                keys.add(safeKey("District",
//                        "d_w_id", String.valueOf(((District) obj).d_w_id),
//                        "d_id", String.valueOf(((District) obj).d_id)));
//            } else if (obj instanceof Customer) {
//                keys.add(safeKey("Customer",
//                        "c_w_id", String.valueOf(((Customer) obj).c_w_id),
//                        "c_d_id", String.valueOf(((Customer) obj).c_d_id),
//                        "c_last", String.valueOf(((Customer) obj).c_last)));
//            } else if (obj instanceof History) {
//            } else if (obj instanceof Stock) {
//            } else if (obj instanceof Order) {
//                keys.add(safeKey("Order",
//                        "o_w_id", String.valueOf(((Order) obj).o_w_id),
//                        "o_d_id", String.valueOf(((Order) obj).o_d_id),
//                        "o_c_id", String.valueOf(((Order) obj).o_c_id)));
//                keys.add(safeKey("Order",
//                        "o_w_id", String.valueOf(((Order) obj).o_w_id),
//                        "o_d_id", String.valueOf(((Order) obj).o_d_id),
//                        "o_id", String.valueOf(((Order) obj).o_id)));
//            } else if (obj instanceof NewOrder) {
//                keys.add(safeKey("NewOrder",
//                        "no_w_id", String.valueOf(((NewOrder) obj).no_w_id),
//                        "no_d_id", String.valueOf(((NewOrder) obj).no_d_id)));
//            } else if (obj instanceof OrderLine) {
//                keys.add(safeKey("OrderLine",
//                        "ol_w_id", String.valueOf(((OrderLine) obj).ol_w_id),
//                        "ol_d_id", String.valueOf(((OrderLine) obj).ol_d_id)));
//                keys.add(safeKey("OrderLine",
//                        "ol_w_id", String.valueOf(((OrderLine) obj).ol_w_id),
//                        "ol_d_id", String.valueOf(((OrderLine) obj).ol_d_id),
//                        "ol_o_id", String.valueOf(((OrderLine) obj).ol_o_id)));
//            } else if (obj instanceof Item) {
//            }
//        } else {
//            keys.add(objKey);
//        }
//        for (String key : keys) {
//            Set<ObjId> oids = secondaryIndex.get(key);
////            System.out.println("partition " + StateMachine.getMachine().getReplicaId() + " adding " + key + " - objid" + objId);
//            if (oids == null) {
//                String model = key.split(":")[0];
//                if (model.equals("NewOrder")) {
//                    Comparator comparator = new TpccProcedure.NewOrderIdComparator();
//                    oids = new TreeSet(comparator);
//                } else if (model.equals("OrderLine")) {
//                    Comparator comparator = new TpccProcedure.OrderLineOrderIdComparator();
//                    oids = new TreeSet(comparator);
//                } else if (model.equals("Order")) {
//                    Comparator comparator = new TpccProcedure.OrderIdComparator();
//                    oids = new TreeSet(comparator);
//                } else {
//                    oids = new HashSet<>();
//                }
//                secondaryIndex.put(key, oids);
//            }
//            try {
//                oids.add(objId);
//            } catch (Exception e) {
//            }
//        }
//    }


    public static class OrderIdComparator implements Comparator<ObjId> {
        public int compare(ObjId c1, ObjId c2) {
//            return Integer.parseInt(Lists.newArrayList(splitterEqual.split(Lists.newArrayList(splitterCollon.split(c2.getSId())).get(4))).get(1)) - Integer.parseInt(Lists.newArrayList(splitterEqual.split(Lists.newArrayList(splitterCollon.split(c1.getSId())).get(4))).get(1));
            if (c2.sId.split(":").length < 5) System.out.print("AAAA" + c2);
            return Integer.parseInt(c2.getSId().split(":")[4].split("=")[1]) - Integer.parseInt(c1.getSId().split(":")[4].split("=")[1]);
        }
    }

    public static class OrderLineOrderIdComparator implements Comparator<ObjId> {
        public int compare(ObjId c1, ObjId c2) {
//            Iterator tmp1 = splitterCollon.split(c2.getSId()).iterator();
//            tmp1.next();
//            tmp1.next();
//            tmp1.next();
//            Iterator tmp2 = splitterEqual.split((CharSequence) tmp1.next()).iterator();
//            tmp2.next();
//            int i2 = Integer.parseInt((String) tmp2.next());
//
//            tmp1 = splitterCollon.split(c1.getSId()).iterator();
//            tmp1.next();
//            tmp1.next();
//            tmp1.next();
//            tmp2 = splitterEqual.split((CharSequence) tmp1.next()).iterator();
//            tmp2.next();
//            int i1 = Integer.parseInt((String) tmp2.next());
//            return i2 - i1;
            return Integer.parseInt(c2.getSId().split(":")[3].split("=")[1]) - Integer.parseInt(c1.getSId().split(":")[3].split("=")[1]);
        }
    }

    public static class NewOrderIdComparator implements Comparator<ObjId> {
        public int compare(ObjId c1, ObjId c2) {
//            return Integer.parseInt(Lists.newArrayList(splitterEqual.split(Lists.newArrayList(splitterCollon.split(c1.getSId())).get(3))).get(1)) - Integer.parseInt(Lists.newArrayList(splitterEqual.split(Lists.newArrayList(splitterCollon.split(c2.getSId())).get(3))).get(1));
            return Integer.parseInt(c1.getSId().split(":")[3].split("=")[1]) - Integer.parseInt(c2.getSId().split(":")[3].split("=")[1]);
        }
    }
}

