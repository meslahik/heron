package ch.usi.dslab.lel.dynastar.tpcc.tpcc;

//import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
//import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;


public class TpccTransaction {
//    public static Set<ObjId> getInvolvedObjectForNewOrder(TpccCommandPayload payload, Map<String, Set<ObjId>> indexing, PRObjectGraph objectGraph) throws TpccException {
//        Set<ObjId> ret = new HashSet<>();
//        int w_id = (int) payload.attributes.get("w_id");
//        int d_id = (int) payload.attributes.get("d_id");
//        int c_id = (int) payload.attributes.get("c_id");
//        int o_ol_cnt = (int) payload.attributes.get("o_ol_cnt");
//        int o_all_local = (int) payload.attributes.get("o_all_local");
//        int itemIds[] = (int[]) payload.attributes.get("itemIds");
//        int supplierWarehouseIDs[] = (int[]) payload.attributes.get("supplierWarehouseIDs");
//        int orderQuantities[] = (int[]) payload.attributes.get("orderQuantities");
//
//        ret.addAll(indexing.get(Row.genSId("Warehouse", w_id)));
//        ret.addAll(indexing.get(Row.genSId("District", w_id, d_id)));
//        ret.addAll(indexing.get(Row.genSId("Customer", w_id, d_id, c_id)));
//
//        for (int index = 1; index <= o_ol_cnt; index++) {
//            int ol_number = index;
//            int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
//            int ol_i_id = itemIds[ol_number - 1];
//            int ol_quantity = orderQuantities[ol_number - 1];
//
//            // If I_ID has an unused value (see Clause 2.4.1.5), a "not-found" condition is signaled,
//            // resulting in a rollback of the database transaction (see Clause 2.4.2.3).
//            if (ol_i_id == -12345) {
//                // an expected condition generated 1% of the time in the test data...
//                // we throw an illegal access exception and the transaction gets rolled back later on
//                throw new TpccException();
//            }
//            ret.addAll(indexing.get(Row.genSId("Item", ol_i_id)));
//            ret.addAll(indexing.get(Row.genSId("Stock", ol_supply_w_id, ol_i_id)));
////            ret.add(Row.genSId("OrderLine", w_id, d_id,))
//        }
//        return ret;
//    }
}
