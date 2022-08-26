package ch.usi.dslab.lel.dynastar.tpcc.tpcc;

import ch.usi.dslab.lel.dynastar.tpcc.rows.*;
import ch.usi.dslab.lel.dynastar.tpcc.tables.Tables;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * Author: longle, created on 08/04/16.
 */
public class TpccDataGenerator {
//    int warehouseCount, districtCount, customerCount, itemCount;
//    String filePath;
//    Random gen = new Random(System.currentTimeMillis());
//    AtomicInteger objIdCount = new AtomicInteger(0);
//
//    public TpccDataGenerator(String filePath, int warehouseCount, int districtCount, int customerCount, int itemCount) {
//        this.warehouseCount = warehouseCount;
//        this.districtCount = districtCount;
//        this.customerCount = customerCount;
//        this.itemCount = itemCount;
//        this.filePath = filePath;
//    }

//    public static void main(String[] args) {
////        jTPCCDataGenerator.loadData(filePath);
//        System.out.println("Creating dataset for jTPCC. Enter required information");
//        Scanner scan = new Scanner(System.in);
//        String input;
//        System.out.print("Number of warehouses:");
//        input = scan.nextLine();
//        int warehouseCount = Integer.parseInt(input);
//        System.out.print("Number of district per warehouse:");
//        input = scan.nextLine();
//        int districtCount = Integer.parseInt(input);
//        System.out.print("Number of customer per district:");
//        input = scan.nextLine();
//        int customerCount = Integer.parseInt(input);
//        System.out.print("Number of item:");
//        input = scan.nextLine();
//        int itemCount = Integer.parseInt(input);
//        String filePath = "/Users/longle/Documents/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/databases/";
////        String filePath = "/home/long/apps/ScalableSMR/dynastarTPCC/bin/databases/";
//        TpccDataGenerator generator = new TpccDataGenerator(filePath, warehouseCount, districtCount, customerCount, itemCount);
//        generator.generateCSVData();

//        Tables tables = new Tables();
//        String filePath = "/Users/meslahik/MyMac/PhD/Code/SharedMemorySSMR/dynastar-tpcc/bin-heron/databases/w_1_d_10_c_10_i_10.data";
//        loadCSVData(filePath, tables, 1);
//        System.out.println(tables.warehouseTable.get(1));
//        System.out.println(tables.districtTable.get(3));
//        System.out.println(tables.itemsTable.get(4));
//        System.out.println(tables.historyTable.get(2));
//        System.out.println(tables.orderTables[1].get(2));
//        System.out.println(tables.orderTables[5].get(2));
//    }

//    public static void loadCSVData(String filePath, Tables tables, int warehouseId) {
////        Map<String, Object> ret = new HashMap<>();
////        Set<Row> allObj = new HashSet<>();
//
//        Map<String, String[]> headers = new HashMap<>();
//        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
//            int tableIndex;
//            for (String line; (line = br.readLine()) != null; ) {
//                String[] tmp = line.split(",");
//                if (tmp.length == 2)
//                    continue;
//                else {
//                    switch (tmp[0]) {
//                        case "Header":
////                            headers.put(tmp[1], Arrays.copyOfRange(tmp, 1, tmp.length));
//                            headers.put(tmp[1], tmp);
//                            break;
//                        case "Warehouse":
//                            // table is replicated, no exclusion of other warehouse entries
//                            Warehouse w = new Warehouse();
//                            w = w.fromCSV(headers.get("Warehouse"), tmp);
////                            allObj.add(w);
//                            tables.warehouseTable.put(w);
//                            break;
//                        case "Item":
//                            // table is replicated, no exclusion of other warehouse entries
//                            Item i = new Item();
//                            i = i.fromCSV(headers.get("Item"), tmp);
////                            allObj.add(i);
//                            tables.itemsTable.put(i);
//                            break;
//                        case "Customer":
////                            if (Integer.parseInt(tmp[1]) != warehouseId)
////                                break;
////                            Customer c = new Customer();
//////                            c = c.fromCSV((String[]) ret.get("Customer"), tmp);
////                            c = c.fromCSV(headers.get("Customer"), tmp);
////                            tableIndex = Integer.parseInt(tmp[2]);
////                            tables.customerTables[tableIndex].put(c);
//                            break;
//                        case "Stock":
////                            if (Integer.parseInt(tmp[1]) != warehouseId)
////                                break;
////                            Stock s = new Stock();
//////                            s = s.fromCSV((String[]) ret.get("Stock"), tmp);
////                            s = s.fromCSV(headers.get( "Stock"), tmp);
//////                            allObj.add(s);
////                            tables.stockTable.put(s);
//                            break;
//                        case "District":
//                            // table is replicated, no exclusion of other warehouse entries
//                            District d = new District();
////                            d = d.fromCSV((String[]) ret.get("District"), tmp);
////                            allObj.add(d);
//                            d = d.fromCSV(headers.get("District"), tmp);
//                            tables.districtTable.put(d);
//                            break;
//                        case "History":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
//                            History h = new History();
////                            h = h.fromCSV((String[]) ret.get("History"), tmp);
////                            allObj.add(h);
//                            h = h.fromCSV(headers.get("History"), tmp);
//                            tables.historyTable.put(h);
//                            break;
//                        case "Order":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
//                            Order o = new Order();
////                            o = o.fromCSV((String[]) ret.get("Order"), tmp);
////                            allObj.add(o);
//                            o = o.fromCSV(headers.get("Order"), tmp);
//                            tableIndex = Integer.parseInt(tmp[2]);
//                            tables.orderTables[tableIndex].put(o);
//                            break;
//                        case "NewOrder":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
//                            NewOrder no = new NewOrder();
////                            no = no.fromCSV((String[]) ret.get("NewOrder"), tmp);
////                            allObj.add(no);
//                            no = no.fromCSV(headers.get("NewOrder"), tmp);
//                            tableIndex = Integer.parseInt(tmp[2]);
//                            tables.newOrderTables[tableIndex].put(no);
//                            break;
//                        case "OrderLine":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
//                            OrderLine ol = new OrderLine();
////                            ol = ol.fromCSV((String[]) ret.get("OrderLine"), tmp);
////                            allObj.add(ol);
//                            ol = ol.fromCSV(headers.get("OrderLine"), tmp);
//                            tableIndex = Integer.parseInt(tmp[2]);
//                            tables.orderTables[tableIndex].addOrderLine(ol);
//                            break;
//                    }
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

//    public static void loadCSVData(String filePath, TpccUtil.Callback callback) {
//        //read file into stream, try-with-resources
//        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
//            stream.forEach(callback::callback);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

//    private void generateCSVData() {
//        List<String> lines = new ArrayList<>();
//        lines.add("warehouseCount," + warehouseCount);
//        lines.add("districtCount," + districtCount);
//        lines.add("customerCount," + customerCount);
//        lines.add("itemCount," + itemCount);
//        List<Warehouse> warehouses = genWarehouses();
//        List<Item> items = genItems();
//        List<Stock> stocks = genStocks();
//        List<District> districts = genDistricts();
//        Map<String, List> cust_hists = genCustomersAndOrderHistory();
//        List<Customer> customers = cust_hists.get("customers");
//        List<History> orderHistories = cust_hists.get("orderHistories");
//        Map<String, List> orderList = genOrders();
//        List<Order> orders = orderList.get("orders");
//        List<OrderLine> orderLines = orderList.get("orderLines");
//        List<NewOrder> newOrders = orderList.get("newOrders");
//
//        lines.add(warehouses.get(0).toCSVHeader());
//        lines.add(districts.get(0).toCSVHeader());
//        lines.add(items.get(0).toCSVHeader());
//        lines.add(stocks.get(0).toCSVHeader());
//        lines.add(customers.get(0).toCSVHeader());
//        lines.add(orderHistories.get(0).toCSVHeader());
//        lines.add(orders.get(0).toCSVHeader());
//        lines.add(orderLines.get(0).toCSVHeader());
//        lines.add(newOrders.get(0).toCSVHeader());
//
//        warehouses.stream().forEach(warehouse -> lines.add(warehouse.toCSVString()));
//        districts.stream().forEach(district -> lines.add(district.toCSVString()));
//        items.stream().forEach(item -> lines.add(item.toCSVString()));
//        stocks.stream().forEach(stock -> lines.add(stock.toCSVString()));
//        customers.stream().forEach(customer -> lines.add(customer.toCSVString()));
//        orderHistories.stream().forEach(orderHistory -> lines.add(orderHistory.toCSVString()));
//        orders.stream().forEach(order -> lines.add(order.toCSVString()));
//        orderLines.stream().forEach(orderLine -> lines.add(orderLine.toCSVString()));
//        newOrders.stream().forEach(newOrder -> lines.add(newOrder.toCSVString()));
//
//
//        try {
//            String fileName = "w_" + this.warehouseCount + "_d_" + this.districtCount + "_c_" + this.customerCount + "_i_" + this.itemCount + ".data";
//            Path file = Paths.get(filePath + fileName);
//            Files.write(file, lines);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void generateJSONData() {
//        JSONObject config = new JSONObject();
//        config.put("warehouseCount", warehouseCount);
//        config.put("districtCount", districtCount);
//        config.put("customerCount", customerCount);
//        config.put("itemCount", itemCount);
//        JSONArray warehouses = genJSONWarehouses();
//        config.put("warehouses", warehouses);
//        JSONArray districts = genJSONDistricts();
//        config.put("districts", districts);
//        JSONArray items = genJSONItems();
//        config.put("items", items);
//        JSONArray stocks = genJSONStocks();
//        config.put("stocks", stocks);
//
//        Map<String, JSONArray> cust_hists = genJSONCustomersAndOrderHistory();
//        JSONArray customers = cust_hists.get("customers");
//        config.put("customers", customers);
//        JSONArray orderHistories = cust_hists.get("orderHistories");
//        config.put("orderHistories", orderHistories);
//
//        Map<String, JSONArray> orderList = genJSONOrders();
//        JSONArray orders = orderList.get("orders");
//        config.put("orders", orders);
//        JSONArray orderLines = orderList.get("orderLines");
//        config.put("orderLines", orderLines);
//        JSONArray newOrders = orderList.get("newOrders");
//        config.put("newOrders", newOrders);
//
//        try {
//            String fileName = "w_" + this.warehouseCount + "_d_" + this.districtCount + "_c_" + this.customerCount + "_i_" + this.itemCount + ".data";
//            Path file = Paths.get(filePath + fileName);
//            ArrayList<String> str = new ArrayList<>();
//            str.add(config.toJSONString());
//            Files.write(file, str);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
////        try {
////            String fileName = "w_" + this.warehouseCount + "_d_" + this.districtCount + "_i_" + this.itemCount + "_c_" + this.customerCount + ".json";
////            FileWriter file = new FileWriter(filePath + fileName);
////            file.write(config.toJSONString());
////            file.flush();
////            file.close();
////        } catch (IOException e) {
////            e.printStackTrace();
////        }
//    }
//
//    private List<Warehouse> genWarehouses() {
//        List<Warehouse> warehouses = new ArrayList<>();
//        for (int w = 1; w <= warehouseCount; w++) {
//
////            ObjId oid1 = Row.genObjId(Row.MODEL.WAREHOUSE, new String[]{"w_id", String.valueOf(w)});
//            Warehouse warehouse = new Warehouse(w);
//            warehouse.setId(new ObjId(objIdCount.getAndIncrement()));
//            warehouse.w_ytd = 300000;
//
//            // random within [0.0000 .. 0.2000]
//            warehouse.w_tax = (float) ((TpccUtil.randomNumber(0, 2000, gen)) / 10000.0);
//
//            warehouse.w_name = TpccUtil.randomStr(TpccUtil.randomNumber(6, 10, gen));
//            warehouse.w_street_1 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
//            warehouse.w_street_2 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
//            warehouse.w_city = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
//            warehouse.w_state = TpccUtil.randomStr(3).toUpperCase();
//            warehouse.w_zip = 123456;
//            warehouses.add(warehouse);
//        }
//        return warehouses;
//    }
//
//    private JSONArray genJSONWarehouses() {
//        JSONArray warehouses = new JSONArray();
//        genWarehouses().stream().forEach(warehouse -> warehouses.add(warehouse.toJSON()));
//        return warehouses;
//    }
//
//    private List<Item> genItems() {
//        int randPct;
//        int len;
//        int startORIGINAL;
//        List<Item> items = new ArrayList<>();
//
//        for (int i = 1; i <= itemCount; i++) {
////            ObjId oid1 = Row.genObjId(Row.MODEL.ITEM, new String[]{"i_id", String.valueOf(i)});
//            Item item = new Item(i);
//            item.setId(new ObjId(objIdCount.getAndIncrement()));
//            item.i_name = TpccUtil.randomStr(TpccUtil.randomNumber(14, 24, gen));
//            item.i_price = (float) (TpccUtil.randomNumber(100, 10000, gen) / 100.0);
//
//            // i_data
//            randPct = TpccUtil.randomNumber(1, 100, gen);
//            len = TpccUtil.randomNumber(26, 50, gen);
//            if (randPct > 10) {
//                // 90% of time i_data isa random string of length [26 .. 50]
//                item.i_data = TpccUtil.randomStr(len);
//            } else {
//                // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
//                startORIGINAL = TpccUtil.randomNumber(2, (len - 8), gen);
//                item.i_data = TpccUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TpccUtil.randomStr(len - startORIGINAL - 9);
//            }
//
//            item.i_im_id = TpccUtil.randomNumber(1, 10000, gen);
//            items.add(item);
//        } // end for
//        return items;
//    }
//
//    private JSONArray genJSONItems() {
//        JSONArray items = new JSONArray();
//        genItems().stream().forEach(item -> items.add(item.toJSON()));
//        return items;
//    }
//
//    private List<Stock> genStocks() {
//        int randPct = 0;
//        int len = 0;
//        int startORIGINAL = 0;
//        List<Stock> stocks = new JSONArray();
//        for (int i = 1; i <= itemCount; i++) {
//            for (int w = 1; w <= warehouseCount; w++) {
////                getPermit();
//                //TODO: handle 2 primary keys
////                ObjId oid1 = Row.genObjId(Row.MODEL.STOCK, new String[]{"s_w_id", String.valueOf(w), "s_i_id", String.valueOf(i)});
//                Stock stock = new Stock(w, i);
//                stock.setId(new ObjId(objIdCount.getAndIncrement()));
//                stock.s_i_id = i;
//                stock.s_w_id = w;
//                stock.s_quantity = TpccUtil.randomNumber(10, 100, gen);
//                stock.s_ytd = 0;
//                stock.s_order_cnt = 0;
//                stock.s_remote_cnt = 0;
//
//                // s_data
//                randPct = TpccUtil.randomNumber(1, 100, gen);
//                len = TpccUtil.randomNumber(26, 50, gen);
//                if (randPct > 10) {
//                    // 90% of time i_data isa random string of length [26 .. 50]
//                    stock.s_data = TpccUtil.randomStr(len);
//                } else {
//                    // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
//                    startORIGINAL = TpccUtil.randomNumber(2, (len - 8), gen);
//                    stock.s_data = TpccUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TpccUtil.randomStr(len - startORIGINAL - 9);
//                }
//
//                stock.s_dist_01 = TpccUtil.randomStr(24);
//                stock.s_dist_02 = TpccUtil.randomStr(24);
//                stock.s_dist_03 = TpccUtil.randomStr(24);
//                stock.s_dist_04 = TpccUtil.randomStr(24);
//                stock.s_dist_05 = TpccUtil.randomStr(24);
//                stock.s_dist_06 = TpccUtil.randomStr(24);
//                stock.s_dist_07 = TpccUtil.randomStr(24);
//                stock.s_dist_08 = TpccUtil.randomStr(24);
//                stock.s_dist_09 = TpccUtil.randomStr(24);
//                stock.s_dist_10 = TpccUtil.randomStr(24);
//
//                stocks.add(stock);
//            } // end for [w]
//        } // end for [i]
//        return stocks;
//    }
//
//    private JSONArray genJSONStocks() {
//        JSONArray stocks = new JSONArray();
//        genStocks().stream().forEach(stock -> stocks.add(stock.toJSON()));
//        return stocks;
//    }
//
//    private List<District> genDistricts() {
//        List<District> districts = new ArrayList<>();
//        final int t = warehouseCount * districtCount;
//        for (int w = 1; w <= warehouseCount; w++) {
//            for (int d = 1; d <= districtCount; d++) {
////                ObjId oid1 = Row.genObjId(Row.MODEL.DISTRICT, new String[]{"d_id", String.valueOf(d), "d_w_id", String.valueOf(w)});
//                District district = new District(d, w);
//                district.setId(new ObjId(objIdCount.getAndIncrement()));
//                district.d_id = d;
//                district.d_w_id = w;
//                district.d_ytd = 30000;
//
//                // random within [0.0000 .. 0.2000]
//                district.d_tax = (float) ((TpccUtil.randomNumber(0, 2000, gen)) / 10000.0);
//
//                district.d_next_o_id = customerCount + 1;
//                district.d_name = TpccUtil.randomStr(TpccUtil.randomNumber(6, 10, gen));
//                district.d_street_1 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
//                district.d_street_2 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
//                district.d_city = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
//                district.d_state = TpccUtil.randomStr(3).toUpperCase();
//                district.d_zip = 12345678;
//
//                districts.add(district);
//            } // end for [d]
//        } // end for [w]
//        return districts;
//    }
//
//    private JSONArray genJSONDistricts() {
//        JSONArray districts = new JSONArray();
//        genDistricts().stream().forEach(district -> districts.add(district.toJSON()));
//        return districts;
//    }
//
//    private Map<String, List> genCustomersAndOrderHistory() {
//        Map<String, List> ret = new HashMap<>();
//        List<Customer> customers = new ArrayList();
//        List<History> orderHistories = new ArrayList();
//
//        for (int w = 1; w <= warehouseCount; w++) {
//            for (int d = 1; d <= districtCount; d++) {
//                for (int c = 1; c <= customerCount; c++) {
//                    long sysdate = System.currentTimeMillis();
//                    String c_last = TpccUtil.getLastName(gen);
//                    Customer customer = new Customer(c, d, w, c_last);
//                    customer.setId(new ObjId(objIdCount.getAndIncrement()));
////                    ObjId oidCust = Row.genObjId(Row.MODEL.CUSTOMER, new String[]{"c_id", String.valueOf(c), "c_d_id", String.valueOf(d), "c_w_id", String.valueOf(w), "c_last", String.valueOf(c_last)});
//
//                    // discount is random between [0.0000 ... 0.5000]
//                    customer.c_discount = (float) (TpccUtil.randomNumber(1, 5000, gen) / 10000.0);
//
//                    if (TpccUtil.randomNumber(1, 100, gen) <= 90) {
//                        customer.c_credit = "BC";   // 10% Bad Credit
//                    } else {
//                        customer.c_credit = "GC";   // 90% Good Credit
//                    }
//
//                    customer.c_last = c_last;
//                    customer.c_first = TpccUtil.randomStr(TpccUtil.randomNumber(8, 16, gen));
//                    customer.c_credit_lim = 50000;
//
//                    customer.c_balance = -10;
//                    customer.c_ytd_payment = 10;
//                    customer.c_payment_cnt = 1;
//                    customer.c_delivery_cnt = 0;
//
//                    customer.c_street_1 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
//                    customer.c_street_2 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
//                    customer.c_city = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
//                    customer.c_state = TpccUtil.randomStr(3).toUpperCase();
//                    customer.c_zip = 123456789;
//
//                    customer.c_phone = "(732)744-1700";
//
//                    customer.c_since = sysdate;
//                    customer.c_middle = "OE";
//                    customer.c_data = TpccUtil.randomStr(TpccUtil.randomNumber(300, 500, gen));
//
////                    ObjId oidHist = Row.genObjId(Row.MODEL.HISTORY, new String[]{"h_c_id", String.valueOf(c), "h_c_d_id", String.valueOf(d), "h_c_w_id", String.valueOf(w)});
//                    History history = new History(c, d, w);
//                    history.setId(new ObjId(objIdCount.getAndIncrement()));
//                    history.h_d_id = d;
//                    history.h_w_id = w;
//                    history.h_date = sysdate;
//                    history.h_amount = 10;
//                    history.h_data = TpccUtil.randomStr(TpccUtil.randomNumber(10, 24, gen));
//
//                    customers.add(customer);
//                    orderHistories.add(history);
//                } // end for [c]
//            } // end for [d]
//        } // end for [w]
//        ret.put("customers", customers);
//        ret.put("orderHistories", orderHistories);
//        return ret;
//    }
//
//    private Map<String, JSONArray> genJSONCustomersAndOrderHistory() {
//        Map<String, List> objs = genCustomersAndOrderHistory();
//        Map<String, JSONArray> ret = new HashMap<>();
//        JSONArray cus = new JSONArray();
//        JSONArray his = new JSONArray();
//        ((List<Customer>) objs.get("customers")).stream().forEach(o -> {
//            cus.add(o.toJSON());
//        });
//        ((List<History>) objs.get("orderHistories")).stream().forEach(o -> {
//            his.add(o.toJSON());
//        });
//        ret.put("customers", cus);
//        ret.put("orderHistories", his);
//        return ret;
//    }
//
//    private Map<String, JSONArray> genJSONOrders() {
//        Map<String, JSONArray> ret = new HashMap<>();
//        Map<String, List> objs = genOrders();
//        JSONArray orders = new JSONArray();
//        JSONArray newOrders = new JSONArray();
//        JSONArray orderLines = new JSONArray();
//        ((List<Order>) objs.get("orders")).stream().forEach(o -> {
//            orders.add(o.toJSON());
//        });
//        ((List<NewOrder>) objs.get("newOrders")).stream().forEach(o -> {
//            newOrders.add(o.toJSON());
//        });
//        ((List<OrderLine>) objs.get("orderLines")).stream().forEach(o -> {
//            orderLines.add(o.toJSON());
//        });
//        ret.put("orders", orders);
//        ret.put("newOrders", newOrders);
//        ret.put("orderLines", orderLines);
//        return ret;
//    }
//
//    public Map<String, List> genOrders() {
//        Map<String, List> ret = new HashMap<>();
//        List<Order> orders = new ArrayList<>();
//        List<NewOrder> newOrders = new ArrayList<>();
//        List<OrderLine> orderLines = new ArrayList<>();
//        int createCount = 0;
//        final int t = (warehouseCount * districtCount * customerCount);
//        List<Order> orderList = new ArrayList<>();
//        for (int w = 1; w <= warehouseCount; w++) {
//            for (int d = 1; d <= districtCount; d++) {
//                for (int c = 1; c <= customerCount; c++) {
//                    int o_c_id = TpccUtil.randomNumber(1, customerCount, gen);
//                    final Order oorder = new Order(w, d, o_c_id, c);
//                    oorder.setId(new ObjId(objIdCount.getAndIncrement()));
//                    oorder.o_c_id = o_c_id;
//                    oorder.o_carrier_id = TpccUtil.randomNumber(1, 10, gen);
//                    oorder.o_ol_cnt = TpccUtil.randomNumber(5, 15, gen);
//                    oorder.o_all_local = 1;
//                    oorder.o_entry_d = System.currentTimeMillis();
//                    orders.add(oorder);
//                    orderList.add(oorder);
//
//                    // 900 rows in the NEW-ORDER table corresponding to the last
//                    // 900 rows in the ORDER table for that district (i.e., with
//                    // NO_O_ID between 2,101 and 3,000) (70%)
//                    if (oorder.o_id > customerCount * 0.7) {
//                        NewOrder new_order = new NewOrder(oorder.o_w_id, oorder.o_d_id, oorder.o_id);
//                        new_order.setId(new ObjId(objIdCount.getAndIncrement()));
//                        newOrders.add(new_order);
//                    }
//
//                    if (++createCount == t) {
//                        int a = 0;
//                        for (Order tmp : orderList) {
//                            a += tmp.o_ol_cnt;
//                        }
//                        final int totalOderLine = a;
//                        for (Order order : orderList) {
//                            for (int l = 1; l <= order.o_ol_cnt; l++) {
//                                OrderLine order_line = new OrderLine(order.o_w_id, order.o_d_id, order.o_id, l);
//                                order_line.setId(new ObjId(objIdCount.getAndIncrement()));
//                                order_line.ol_i_id = TpccUtil.randomNumber(1, itemCount, gen);
//                                order_line.ol_delivery_d = order.o_entry_d;
//
//                                if (order_line.ol_o_id < customerCount * 0.7) {
//                                    order_line.ol_amount = 0;
//                                } else {
//                                    // random within [0.01 .. 9,999.99]
//                                    order_line.ol_amount = (float) (TpccUtil.randomNumber(1, 999999, gen) / 100.0);
//                                }
//
//                                order_line.ol_supply_w_id = TpccUtil.randomNumber(1, warehouseCount, gen);
//                                order_line.ol_quantity = 5;
//                                order_line.ol_dist_info = TpccUtil.randomStr(24);
//                                orderLines.add(order_line);
//                            } // end for [l]
//                        }
//                    }
//                } // end for [c]
//            } // end for [d]
//        } // end for [w]
//        ret.put("orders", orders);
//        ret.put("newOrders", newOrders);
//        ret.put("orderLines", orderLines);
//        return ret;
//    }
//
//
//    public interface Callback {
//        void callback(String line);
//    }

}
