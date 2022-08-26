package ch.usi.dslab.lel.dynastar.tpcc.tpcc;

import ch.usi.dslab.lel.dynastar.tpcc.rows.*;
import ch.usi.dslab.lel.dynastar.tpcc.tables.Tables;
import com.ibm.disni.*;
import com.ibm.disni.verbs.RdmaCmId;
import org.json.simple.JSONArray;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: longle, created on 08/04/16.
 */
public class TpccDataGeneratorV3 {
    int warehouseCount, districtCount, customerCount, itemCount;
    String filePath;
    Random gen = new Random(System.currentTimeMillis());
    AtomicInteger objIdCount = new AtomicInteger(0);
    boolean MINIMAL = true;

    public TpccDataGeneratorV3(String filePath, int warehouseCount, int districtCount, int customerCount, int itemCount) {
        this.warehouseCount = warehouseCount;
        this.districtCount = districtCount;
        this.customerCount = customerCount;
        this.itemCount = itemCount;
        this.filePath = filePath;
    }

    public static void main(String[] args) {
//        jTPCCDataGenerator.loadData(filePath);
        System.out.println("Creating dataset for jTPCC. Enter required information");
        Scanner scan = new Scanner(System.in);
        String input;
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


        System.out.print("Number of warehouses:");
        input = scan.nextLine();
        int warehouseCount = Integer.parseInt(input);
        int districtCount = 10;
        int customerCount = 3000;
        int itemCount = 100000;


//        // generate TPCC data
        String filePath = "/Users/meslahik/MyMac/PhD/Code/SharedMemorySSMR/dynastar-tpcc/bin-heron/databases/";
        TpccConfig.configCustPerDist = customerCount;
        TpccConfig.configItemCount = itemCount;
        TpccDataGeneratorV3 generator = new TpccDataGeneratorV3(filePath, warehouseCount, districtCount, customerCount, itemCount);
        generator.generateCSVData();


        // test data load
//        CustomEndpoint customEndpoint = null;
//        RdmaServerEndpoint serverEndpoint = null;
//        try {
//            // EndpointGroup needs factory to create Endpoints
//            // Factory needs to pass EndpointGroup to Endpoints
//            RdmaPassiveEndpointGroup<CustomEndpoint> endpointGroup = new RdmaPassiveEndpointGroup<>(1,1,1,1);
//            CustomFactory factory = new CustomFactory(endpointGroup);
//            endpointGroup.init(factory);
//            customEndpoint = endpointGroup.createEndpoint();
//            serverEndpoint = endpointGroup.createServerEndpoint();
//            serverEndpoint.bind(new InetSocketAddress("192.168.4.1", 60000), 100);
//        } catch (Exception ex) {
//            ex.printStackTrace();
//        }
//
//        Tables tables = new Tables(serverEndpoint);
////        String filePath = "/Users/meslahik/MyMac/PhD/Code/SharedMemorySSMR/dynastar-tpcc/bin-heron/databases/w_1_d_2_c_2_i_2.data";
//        String filePath = "/home/eslahm/Code/SharedMemorySSMR/dynastar-tpcc/bin-heron/databases/w_2_d_10_c_10_i_10.data";
//        loadCSVData(filePath, tables, 1);
//        System.out.println(tables.warehouseTable.get(1));
//        System.out.println(tables.districtTable.get(2));
//        System.out.println(tables.itemsTable.get(2));
//        System.out.println(tables.customerTables[1].get(1));
//        System.out.println(tables.customerTables[2].get(2));
//        System.out.println(tables.stockTable.get(2));
//        System.out.println(tables.historyTable.get(2));
//        System.out.println(tables.orderTables[1].get(2));
//        System.out.println(tables.orderTables[2].get(2));
    }


    public static class CustomEndpoint extends RdmaEndpoint {
        protected CustomEndpoint(RdmaEndpointGroup<? extends RdmaEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
            super(group, idPriv, serverSide);
        }

        public void init() throws IOException{
            //allocate and register buffers
            //initiate postRecv call to pre-post some recvs if necessary
            //...
        }
    }

    public static class CustomFactory implements RdmaEndpointFactory<CustomEndpoint> {
        RdmaPassiveEndpointGroup<CustomEndpoint> group;

        public CustomFactory(RdmaPassiveEndpointGroup<CustomEndpoint> group) {
            this.group = group;
        }

        @Override
        public CustomEndpoint createEndpoint(RdmaCmId rdmaCmId, boolean serverSide) throws IOException {
            return new CustomEndpoint(group, rdmaCmId, serverSide);
        }
    }

    public static void loadCSVData(String filePath, Tables tables, int warehouseId) {
//        Map<String, Object> ret = new HashMap<>();
//        Set<Row> allObj = new HashSet<>();

        Map<String, String[]> headers = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            int tableIndex;
            for (String line; (line = br.readLine()) != null; ) {
                String[] tmp = line.split(",");
                if (tmp.length == 2)
                    continue;
                else {
                    switch (tmp[0]) {
                        case "Header":
//                            headers.put(tmp[1], Arrays.copyOfRange(tmp, 1, tmp.length));
                            headers.put(tmp[1], tmp);
                            break;
                        case "Warehouse":
                            // table is replicated, no exclusion of other warehouse entries
                            Warehouse w = new Warehouse();
                            w = w.fromCSV(headers.get("Warehouse"), tmp);
//                            allObj.add(w);
                            tables.warehouseTable.put(w);
                            break;
                        case "Item":
                            // table is replicated, no exclusion of other warehouse entries
                            Item i = new Item();
                            i = i.fromCSV(headers.get("Item"), tmp);
//                            allObj.add(i);
                            tables.itemsTable.put(i);
                            break;
                        case "Customer":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
                            Customer c = new Customer();
//                            c = c.fromCSV((String[]) ret.get("Customer"), tmp);
                            c = c.fromCSV(headers.get("Customer"), tmp);
                            c.setWarehouse(warehouseId);
                            tableIndex = Integer.parseInt(tmp[2]);
                            tables.customerTables[tableIndex].putInit(c);
                            break;
                        case "Stock":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
                            Stock s = new Stock();
//                            s = s.fromCSV((String[]) ret.get("Stock"), tmp);
                            s = s.fromCSV(headers.get("Stock"), tmp);
                            s.setWarehouse(warehouseId);
//                            allObj.add(s);
                            tables.stockTable.putInit(s);
                            break;
                        case "District":
                            // table is replicated, no exclusion of other warehouse entries
                            District d = new District();
//                            d = d.fromCSV((String[]) ret.get("District"), tmp);
//                            allObj.add(d);
                            d = d.fromCSV(headers.get("District"), tmp);
                            tables.districtTable.put(d);
                            break;
                        case "History":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
                            History h = new History();
//                            h = h.fromCSV((String[]) ret.get("History"), tmp);
//                            allObj.add(h);
                            h = h.fromCSV(headers.get("History"), tmp);
                            h.setWarehouse(warehouseId);
                            tables.historyTable.put(h);
                            break;
                        case "Order":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
                            Order o = new Order();
//                            o = o.fromCSV((String[]) ret.get("Order"), tmp);
//                            allObj.add(o);
                            o = o.fromCSV(headers.get("Order"), tmp);
                            o.setWarehouse(warehouseId);
                            tableIndex = Integer.parseInt(tmp[2]);
                            tables.orderTables[tableIndex].put(o);
                            Customer cc = tables.customerTables[tableIndex].get(o.o_c_id);
                            cc.c_last_order = o.o_id;
                            tables.customerTables[tableIndex].put(cc);
                            break;
                        case "NewOrder":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
                            NewOrder no = new NewOrder();
//                            no = no.fromCSV((String[]) ret.get("NewOrder"), tmp);
//                            allObj.add(no);
                            no = no.fromCSV(headers.get("NewOrder"), tmp);
                            no.setWarehouse(warehouseId);
                            tableIndex = Integer.parseInt(tmp[2]);
                            tables.newOrderTables[tableIndex].put(no);
                            District district = tables.districtTable.get(tableIndex);
                            if (district.oldest_new_order > no.no_o_id)
                                district.oldest_new_order = no.no_o_id;
                            break;
                        case "OrderLine":
//                            if (Integer.parseInt(tmp[1]) != warehouseId)
//                                break;
                            OrderLine ol = new OrderLine();
//                            ol = ol.fromCSV((String[]) ret.get("OrderLine"), tmp);
//                            allObj.add(ol);
                            ol = ol.fromCSV(headers.get("OrderLine"), tmp);
                            ol.setWarehouse(warehouseId);
                            tableIndex = Integer.parseInt(tmp[2]);
                            tables.orderTables[tableIndex].addOrderLine(ol);
                            break;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    private void generateCSVData() {
        try {
            String fileName = "w_" + this.warehouseCount + "_d_" + this.districtCount + "_c_" + this.customerCount + "_i_" + this.itemCount + ".data";
            Path file = Paths.get(filePath + fileName);

            List<String> lines = new ArrayList<>();
            lines.add("warehouseCount," + warehouseCount);
            lines.add("districtCount," + districtCount);
            lines.add("customerCount," + customerCount);
            lines.add("itemCount," + itemCount);

//            lines.add(warehouses.get(0).toCSVHeader());
//            lines.add(districts.get(0).toCSVHeader());
//            lines.add(items.get(0).toCSVHeader());
//            lines.add(stocks.get(0).toCSVHeader());
//            lines.add(customers.get(0).toCSVHeader());
//            lines.add(orderHistories.get(0).toCSVHeader());
//            lines.add(orders.get(0).toCSVHeader());
//            lines.add(orderLines.get(0).toCSVHeader());
//            lines.add(newOrders.get(0).toCSVHeader());
//            Files.write(file, lines);
//            lines.clear();
//            System.out.println("Header data written to file");

            lines.add(Warehouse.getHeader());
            lines.add(District.getHeader());
            lines.add(Item.getHeader());
            lines.add(Stock.getHeader());
            lines.add(Customer.getHeader());
            lines.add(History.getHeader());
            lines.add(Order.getHeader());
            lines.add(OrderLine.getHeader());
            lines.add(NewOrder.getHeader());
//            Files.write(file, lines);
//            lines.clear();
//            System.out.println("Header data written to file");

            List<Warehouse> warehouses = genWarehouses();
            System.out.println("Warehouse data generated");
            warehouses.stream().forEach(warehouse -> lines.add(warehouse.toCSVString()));
//            Files.write(file, lines, StandardOpenOption.APPEND);
//            lines.clear();
//            warehouses.clear();
//            System.out.println("Warehouse data written to file");

            List<District> districts = genDistricts();
            System.out.println("District data generated");
            districts.stream().forEach(district -> lines.add(district.toCSVString()));
//            Files.write(file, lines, StandardOpenOption.APPEND);
//            lines.clear();
//            districts.clear();
//            System.out.println("District data written to file");

            List<Item> items = genItems();
            System.out.println("Item data generated");
            items.stream().forEach(item -> lines.add(item.toCSVString()));
//            Files.write(file, lines, StandardOpenOption.APPEND);
//            lines.clear();
//            items.clear();
//            System.out.println("Item data written to file");

//            for (int w = 1; w <= warehouseCount; w++) {
                int w=1;
                List<Stock> stocks = genStocks(w);
                System.out.println("Stock " + w + " data generated");
                stocks.stream().forEach(stock -> lines.add(stock.toCSVString()));
//                Files.write(file, lines, StandardOpenOption.APPEND);
//                System.out.println("Stock " + w + " data written to file");
//                lines.clear();
//            }

            Map<String, List> cust_hists = genCustomersAndOrderHistory();
            List<Customer> customers = cust_hists.get("customers");
            System.out.println("Customer and History data generated");
//            count = 0;
//            for (Customer customer: customers) {
                customers.stream().forEach(customer -> lines.add(customer.toCSVString()));
//                lines.add(customer.toCSVString());
//                if (count == 1000) {
//                    count = 0;
//                    Files.write(file, lines, StandardOpenOption.APPEND);
//                    lines.clear();
//                }
//            }
//            customers.clear();
//            System.out.println("Customer data written to file");

            List<History> orderHistories = cust_hists.get("orderHistories");
            orderHistories.stream().forEach(orderHistory -> lines.add(orderHistory.toCSVString()));
//            Files.write(file, lines, StandardOpenOption.APPEND);
//            lines.clear();
//            orderHistories.clear();
//            cust_hists.clear();
//            System.out.println("History data written to file");

            Map<String, List> orderList = genOrders();
            System.out.println("Order data generated");
            List<Order> orders = orderList.get("orders");
            orders.stream().forEach(order -> lines.add(order.toCSVString()));
//            Files.write(file, lines, StandardOpenOption.APPEND);
//            lines.clear();

            List<OrderLine> orderLines = orderList.get("orderLines");
            System.out.println("OrderLine data generated");
            orderLines.stream().forEach(orderLine -> lines.add(orderLine.toCSVString()));
//            Files.write(file, lines, StandardOpenOption.APPEND);
//            lines.clear();
//            orderLines.clear();

            List<NewOrder> newOrders = orderList.get("newOrders");
            System.out.println("NewOrder data generated");
            newOrders.stream().forEach(newOrder -> lines.add(newOrder.toCSVString()));

//            Files.write(file, lines, StandardOpenOption.APPEND);
            Files.write(file, lines);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
//    }

    private List<Warehouse> genWarehouses() {
        List<Warehouse> warehouses = new ArrayList<>();
        for (int w = 1; w <= warehouseCount; w++) {

//            ObjId oid1 = Row.genObjId(Row.MODEL.WAREHOUSE, new String[]{"w_id", String.valueOf(w)});
            Warehouse warehouse = new Warehouse(w);
//            warehouse.setId(Row.genObjId("Warehouse", w));
//            warehouse.setId(new ObjId(objIdCount.getAndIncrement()));
            warehouse.w_ytd = 300000;

            // random within [0.0000 .. 0.2000]
            warehouse.w_tax = (float) ((TpccUtil.randomNumber(0, 2000, gen)) / 10000.0);

            warehouse.w_name = TpccUtil.randomStr(TpccUtil.randomNumber(6, 10, gen));
            warehouse.w_street_1 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
            warehouse.w_street_2 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
            warehouse.w_city = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
            warehouse.w_state = TpccUtil.randomStr(3).toUpperCase();
            warehouse.w_zip = 123456;
            warehouses.add(warehouse);
        }
        return warehouses;
    }

    private JSONArray genJSONWarehouses() {
        JSONArray warehouses = new JSONArray();
        genWarehouses().stream().forEach(warehouse -> warehouses.add(warehouse.toJSON()));
        return warehouses;
    }

    private List<Item> genItems() {
        int randPct;
        int len;
        int startORIGINAL;
        List<Item> items = new ArrayList<>();

        for (int i = 1; i <= itemCount; i++) {
//            ObjId oid1 = Row.genObjId(Row.MODEL.ITEM, new String[]{"i_id", String.valueOf(i)});
            Item item = new Item(i);
//            item.setId(Row.genObjId("Item", i));
//            item.setId(new ObjId(objIdCount.getAndIncrement()));
            item.i_name = TpccUtil.randomStr(TpccUtil.randomNumber(14, 24, gen));
            item.i_price = (float) (TpccUtil.randomNumber(100, 10000, gen) / 100.0);

            // i_data
            randPct = TpccUtil.randomNumber(1, 100, gen);
            len = TpccUtil.randomNumber(26, 50, gen);
            if (randPct > 10) {
                // 90% of time i_data isa random string of length [26 .. 50]
                item.i_data = TpccUtil.randomStr(len);
            } else {
                // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
                startORIGINAL = TpccUtil.randomNumber(2, (len - 8), gen);
                item.i_data = TpccUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TpccUtil.randomStr(len - startORIGINAL - 9);
            }

            item.i_im_id = TpccUtil.randomNumber(1, 10000, gen);
            items.add(item);
        } // end for
        return items;
    }

//    private JSONArray genJSONItems() {
//        JSONArray items = new JSONArray();
//        genItems().stream().forEach(item -> items.add(item.toJSON()));
//        return items;
//    }

    private List<Stock> genStocks(int w) {
        int randPct = 0;
        int len = 0;
        int startORIGINAL = 0;
        List<Stock> stocks = new JSONArray();
        for (int i = 1; i <= itemCount; i++) {
//                getPermit();
                //TODO: handle 2 primary keys
//                ObjId oid1 = Row.genObjId(Row.MODEL.STOCK, new String[]{"s_w_id", String.valueOf(w), "s_i_id", String.valueOf(i)});
                Stock stock = new Stock(w, i);
//                stock.setId(Row.genObjId("Stock", w, i));
//                stock.setId(new ObjId(objIdCount.getAndIncrement()));
                stock.s_i_id = i;
                stock.s_w_id = w;
                stock.s_quantity = TpccUtil.randomNumber(10, 100, gen);
                stock.s_ytd = 0;
                stock.s_order_cnt = 0;
                stock.s_remote_cnt = 0;

                // s_data
                randPct = TpccUtil.randomNumber(1, 100, gen);
                len = TpccUtil.randomNumber(26, 50, gen);
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 .. 50]
                    ByteBuffer buffer = ByteBuffer.allocateDirect(50);
                    stock.s_data = buffer.clear().put(TpccUtil.randomStr(len).getBytes(StandardCharsets.ISO_8859_1));
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in middle
                    startORIGINAL = TpccUtil.randomNumber(2, (len - 8), gen);
                    String str = TpccUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TpccUtil.randomStr(len - startORIGINAL - 9);
                    ByteBuffer buffer = ByteBuffer.allocateDirect(50);
                    stock.s_data = buffer.clear().put(str.getBytes(StandardCharsets.ISO_8859_1));
                }

                ByteBuffer buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_01 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));
                buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_02 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));
                buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_03 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));
                buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_04 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));
                buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_05 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));
                buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_06 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));
                buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_07 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));
                buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_08 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));
                buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_09 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));
                buffer = ByteBuffer.allocateDirect(24);
                stock.s_dist_10 = buffer.clear().put(TpccUtil.randomStr(24).getBytes(StandardCharsets.ISO_8859_1));

                stocks.add(stock);
        } // end for [i]
        return stocks;
    }

//    private JSONArray genJSONStocks() {
//        JSONArray stocks = new JSONArray();
//        genStocks().stream().forEach(stock -> stocks.add(stock.toJSON()));
//        return stocks;
//    }

    private List<District> genDistricts() {
        List<District> districts = new ArrayList<>();
        final int t = warehouseCount * districtCount;
        for (int w = 1; w <= warehouseCount; w++) {
            for (int d = 1; d <= districtCount; d++) {
//                ObjId oid1 = Row.genObjId(Row.MODEL.DISTRICT, new String[]{"d_id", String.valueOf(d), "d_w_id", String.valueOf(w)});
                District district = new District(d, w);
//                district.setId(Row.genObjId("District", w, d));
//                district.setId(new ObjId(objIdCount.getAndIncrement()));
//                System.out.println("Generating " + district.getId());
                district.d_id = d;
                district.d_w_id = w;
                district.d_ytd = 30000;

                // random within [0.0000 .. 0.2000]
                district.d_tax = (float) ((TpccUtil.randomNumber(0, 2000, gen)) / 10000.0);

                district.d_next_o_id = customerCount + 1;
                district.d_name = TpccUtil.randomStr(TpccUtil.randomNumber(6, 10, gen));
                district.d_street_1 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
                district.d_street_2 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
                district.d_city = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
                district.d_state = TpccUtil.randomStr(3).toUpperCase();
                district.d_zip = 12345678;

                districts.add(district);
            } // end for [d]
        } // end for [w]
        return districts;
    }

//    private JSONArray genJSONDistricts() {
//        JSONArray districts = new JSONArray();
//        genDistricts().stream().forEach(district -> districts.add(district.toJSON()));
//        return districts;
//    }

    private Map<String, List> genCustomersAndOrderHistory() {
        Map<String, List> ret = new HashMap<>();
        List<Customer> customers = new ArrayList();
        List<History> orderHistories = new ArrayList();

//        for (int w = 1; w <= warehouseCount; w++) {
          int w=1;
            for (int d = 1; d <= districtCount; d++) {
                for (int c = 1; c <= customerCount; c++) {
                    long sysdate = System.currentTimeMillis();
                    Customer customer = new Customer(c, d, w);
//                    customer.setId(Row.genObjId("Customer", w, d, c));
//                    customer.setId(new ObjId(objIdCount.getAndIncrement()));
//                    ObjId oidCust = Row.genObjId(Row.MODEL.CUSTOMER, new String[]{"c_id", String.valueOf(c), "c_d_id", String.valueOf(d), "c_w_id", String.valueOf(w), "c_last", String.valueOf(c_last)});

                    // discount is random between [0.0000 ... 0.5000]
                    customer.c_discount = (float) (TpccUtil.randomNumber(1, 5000, gen) / 10000.0);

                    if (TpccUtil.randomNumber(1, 100, gen) <= 90) {
                         String str = "BC";   // 10% Bad Credit
                        ByteBuffer buffer = ByteBuffer.allocateDirect(2);
                        customer.c_credit = buffer.clear().put(str.getBytes(StandardCharsets.ISO_8859_1));
                    } else {
                        String str = "GC";   // 90% Good Credit
                        ByteBuffer buffer = ByteBuffer.allocateDirect(2);
                        customer.c_credit = buffer.clear().put(str.getBytes(StandardCharsets.ISO_8859_1));
                    }

                    String c_first = TpccUtil.randomStr(TpccUtil.randomNumber(8, 16, gen));
                    ByteBuffer buffer = ByteBuffer.allocateDirect(16);
                    customer.c_first = buffer.clear().put(c_first.getBytes(StandardCharsets.ISO_8859_1));

                    String c_middle = "OE";
                    buffer = ByteBuffer.allocateDirect(2);
                    customer.c_middle = buffer.clear().put(c_middle.getBytes(StandardCharsets.ISO_8859_1));

                    String c_last = TpccUtil.randomStr(TpccUtil.randomNumber(8, 16, gen));
                    buffer = ByteBuffer.allocateDirect(16);
                    customer.c_last = buffer.clear().put(c_last.getBytes(StandardCharsets.ISO_8859_1));

                    customer.c_credit_lim = 50000;
                    customer.c_balance = -10;
                    customer.c_ytd_payment = 10;
                    customer.c_payment_cnt = 1;
                    customer.c_delivery_cnt = 0;

                    String c_street_1 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
                    buffer = ByteBuffer.allocateDirect(20);
                    customer.c_street_1 = buffer.clear().put(c_street_1.getBytes(StandardCharsets.ISO_8859_1));

                    String c_street_2 = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
                    buffer = ByteBuffer.allocateDirect(20);
                    customer.c_street_2 = buffer.clear().put(c_street_2.getBytes(StandardCharsets.ISO_8859_1));

                    String c_city = TpccUtil.randomStr(TpccUtil.randomNumber(10, 20, gen));
                    buffer = ByteBuffer.allocateDirect(20);
                    customer.c_city = buffer.clear().put(c_city.getBytes(StandardCharsets.ISO_8859_1));

                    String c_state = TpccUtil.randomStr(3).toUpperCase();
                    buffer = ByteBuffer.allocateDirect(2);
                    customer.c_state = buffer.clear().put(c_state.getBytes(StandardCharsets.ISO_8859_1));

                    customer.c_zip = 123456789;

                    String c_phone = "(732)744-1700";
                    buffer = ByteBuffer.allocateDirect(16);
                    customer.c_phone = buffer.clear().put(c_phone.getBytes(StandardCharsets.ISO_8859_1));

                    customer.c_since = sysdate;

                    String c_data = TpccUtil.randomStr(TpccUtil.randomNumber(300, 500, gen));
                    buffer = ByteBuffer.allocateDirect(500);
                    customer.c_data = buffer.clear().put(c_data.getBytes(StandardCharsets.ISO_8859_1));

//                    ObjId oidHist = Row.genObjId(Row.MODEL.HISTORY, new String[]{"h_c_id", String.valueOf(c), "h_c_d_id", String.valueOf(d), "h_c_w_id", String.valueOf(w)});
                    History history = new History(c, d, w);
//                    history.setId(Row.genObjId("History", w, d, c));
//                    history.setId(new ObjId(objIdCount.getAndIncrement()));
                    history.h_d_id = d;
                    history.h_w_id = w;
                    history.h_date = sysdate;
                    history.h_amount = 10;
                    history.h_data = TpccUtil.randomStr(TpccUtil.randomNumber(10, 24, gen));

                    customers.add(customer);
                    orderHistories.add(history);
                } // end for [c]
            } // end for [d]
//        } // end for [w]
        ret.put("customers", customers);
        ret.put("orderHistories", orderHistories);
        return ret;
    }

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

    public Map<String, List> genOrders() {
        Map<String, List> ret = new HashMap<>();
        List<Order> orders = new ArrayList<>();
        List<NewOrder> newOrders = new ArrayList<>();
        List<OrderLine> orderLines = new ArrayList<>();
        int createCount = 0;
//        final int t = (warehouseCount * districtCount * customerCount);
        final int t = (1 * districtCount * customerCount);
        List<Order> orderList = new ArrayList<>();
//        for (int w = 1; w <= warehouseCount; w++) {
            int w = 1;
            for (int d = 1; d <= districtCount; d++) {
                for (int c = 1; c <= customerCount; c++) {
                    int o_c_id = TpccUtil.randomNumber(1, customerCount, gen);
                    final Order oorder = new Order(w, d, o_c_id, c);
//                    oorder.setId(Row.genObjId("Order", w, d, o_c_id, c));
//                    oorder.setId(new ObjId(objIdCount.getAndIncrement()));
                    oorder.o_c_id = o_c_id;
                    oorder.o_carrier_id = TpccUtil.randomNumber(1, 10, gen);
                    oorder.o_ol_cnt = TpccUtil.randomNumber(5, 15, gen);
                    oorder.o_all_local = 1;
                    oorder.o_entry_d = System.currentTimeMillis();
                    orders.add(oorder);
                    orderList.add(oorder);

                    // 900 rows in the NEW-ORDER table corresponding to the last
                    // 900 rows in the ORDER table for that district (i.e., with
                    // NO_O_ID between 2,101 and 3,000) (70%)
                    if (oorder.o_id > customerCount * 0.7) {
                        NewOrder new_order = new NewOrder(oorder.o_w_id, oorder.o_d_id, oorder.o_id);
//                        new_order.setId(Row.genObjId("NewOrder", oorder.o_w_id, oorder.o_d_id, oorder.o_id));
//                        new_order.setId(new ObjId(objIdCount.getAndIncrement()));
                        newOrders.add(new_order);
                    }

                    if (++createCount == t) {
                        int a = 0;
                        for (Order tmp : orderList) {
                            a += tmp.o_ol_cnt;
                        }
                        final int totalOderLine = a;
                        for (Order order : orderList) {
                            for (int l = 1; l <= order.o_ol_cnt; l++) {
                                OrderLine order_line = new OrderLine(order.o_w_id, order.o_d_id, order.o_id, l);
//                                order_line.setId(Row.genObjId("OrderLine", order.o_w_id, order.o_d_id, order.o_id, l));
//                                order_line.setId(new ObjId(objIdCount.getAndIncrement()));
                                order_line.ol_i_id = TpccUtil.randomNumber(1, itemCount, gen);
                                order_line.ol_delivery_d = order.o_entry_d;

                                if (order_line.ol_o_id < customerCount * 0.7) {
                                    order_line.ol_amount = 0;
                                } else {
                                    // random within [0.01 .. 9,999.99]
                                    order_line.ol_amount = (float) (TpccUtil.randomNumber(1, 999999, gen) / 100.0);
                                }

                                order_line.ol_supply_w_id = TpccUtil.randomNumber(1, warehouseCount, gen);
                                order_line.ol_quantity = 5;
                                order_line.ol_dist_info = TpccUtil.randomStr(24);
                                orderLines.add(order_line);
                            } // end for [l]
                        }
                    }
                } // end for [c]
            } // end for [d]
//        } // end for [w]
        ret.put("orders", orders);
        ret.put("newOrders", newOrders);
        ret.put("orderLines", orderLines);
        return ret;
    }



//    public static Map<String, Object> loadCSVData(String filePath) {
//        Map<String, Object> ret = new HashMap<>();
//        Set<Row> allObj = new HashSet<>();
//
//        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
//            for (String line; (line = br.readLine()) != null; ) {
//                String[] tmp = line.split(",");
//                if (tmp.length == 2) {
//                    if (tmp[0].equals("warehouseCount")) ret.put("warehouseCount", tmp[1]);
//                    if (tmp[0].equals("districtCount")) ret.put("districtCount", tmp[1]);
//                    if (tmp[0].equals("customerCount")) ret.put("customerCount", tmp[1]);
//                    if (tmp[0].equals("itemCount")) ret.put("itemCount", tmp[1]);
//                } else {
//                    switch (tmp[0]) {
//                        case "Header":
//                            ret.put(tmp[1], Arrays.copyOfRange(tmp, 1, tmp.length));
//                            break;
//                        case "Warehouse":
//                            Warehouse w = new Warehouse();
//                            w = w.fromCSV((String[]) ret.get("Warehouse"), tmp);
//                            allObj.add(w);
//                            break;
//                        case "Item":
//                            Item i = new Item();
//                            i = i.fromCSV((String[]) ret.get("Item"), tmp);
//                            allObj.add(i);
//                            break;
//                        case "Customer":
//                            Customer c = new Customer();
//                            c = c.fromCSV((String[]) ret.get("Customer"), tmp);
//                            allObj.add(c);
//                            break;
//                        case "Stock":
//                            Stock s = new Stock();
//                            s = s.fromCSV((String[]) ret.get("Stock"), tmp);
//                            allObj.add(s);
//                            break;
//                        case "District":
//                            District d = new District();
//                            d = d.fromCSV((String[]) ret.get("District"), tmp);
//                            allObj.add(d);
//                            break;
//                        case "History":
//                            History h = new History();
//                            h = h.fromCSV((String[]) ret.get("History"), tmp);
//                            allObj.add(h);
//                            break;
//                        case "Order":
//                            Order o = new Order();
//                            o = o.fromCSV((String[]) ret.get("Order"), tmp);
//                            allObj.add(o);
//                            break;
//                        case "NewOrder":
//                            NewOrder no = new NewOrder();
//                            no = no.fromCSV((String[]) ret.get("NewOrder"), tmp);
//                            allObj.add(no);
//                            break;
//                        case "OrderLine":
//                            OrderLine ol = new OrderLine();
//                            ol = ol.fromCSV((String[]) ret.get("OrderLine"), tmp);
//                            allObj.add(ol);
//                            break;
//                    }
//                }
//            }
//            ret.put("objs", allObj);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return ret;
//    }

//    public static void loadCSVData(String filePath, Callback callback) {
//        //read file into stream, try-with-resources
//        try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
//            stream.forEach(callback::callback);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }


//    public interface Callback {
//        void callback(String line);
//    }

}
