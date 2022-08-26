package ch.usi.dslab.lel.dynastar.tpcc.tpcc;

//import ch.usi.dslab.lel.dynastarv2.probject.ObjId;


/**
 * Author: longle, created on 08/04/16.
 */
public class TpccDataPerfectPartitionerV3 {
//
//    String file;
//    int numPartition;
//
//    public TpccDataPerfectPartitionerV3(String file, int numPartition) {
//        this.file = file;
//        this.numPartition = numPartition;
//    }
//
//    public static void loadDataToCache(String dataFile, Callback callback) {
//        Map<String, String[]> ret = new HashMap<>();
//        TpccDataGenerator.loadCSVData(dataFile, line -> {
//            String[] tmp = line.split(",");
//            ObjId objId = null;
//            Map<String, Object> obj = null;
//            if (tmp.length == 2) {
//
//            } else {
//                switch (tmp[0]) {
//                    case "Header":
//                        ret.put(tmp[1], Arrays.copyOfRange(tmp, 1, tmp.length));
//                        if (callback != null) callback.callback(null, null, line);
//                        break;
//                    default:
//                        objId = new ObjId(Integer.parseInt(tmp[1]));
//                        obj = Row.csvToHashMap(ret.get(tmp[0]), tmp);
//                        break;
//                }
//            }
//            if (objId != null) {
//                obj.put("model", tmp[0]);
//                objId.setSId(Row.genSId(obj));
//                if (callback != null) callback.callback(objId, obj, line);
//            }
//        });
//    }
//
//    public static void main(String args[]) {
//        int index = 0;
//        System.out.println("Creating dataset for jTPCC. Enter required information");
//        Scanner scan = new Scanner(System.in);
//        String input;
////        int warehouseCount = Integer.parseInt(input);
////        System.out.print("Number of district per warehouse:");
////        input = scan.nextLine();
////        int districtCount = Integer.parseInt(input);
////        System.out.print("Number of customer per district:");
////        input = scan.nextLine();
////        int customerCount = Integer.parseInt(input);
////        System.out.print("Number of item:");
////        input = scan.nextLine();
////        int itemCount = Integer.parseInt(input);
//
//        System.out.print("Number of warehouses:");
//        input = scan.nextLine();
//        int warehouseCount = Integer.parseInt(input);
//
//        int districtCount = 10;
//
//        int customerCount = 20;
//
//        int itemCount = 100;
//
//
//        String file = "/Users/longle/Documents/Workspace/PhD/ScalableSMR/dynastarTPCC/bin/databasesV3/w_" + warehouseCount + "_d_" + districtCount + "_c_" + customerCount + "_i_" + itemCount + ".data";
////        String file = "/home/long/apps/ScalableSMR/dynastarTPCC/bin/databasesV3/w_" + warehouseCount + "_d_" + districtCount + "_c_" + customerCount + "_i_" + itemCount + ".data";
//        TpccDataPerfectPartitionerV3 app = new TpccDataPerfectPartitionerV3(file, warehouseCount);
//        app.split();
//    }
//
//    public void split() {
//        Path[] files = new Path[numPartition];
//        StringBuilder[] contents = new StringBuilder[numPartition];
//        StringBuilder oracleContent = new StringBuilder();
//        String[] tmp1 = this.file.split("/");
//        String tmp2 = tmp1[tmp1.length - 1] + ".oracle";
//        tmp1[tmp1.length - 1] = "partitioned/" + tmp2;
//        Path oraclePath = Paths.get(String.join("/", tmp1));
//        try {
//            for (int i = 0; i < numPartition; i++) {
//                String[] fileNamePart = this.file.split("/");
//                String fileoutName = fileNamePart[fileNamePart.length - 1] + "." + i;
//                fileNamePart[fileNamePart.length - 1] = "partitioned/" + fileoutName;
//                String filePath = String.join("/", fileNamePart);
//                contents[i] = new StringBuilder();
//                files[i] = Paths.get(filePath);
//
//            }
//            loadDataToCache(this.file, (objId, obj, line) -> {
//                if (objId == null && obj == null) {
//                    for (int i = 0; i < numPartition; i++) {
//                        contents[i].append(line + "\n");
//                    }
//                    oracleContent.append(line + "\n");
//                    return;
//                }
////                if (obj.get("model").equals("Item") || obj.get("model").equals("District") || obj.get("model").equals("Warehouse") || obj.get("model").equals("Customer")|| obj.get("model").equals("NewOrder")|| obj.get("model").equals("Order")|| obj.get("model").equals("OrderLine")) {
//                if (obj.get("model").equals("Item") || obj.get("model").equals("District") || obj.get("model").equals("Warehouse")) {
////                if (obj.get("model").equals("Item") || obj.get("model").equals("District") || obj.get("model").equals("Warehouse")) {
//                    for (int i = 0; i < numPartition; i++) {
//                        contents[i].append(line + "\n");
////                        System.out.println("partition " + i + " load " + obj);
////                        if (!obj.get("model").equals("Item") && !obj.get("model").equals("Customer") && !obj.get("model").equals("NewOrder") && !obj.get("model").equals("Order") && !obj.get("model").equals("OrderLine"))
//                    }
//                    if (!obj.get("model").equals("Item") && !obj.get("model").equals("Customer") && !obj.get("model").equals("NewOrder") && !obj.get("model").equals("Order") && !obj.get("model").equals("OrderLine"))
//                        oracleContent.append(line + "\n");
//                } else {
//                    int dest = mapIdToPartition(objId);
//                    contents[dest] = contents[dest].append(line + "\n");
////                    if (obj.get("model").equals("Customer")) {
////                        oracleContent.append(line + "\n");
////                    }
////                    System.out.println("partition " + dest + " load " + obj);
////                    if (obj.get("model").equals("NewOrder") || obj.get("model").equals("Order") || obj.get("model").equals("OrderLine")) {
////                        int destWarehouse = mapIdToWarehousePartition(objId);
////                        if (destWarehouse != dest) {
////                            contents[destWarehouse].append(line + "\n");
//////                            System.out.println("partition " + destWarehouse + " load " + obj);
////                        }
////                    }
//                }
//
//            });
//            Files.write(oraclePath, oracleContent.toString().getBytes());
//
//            for (int i = 0; i < numPartition; i++) {
//                String str = contents[i].toString();
//                byte[] bytes = str.getBytes();
//                System.out.println("Writing to file " + files[i] + " data length " + str.length() + " byte length=" + bytes.length);
//                Files.write(files[i], bytes);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    public int mapIdToPartition(ObjId objId) {
//        if (objId.getSId() == null) return objId.hashCode() % this.numPartition;
//        String parts[] = objId.getSId().split(":");
//        int ret = -1;
//        switch (parts[0]) {
//            case "Stock":
//            case "District":
//            case "History":
//            case "Order":
//            case "NewOrder":
//            case "OrderLine":
//            case "Customer":
//            case "Warehouse":
//                ret = Integer.parseInt(parts[1].split("=")[1]) % this.numPartition;
//                break;
//            case "Item":
//                ret = objId.hashCode() % this.numPartition;
//                break;
//            default:
//                ret = 0;
//                break;
//        }
//        return ret;
//
//    }
//
//    public int mapIdToWarehousePartition(ObjId objId) {
//        if (objId.getSId() == null) return objId.hashCode() % this.numPartition;
//        String parts[] = objId.getSId().split(":");
//        switch (parts[0]) {
//            case "Warehouse":
//                return Integer.parseInt(parts[1].split("=")[1]) % this.numPartition;
//            case "Stock":
//                return mapStockToDistrict(objId.sId) % this.numPartition;
////                return TpccConfig.defautDistrictForStock % this.numPartition;
//            case "District":
//            case "Customer":
//            case "History":
//            case "Order":
//            case "NewOrder":
//            case "OrderLine":
//                return Integer.parseInt(parts[1].split("=")[1]) % this.numPartition;
//            case "Item":
//                return objId.hashCode() % this.numPartition;
//            default:
//                return 0;
//        }
//
//    }
//
//    public interface Callback {
//        void callback(ObjId objId, Map<String, Object> obj, String line);
//    }
}
