package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.bezerra.netwrapper.codecs.Codec;
import ch.usi.dslab.bezerra.netwrapper.codecs.CodecUncompressedKryo;
import ch.usi.dslab.lel.dynastar.tpcc.tpcc.TpccUtil;
//import ch.usi.dslab.lel.dynastarv2.OracleStateMachine;
//import ch.usi.dslab.lel.dynastarv2.Partition;
//import ch.usi.dslab.lel.dynastarv2.command.Command;
//import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
//import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
//import ch.usi.dslab.lel.dynastarv2.probject.PRObjectGraph;
//import ch.usi.dslab.lel.dynastarv2.probject.PRObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import redis.clients.jedis.BinaryJedis;
//import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TpccOracle { //extends OracleStateMachine {

//    public static final Logger log = LoggerFactory.getLogger(TpccOracle.class);
//
//    private int repartitioningThreshold = 3;
//    private long repartitioningInterval = 150000;
//
//
//    // need 24s for loading data for 2, 15s for cache
//    // 71s for 4, 20s for cache
//    // 100s for 8, cache server2 26
//    // cache server 1 45
//
////    60000 for 2,4
////    90000 for 8
////    120000 for 16
//
//    public TpccOracle(int serverId, String systemConfig, String partitionsConfig, TpccProcedure appProcedure) {
//        super(serverId, systemConfig, partitionsConfig, appProcedure);
//        this.setRepartitioningThreshold(0); // no dynamic
//        this.setRepartitioningInterval(repartitioningInterval);
//        this.setRepartitioningLimit(1);
//        this.setHyperGraph(true);
//
//        this.setRepeatingPartitioning(false);
//    }
//
//    public static void main(String args[]) {
//        String systemConfigFile;
//        String partitionConfigFile;
//        String database;
//        int oracleId;
//        if (args.length == 9) {
//            int argIndex = 0;
//            oracleId = Integer.parseInt(args[argIndex++]);
//            systemConfigFile = args[argIndex++];
//            partitionConfigFile = args[argIndex++];
//            database = args[argIndex++];
//
//            String gathererHost = args[argIndex++];
//            int gathererPort = Integer.parseInt(args[argIndex++]);
//            String gathererDir = args[argIndex++];
//            int gathererDuration = Integer.parseInt(args[argIndex++]);
//            int gathererWarmup = Integer.parseInt(args[argIndex++]);
//            TpccProcedure appProcedure = new TpccProcedure();
//            TpccOracle oracle = new TpccOracle(oracleId, systemConfigFile, partitionConfigFile, appProcedure);
//            oracle.preLoadData(database, gathererHost);
//            appProcedure.init("ORACLE", oracle.objectGraph, oracle.secondaryIndex, logger, oracle.partitionId);
//            System.out.println("Oracle DynaStar TPCC, Sample data loaded...");
//            oracle.setupMonitoring(gathererHost, gathererPort, gathererDir, gathererDuration, gathererWarmup);
//            oracle.runStateMachine();
//        } else {
//            System.out.print("Usage: <oracleId> <system config> <partition config> <database> <host> <port> <logDir> <duration> <warmup>");
//            System.exit(0);
//        }
//    }
//
//    @Override
//    protected void preprocessCommand(Command command) {
//
//    }
//
//    public void preLoadData(String file, String redisHost) {
//        String hostName = null;
//        try {
//            hostName = InetAddress.getLocalHost().getHostName();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        if (hostName.indexOf("node") == 0) {
//            redisHost = "192.168.3.45";
//        } else if (hostName.indexOf("Long") == 0) {
//            redisHost = "127.0.0.1";
//        } else {
//            redisHost = "172.31.42.68";
//        }
//        boolean cacheLoaded = false;
//        long start = System.currentTimeMillis();
//        String[] fileNameParts = file.split("/");
//        String fileName = fileNameParts[fileNameParts.length - 1];
//        Integer partitionCount = Partition.getPartitionsCount();
//        System.out.println("Creating connection to redis host " + redisHost);
//        BinaryJedis jedis = new BinaryJedis(redisHost, 6379, 600000);
//        Codec codec = new CodecUncompressedKryo();
//        boolean redisAvailabled = true;
//        byte[] keyObjectGraph = (fileName + "_p_" + partitionCount + "_ORACLE_" + this.partitionId + "_objectGraph").getBytes();
//        byte[] keySecondaryIndex = (fileName + "_p_" + partitionCount + "_ORACLE_" + this.partitionId + "_secondaryIndex").getBytes();
//        byte[] keyDataLoaded = (fileName + "_p_" + partitionCount + "_ORACLE_" + this.partitionId + "_data_loaded").getBytes();
//        try {
//            byte[] cached = jedis.get(keyDataLoaded);
//            if (cached != null && new String(cached).equals("OK")) {
//                System.out.println("[ORACLE" + this.partitionId + "] loading sample data from cache..." + System.currentTimeMillis());
//                this.objectGraph = (PRObjectGraph) codec.createObjectFromBytes(jedis.get(keyObjectGraph));
//                this.secondaryIndex = (ConcurrentHashMap<String, Set<ObjId>>) codec.createObjectFromBytes(jedis.get(keySecondaryIndex));
//                this.objectGraph.setLogger(this.logger);
//                cacheLoaded = true;
//            }
//        } catch (JedisConnectionException e) {
//            log.info("[ORACLE" + this.partitionId + "] Redis Cache is not available. Loading from file");
//            redisAvailabled = false;
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.exit(-1);
//        }
//
//        if (!cacheLoaded) {
//            System.out.println("[ORACLE" + this.partitionId + "] loading sample data from file..." + System.currentTimeMillis());
//            TpccUtil.loadDataToCache(file, this.objectGraph, this.secondaryIndex, (objId, obj) -> {
//                int dest = TpccUtil.mapIdToPartition(objId);
//                String modelName = (String) obj.get("model");
//                if (!modelName.equals("District") && !modelName.equals("Warehouse")) {
//                    //do nothing
//                } else {
//                    PRObjectNode node = new PRObjectNode(objId, dest);
//                    this.objectGraph.addNode(node);
//                }
//            });
//            if (redisAvailabled) {
//                jedis.set(keyObjectGraph, codec.getBytes(this.objectGraph));
//                jedis.set(keySecondaryIndex, codec.getBytes(this.secondaryIndex));
//                jedis.set(keyDataLoaded, new String("OK").getBytes());
//            }
//
//        }
//
//        System.out.println("[ORACLE" + this.partitionId + "] Data loaded, takes " + (System.currentTimeMillis() - start));
//
//    }
//
//    @Override
//    public PRObject createObject(ObjId id, Object value) {
//        return null;
//    }
}

