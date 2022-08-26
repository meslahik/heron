package ch.usi.dslab.lel.dynastar.tpcc;

import ch.usi.dslab.bezerra.netwrapper.codecs.Codec;
import ch.usi.dslab.bezerra.netwrapper.codecs.CodecUncompressedKryo;
//import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
//import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class MyObject implements Serializable {
//    int value = 0;
//
//    public MyObject(){
//
//    }
//
//    public MyObject(int value) {
//        this.value = value;
//    }
//
//    @Override
//    public String toString() {
//        return String.valueOf(this.value);
//    }
}

public class TpccDataLoader {
//    static Codec codec = new CodecUncompressedKryo();
//    //    public PRObjectGraph objectGraph = new PRObjectGraph(-1);
//    protected Map<String, Set<ObjId>> secondaryIndex = new ConcurrentHashMap<>();
//
//    public static void main(String[] args) throws Exception {
//
//        int index = 0;
//        String host = args[index++];
//        int port = Integer.parseInt(args[index++]);
//        String dataFile = args[index++];
//
//        Jedis jedis = new Jedis("localhost");
//        jedis.set("foo", "bar");
//        MyObject objectToCache = new MyObject(1);
//        jedis.set("keyObject", codec.getString(objectToCache));
//
//        MyObject obj = (MyObject) codec.createObjectFromString(jedis.get("keyObject"));
//        System.out.println(obj);
//
////        MemcachedClient memcacheClient = new MemcachedClient(new InetSocketAddress(host, port));
////
////
////        memcacheClient.set("keyObject", 3600, objectToCache);
//////
////        MyObject startDate = new MyObject(2);
////        memcacheClient.set("keyDate", 3600, startDate);
////
////        MyObject obj = (MyObject) memcacheClient.get("keyObject");
////        System.out.println("keyObject " + obj);
////
////        MyObject obj2 = (MyObject) memcacheClient.get("keyDate");
////        System.out.println("keyDate " + obj2);
//    }
}

