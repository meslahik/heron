import ch.usi.dslab.bezerra.netwrapper.codecs.Codec;
import ch.usi.dslab.bezerra.netwrapper.codecs.CodecUncompressedKryo;
import org.junit.Test;
import redis.clients.jedis.BinaryJedis;

import java.io.Serializable;

public class TestBinaryJedis {
    static Codec codec = new CodecUncompressedKryo();
    @Test
    public void testBinaryJedis() {
        String redisHost = "localhost";
        BinaryJedis jedis = new BinaryJedis(redisHost);
        String key = "mykey";
        MyObject objectToCache = new MyObject(1);
        jedis.set(key.getBytes(), codec.getBytes(objectToCache));

        MyObject obj = (MyObject) codec.createObjectFromBytes(jedis.get(key.getBytes()));
        System.out.println(obj);
    }
}


class MyObject implements Serializable {
    int value = 0;

    public MyObject(){

    }

    public MyObject(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return String.valueOf(this.value);
    }
}