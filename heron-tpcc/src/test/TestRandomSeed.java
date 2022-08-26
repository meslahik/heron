import org.junit.Test;

import java.util.Random;

public class TestRandomSeed {

    @Test
    public void TestRandomSeed(){
        long seed = 1;
        Random ran = new Random(seed);
        System.out.println(ran.nextInt());
        System.out.println(ran.nextInt());
    }
}
