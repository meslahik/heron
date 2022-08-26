package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.models.AtomicVectorClock;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VectorClockTest {


  public static void testVectorClock() {
    AtomicVectorClock v1 = new AtomicVectorClock(0);
    AtomicVectorClock v2 = new AtomicVectorClock(1);
    AtomicVectorClock v3 = new AtomicVectorClock(2);
    System.out.println("Group="+0+" value="+0+ "get()="+v1.get());
    v1.incrementAndGet();
    System.out.println("Group="+0+" value="+1+ "get()="+v1.get());

    System.out.println("Group="+1+" value="+0+ "get()="+v2.get());
    v2.incrementAndGet();
    System.out.println("Group="+1+" value="+1+ "get()="+v2.get());

    System.out.println("Group="+2+" value="+0+ "get()="+v3.get());
    v3.incrementAndGet();
    System.out.println("Group="+2+" value="+1+ "get()="+v3.get());

    AtomicVectorClock v4 = AtomicVectorClock.parse(v3.get());
    System.out.println(v4.getGroupId()+" / "+v4.getValue());

    AtomicVectorClock v5 = AtomicVectorClock.parse(16843264);
    System.out.println(v5.getGroupId()+" / "+v5.getValue());
  }

  public static void main(String[] args) {
    testVectorClock();
  }
}
