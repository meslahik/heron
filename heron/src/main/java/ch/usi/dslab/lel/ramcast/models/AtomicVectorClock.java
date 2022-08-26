package ch.usi.dslab.lel.ramcast.models;

import ch.usi.dslab.lel.ramcast.RamcastAgent;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicVectorClock {
  private static final int SIZE_GROUP_ID = 1;  // value is int 4 bytes: 1 byte identifies group => right-most byte
  private static final int SIZE_CLOCK_VALUE = 3;  // 3 bytes identify value
  private int groupId;
  private AtomicInteger value;
  private AtomicInteger latest;
  private RamcastAgent agent;

  public AtomicVectorClock(RamcastAgent agent, int value) {
    this.groupId = agent.getGroupId();
    this.value = new AtomicInteger(value);
    this.latest = new AtomicInteger(value);
    this.agent = agent;
  }

  public AtomicVectorClock(int groupId, int value) {
    this.groupId = groupId;
    this.value = new AtomicInteger(value);
    this.latest = new AtomicInteger(value);
  }

  public AtomicVectorClock(int groupId) {
    this(groupId, 0);
  }

  public static AtomicVectorClock parse(int clock) {
    // right-most byte identifies group
    // following zeros bits on the left of right-most byte => gives group
    // clock << 24 >> 24  => 0 0 0 byte

    // following zeros bits in right-most byte => gives clock value
    // clock >> 8  => byte byte byte 0
    return new AtomicVectorClock(clock << 8 * SIZE_CLOCK_VALUE >> 8 * SIZE_CLOCK_VALUE, clock >> 8 * SIZE_GROUP_ID);
  }

  public int get() {
    return (value.get() << 8 * SIZE_GROUP_ID) | groupId;
  }

  public int get(int v) {
    return (v << 8 * SIZE_GROUP_ID) | groupId;
  }

  public int getGroupId() {
    return groupId;
  }

  public int getValue() {
    return value.get();
  }

  public boolean compareAndSet(int expect, int newValue) {
//    if (this.agent!=null && this.agent.isLeader()) System.out.println("set value=" + value + " = " + (value >> 8 * 1));
//    this.latest.set(value >> 8 * 1);
//    this.value.set(value >> 8 * 1);

    return this.value.compareAndSet(expect >> 8 * SIZE_GROUP_ID, newValue >> 8 * SIZE_GROUP_ID);
  }

  public int incrementAndGet() {
//    int latest = this.latest.get();
    int v = this.value.incrementAndGet();
//    if (this.agent!=null && this.agent.isLeader()) System.out.println("Group=" + groupId + " latest=" + latest + " value=" + value);
//    while (v <= latest) {
//      latest = this.latest.get();
//      v = this.value.incrementAndGet();
//    }
//    this.latest.set(v);
    return this.get(v);
  }
}

/*

int k = 5;
int x = 3;


                  +---+---+---+
                k:| 1 | 0 | 1 |
                  +---+---+---+
                  +---+---+---+
                x:| 0 | 1 | 1 |
                  +---+---+---+

      +---+---+---+---+---+---+
k<<=3:| 1 | 0 | 1 | 0 | 0 | 0 |
      +---+---+---+---+---+---+
                  +---+---+---+
                x:| 0 | 1 | 1 |
                  +---+---+---+

      +---+---+---+---+---+---+
 k|=3:| 1 | 0 | 1 | 0 | 1 | 1 |
      +---+---+---+---+---+---+
                    ^   ^   ^
                  +---+---+---+
                x:| 0 | 1 | 1 |
                  +---+---+---+
 */