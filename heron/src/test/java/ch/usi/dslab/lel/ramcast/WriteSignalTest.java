//package ch.usi.dslab.lel.ramcast;
//
//import ch.usi.dslab.lel.ramcast.endpoint.CustomHandler;
//import org.junit.jupiter.api.AfterAll;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.IOException;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.List;
//import java.util.concurrent.Semaphore;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//public class WriteSignalTest {
//  static Logger logger = LoggerFactory.getLogger(WriteMessageTest.class);
//  static RamcastConfig config = RamcastConfig.getInstance();
//  static ByteBuffer buffer;
//  static List<RamcastAgent> agents;
//  Semaphore sendPermits;
//  Semaphore lock;
//
//  @BeforeAll
//  public static void setUp() throws Exception {
//    logger.info("Setting up for WriteSignalTest");
//    int groups = 1;
//    int nodes = 3;
//    File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
//    config = RamcastConfig.getInstance();
//    config.loadConfig(configFile.getPath());
//
//    List<RamcastAgent> tmp = new ArrayList<>();
//    agents = Collections.synchronizedList(tmp);
//    List<Thread> threads = new ArrayList<>();
//
//    for (int g = 0; g < groups; g++) {
//      for (int p = 0; p < nodes; p++) {
//        int finalP = p;
//        int finalG = g;
//        Thread t =
//                new Thread(
//                        () -> {
//                          try {
//                            RamcastAgent agent = new RamcastAgent(finalG, finalP);
//                            agents.add(agent);
//                            agent.bind();
//                            agent.establishConnections();
//                          } catch (Exception e) {
//                            e.printStackTrace();
//                          }
//                        });
//        threads.add(t);
//        t.start();
//      }
//    }
//    for (Thread t : threads) {
//      t.join();
//    }
//    agents.sort(Comparator.comparingInt(agents -> agents.getNode().getOrderId()));
//    logger.info("Setting up DONE");
//    logger.info("=============================================");
//  }
//
//  @AfterAll
//  public static void tearDown() throws IOException, InterruptedException {
//    logger.info("Tearing Down");
//    for (RamcastAgent agent : agents) {
//      agent.close();
//    }
//  }
//
//  @BeforeEach
//  public void beforeEach() {
//    sendPermits = new Semaphore(1);
//    lock = new Semaphore(0);
//  }
//
//  void getPermit() {
//    try {
//      sendPermits.acquire();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//      System.exit(1);
//    }
//  }
//
//  public void releasePermit() {
//    sendPermits.release();
//  }
//
//  //  @Test
//  public void testWriteServerHead() throws IOException, InterruptedException {
//    RamcastAgent agent0 = agents.get(0);
//    RamcastAgent agent1 = agents.get(1);
//    long end = System.currentTimeMillis() + 1000;
//    while (System.currentTimeMillis() < end) {
//      agents
//              .get(0)
//              .getEndpointGroup()
//              .writeRemoteHeadOnClient(agent0.getEndpointMap().get(agent1.getNode()), 10, 0);
//    }
//  }
//
//  //  @Test
//  public void testWriteTimestamp() throws IOException, InterruptedException {
//    RamcastAgent agent0 = agents.get(0);
//    RamcastAgent agent1 = agents.get(1);
//    long end = System.currentTimeMillis() + 1000;
//    int slot = 0;
//    int ballot = 1;
//    for (int i = 0; i < RamcastConfig.getInstance().getQueueLength(); i++) {
//      agent0
//              .getEndpointGroup()
//              .writeTimestamp(
//                      agent0.getEndpointMap().get(agent1.getNode()),
//                      slot,
//                      0,
//                      ballot,
//                      ballot + 1);
//      Thread.sleep(10);
//      assertTrue(
//              agent1
//                      .getEndpointGroup()
//                      .getSharedTimestampBuffer()
//                      .getInt(slot * RamcastConfig.SIZE_TIMESTAMP)
//                      == ballot
//                      && agent1
//                      .getEndpointGroup()
//                      .getSharedTimestampBuffer()
//                      .getInt(slot * RamcastConfig.SIZE_TIMESTAMP + 4)
//                      == ballot + 1
//                      && agent1
//                      .getEndpointGroup()
//                      .getSharedTimestampBuffer()
//                      .getInt(slot * RamcastConfig.SIZE_TIMESTAMP + 8)
//                      == ballot + 2);
//      slot++;
//      ballot++;
//    }
//  }
//
//  @Test
//  public void testWriteTimestampWithPermission() throws IOException, InterruptedException {
//    RamcastAgent agent0 = agents.get(0);
//    RamcastAgent agent1 = agents.get(1);
//    RamcastAgent agent2 = agents.get(2);
//    long end = System.currentTimeMillis() + 1000;
//    int slot = 0;
//    int ballot = 1;
//
//    // by default, agent0 should be able to write ts to agent, not the other way around
//    agent0
//            .getEndpointGroup()
//            .writeTimestamp(
//                    agent0.getEndpointMap().get(agent1.getNode()), slot, 0, ballot, ballot + 1);
//    Thread.sleep(10);
//    agent0
//            .getEndpointGroup()
//            .writeTimestamp(
//                    agent0.getEndpointMap().get(agent2.getNode()), slot, 0, ballot, ballot + 1);
//    Thread.sleep(10);
//    assertTrue(
//            agent1
//                    .getEndpointGroup()
//                    .getSharedTimestampBuffer()
//                    .getInt(slot * RamcastConfig.SIZE_TIMESTAMP)
//                    == ballot
//                    && agent1
//                    .getEndpointGroup()
//                    .getSharedTimestampBuffer()
//                    .getInt(slot * RamcastConfig.SIZE_TIMESTAMP + 4)
//                    == ballot + 1
//                    && agent1
//                    .getEndpointGroup()
//                    .getSharedTimestampBuffer()
//                    .getInt(slot * RamcastConfig.SIZE_TIMESTAMP + 8)
//                    == ballot + 2);
//    assertTrue(
//            agent2
//                    .getEndpointGroup()
//                    .getSharedTimestampBuffer()
//                    .getInt(slot * RamcastConfig.SIZE_TIMESTAMP)
//                    == ballot
//                    && agent2
//                    .getEndpointGroup()
//                    .getSharedTimestampBuffer()
//                    .getInt(slot * RamcastConfig.SIZE_TIMESTAMP + 4)
//                    == ballot + 1
//                    && agent2
//                    .getEndpointGroup()
//                    .getSharedTimestampBuffer()
//                    .getInt(slot * RamcastConfig.SIZE_TIMESTAMP + 8)
//                    == ballot + 2);
//
//    // Now we change the leader
//    agent0.setLeader(agent1.getNode().getGroupId(), agent1.getNode().getNodeId());
//    agent1.setLeader(agent1.getNode().getGroupId(), agent1.getNode().getNodeId());
//    agent2.setLeader(agent1.getNode().getGroupId(), agent1.getNode().getNodeId());
//
//    // then leader request permission
//    agent1.getEndpointGroup().requestWritePermission();
//
//    // check error of the write of old leader
//    agent0
//            .getEndpointGroup()
//            .setCustomHandler(
//                    new CustomHandler() {
//                      @Override
//                      public void handlePermissionError(ByteBuffer buffer) {
//                        assertEquals(buffer.getInt(0), ballot);
//                        lock.release();
//                      }
//                    });
//    // old leader should not be able to write
//    agent0
//            .getEndpointGroup()
//            .writeTimestamp(
//                    agent0.getEndpointMap().get(agent1.getNode()), slot, 0, ballot, ballot + 1);
//    // old leader should not be able to write
//    agent0
//            .getEndpointGroup()
//            .writeTimestamp(
//                    agent0.getEndpointMap().get(agent2.getNode()), slot, 0, ballot, ballot + 1);
//
//    lock.acquire(2);
//
//    // new leader should be able to write
//    // check error of the write of new leader
//    agent1
//            .getEndpointGroup()
//            .setCustomHandler(
//                    new CustomHandler() {
//                      @Override
//                      public void handlePermissionError(ByteBuffer buffer) {
//                        // should not come here
//                        fail();
//                      }
//                    });
//    // old leader should not be able to write
//    agent1
//            .getEndpointGroup()
//            .writeTimestamp(
//                    agent1.getEndpointMap().get(agent0.getNode()), slot, 0, ballot, ballot + 1);
//    // old leader should not be able to write
//    agent1
//            .getEndpointGroup()
//            .writeTimestamp(
//                    agent1.getEndpointMap().get(agent2.getNode()), slot, 0, ballot, ballot + 1);
//    Thread.sleep(1000);
//  }
//}
