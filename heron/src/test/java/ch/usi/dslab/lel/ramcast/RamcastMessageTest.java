package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.endpoint.CustomHandler;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RamcastMessageTest {
  static Logger logger = LoggerFactory.getLogger(RamcastMessageTest.class);
  static RamcastConfig config = RamcastConfig.getInstance();
  static Map<RamcastNode, RamcastAgent> agents;
  Semaphore lock;

  static int groups = 1;
  static int nodes = 3;

  @BeforeAll
  public static void setUp() throws Exception {
    //    Thread.sleep(10000);
    logger.info("Setting up for RamcastMessageTest");
    File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
    config = RamcastConfig.getInstance();
    config.loadConfig(configFile.getPath());

    List<RamcastAgent> tmp = new ArrayList<>();
    agents = new ConcurrentHashMap<>();
    List<Thread> threads = new ArrayList<>();

    for (int g = 0; g < groups; g++) {
      for (int p = 0; p < nodes; p++) {
        int finalP = p;
        int finalG = g;
        Thread t =
            new Thread(
                () -> {
                  try {
                    RamcastAgent agent = new RamcastAgent(finalG, finalP);
                    agents.put(RamcastNode.getNode(finalG, finalP), agent);
                    agent.bind();
                    agent.establishConnections();
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                });
        threads.add(t);
        t.start();
      }
    }
    for (Thread t : threads) {
      t.join();
    }
    logger.info("Setting up DONE");
    logger.info("=============================================");
  }

  @AfterAll
  public static void tearDown() throws IOException, InterruptedException {
    logger.info("Tearing Down");
    for (RamcastAgent agent : agents.values()) {
      agent.close();
    }
  }

  @BeforeEach
  public void beforeEach() {
    lock = new Semaphore(0);
  }

  //  @Test
  public void testCreatingMessage() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(128);
    buffer.putInt(100);
    buffer.putChar('A');
    buffer.putLong(10);
    List<RamcastGroup> dests = new ArrayList<>();
    dests.add(RamcastGroup.getGroup(0));
    RamcastMessage message = agents.get(RamcastNode.getNode(0, 0)).createMessage(buffer, dests);
    logger.debug("Created message: \n{}", message);
    assertEquals(1, message.getGroupCount());
    assertEquals(0, message.getGroup(0));

    if (groups == 1) return;
    dests = new ArrayList<>();
    dests.add(RamcastGroup.getGroup(0));
    dests.add(RamcastGroup.getGroup(1));
    message = agents.get(RamcastNode.getNode(0, 0)).createMessage(buffer, dests);
    logger.debug("Created message: \n{}", message);
    assertEquals(2, message.getGroupCount());
    assertEquals(0, message.getGroup(0));
    assertEquals(1, message.getGroup(1));
  }

//  @Test
  public void testWritingMessageSync() throws IOException, InterruptedException {

    RamcastAgent agent0 = agents.get(RamcastNode.getNode(0, 0));
    ByteBuffer buffer = ByteBuffer.allocateDirect(128);
    for (int i = 1; i < 100; i++) {
      buffer.clear();
      buffer.putInt(i);
      buffer.putChar('A');
      buffer.putLong(10);
      List<RamcastGroup> dests = new ArrayList<>();
      dests.add(RamcastGroup.getGroup(0));
      if (groups == 2) dests.add(RamcastGroup.getGroup(1));
      RamcastMessage message = agent0.createMessage(buffer, dests);
//      logger.debug("Created message: \n{}", message);
      assertEquals(groups, message.getGroupCount());
      assertEquals(0, message.getGroup(0));
      for (RamcastAgent agent : agents.values()) {
        agent
            .getEndpointGroup()
            .setCustomHandler(
                new CustomHandler() {
                  @Override
                  public void handleReceiveMessage(Object data) {
                    RamcastMessage msg = (RamcastMessage) data;
                    assertEquals(groups, msg.getGroupCount());
                    assertEquals(0, msg.getGroup(0));
                    assertEquals(128, msg.getMessageLength());
                    assertEquals(buffer.getInt(0), msg.getMessage().getInt(0));
                    try {
                      agent.getEndpointGroup().releaseMemory(msg);
                    } catch (IOException e) {
                      e.printStackTrace();
                    }
                    lock.release();
                  }
                });
      }

      agent0.multicast(message, dests);
      lock.acquire(groups * nodes);
    }
    Thread.sleep(1000);
  }

  @Test
  public void testWritingMessageAsync() throws IOException, InterruptedException {

    RamcastAgent agent0 = agents.get(RamcastNode.getNode(0, 0));
    ByteBuffer buffer = ByteBuffer.allocateDirect(128);
    for (int i = 1; i < 100; i++) {
      buffer.clear();
      buffer.putInt(i);
      buffer.putChar('A');
      buffer.putLong(10);
      List<RamcastGroup> dests = new ArrayList<>();
      dests.add(RamcastGroup.getGroup(0));
      if (groups == 2) dests.add(RamcastGroup.getGroup(1));
      RamcastMessage message = agent0.createMessage(buffer, dests);
      logger.debug("Created message: \n{}", message);
      assertEquals(groups, message.getGroupCount());
      assertEquals(0, message.getGroup(0));
      for (RamcastAgent agent : agents.values()) {
        agent
            .getEndpointGroup()
            .setCustomHandler(
                new CustomHandler() {
                  @Override
                  public void handleReceiveMessage(Object data) {
                    RamcastMessage msg = (RamcastMessage) data;
                    try {
                      agent.getEndpointGroup().releaseMemory(msg);
                    } catch (IOException e) {
                      e.printStackTrace();
                    }
//                    lock.release();
                  }
                });
      }

      agent0.multicast(message, dests);
//      lock.acquire(groups * nodes);
    }
    Thread.sleep(1000);
  }
}
