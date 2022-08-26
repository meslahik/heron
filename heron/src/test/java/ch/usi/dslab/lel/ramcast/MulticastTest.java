package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
import org.junit.jupiter.api.*;
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

public class MulticastTest {
  static Logger logger = LoggerFactory.getLogger(MulticastTest.class);
  static RamcastConfig config = RamcastConfig.getInstance();
  static Map<RamcastNode, RamcastAgent> agents;
  Semaphore lock;

  static int groups = 1;
  static int nodes = 6;

  @BeforeAll
  public static void setUp() throws Exception {
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

  @Test
  @Order(1)
  public void testWritingLocalTs() throws IOException, InterruptedException {

    RamcastAgent agent00 = agents.get(RamcastNode.getNode(0, 0));
    RamcastAgent agent01 = agents.get(RamcastNode.getNode(0, 1));
//    RamcastAgent agent10 = agents.get(RamcastNode.getNode(1, 0));
//    RamcastAgent agent11 = agents.get(RamcastNode.getNode(1, 1));
    ByteBuffer buffer = ByteBuffer.allocateDirect(12);
    for (int i = 1; i < 6; i++) {
      buffer.clear();
      buffer.putInt(i);
      buffer.putInt(i + 1);
      buffer.putInt(i + 2);
      List<RamcastGroup> dests = new ArrayList<>();
      dests.add(RamcastGroup.getGroup(0));
      if (groups == 2) dests.add(RamcastGroup.getGroup(1));
      RamcastMessage message = agent00.createMessage(i, buffer, dests);
      logger.debug("Created message: \n{}", message);
      assertEquals(groups, message.getGroupCount());
      assertEquals(0, message.getGroup(0));

      agent00.multicast(message, dests);
      Thread.sleep(100);
    }

    Thread.sleep(10000);
    for (RamcastAgent agent : agents.values()) {
      logger.debug(
          "agent {} Processing: {}",
          agent.getNode(),
          agent.getEndpointGroup().getMessageProcessor().getProcessing());
    }
  }
}
