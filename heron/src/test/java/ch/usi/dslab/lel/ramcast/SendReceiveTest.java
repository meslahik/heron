package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.endpoint.CustomHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SendReceiveTest {
  static Logger logger = LoggerFactory.getLogger(SendReceiveTest.class);
  static RamcastConfig config = RamcastConfig.getInstance();
  static ByteBuffer buffer;
  static List<RamcastAgent> agents;
  Semaphore sendPermits;

  @BeforeAll
  public static void setUp() throws Exception {
    logger.info("Setting up for SendReceiveTest");
    int groups = 1;
    int nodes = 2;
    File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
    config = RamcastConfig.getInstance();
    config.loadConfig(configFile.getPath());

    buffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_MESSAGE);
    buffer.putInt(10);
    buffer.putInt(11);
    buffer.putInt(12);
    buffer.putChar('A');

    List<RamcastAgent> tmp = new ArrayList<>();
    agents = Collections.synchronizedList(tmp);
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
                    agents.add(agent);
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
    for (RamcastAgent agent : agents) {
      agent.close();
    }
  }

  @Test
  public void testSend() throws IOException {
    agents
        .get(0)
        .getEndpointGroup()
        .setCustomHandler(
            new CustomHandler() {
              @Override
              public void handleSendComplete(Object data) {
                assertTrue(
                    buffer.getInt(0) == ((ByteBuffer) data).getInt(0)
                        && buffer.getInt(4) == ((ByteBuffer) data).getInt(4)
                        && buffer.getInt(8) == ((ByteBuffer) data).getInt(8)
                        && buffer.getChar(12) == ((ByteBuffer) data).getChar(12));
              }
            });
    agents.get(0).getEndpointGroup().send(agents.get(1).getNode(), buffer);
    agents.get(0).getEndpointGroup().send(agents.get(1).getNode(), buffer);
    agents
        .get(0)
        .getEndpointGroup()
        .send(agents.get(1).getNode().getGroupId(), agents.get(1).getNode().getNodeId(), buffer);
  }

  @Test
  public void testRecive() throws IOException {
    agents
        .get(1)
        .getEndpointGroup()
        .setCustomHandler(
            new CustomHandler() {
              @Override
              public void handleReceive(Object data) {
                assertTrue(
                    buffer.getInt(0) == ((ByteBuffer) data).getInt(0)
                        && buffer.getInt(4) == ((ByteBuffer) data).getInt(4)
                        && buffer.getInt(8) == ((ByteBuffer) data).getInt(8)
                        && buffer.getChar(12) == ((ByteBuffer) data).getChar(12));
              }
            });
    agents.get(0).getEndpointGroup().send(agents.get(1).getNode(), buffer);
    agents.get(0).getEndpointGroup().send(agents.get(1).getNode(), buffer);
    agents
        .get(0)
        .getEndpointGroup()
        .send(agents.get(1).getNode().getGroupId(), agents.get(1).getNode().getNodeId(), buffer);
  }

  void getPermit() {
    try {
      sendPermits.acquire();
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void releasePermit() {
    sendPermits.release();
  }

  @Test
  public void testMassSend() throws IOException, InterruptedException {
    sendPermits = new Semaphore(1);
    AtomicInteger send = new AtomicInteger(0);
    AtomicInteger receive = new AtomicInteger(0);
    ByteBuffer response = ByteBuffer.allocateDirect(RamcastConfig.SIZE_MESSAGE);
    response.putInt(100);
    response.putInt(111);
    response.putInt(122);
    response.putChar('A');

    agents
        .get(1)
        .getEndpointGroup()
        .setCustomHandler(
            new CustomHandler() {
              @Override
              public void handleReceive(Object data) {
                try {
                  assertTrue(
                      buffer.getInt(0) == ((ByteBuffer) data).getInt(0)
                          && buffer.getInt(4) == ((ByteBuffer) data).getInt(4)
                          && buffer.getInt(8) == ((ByteBuffer) data).getInt(8)
                          && buffer.getChar(12) == ((ByteBuffer) data).getChar(12));
                  receive.getAndIncrement();
                  agents.get(1).getEndpointGroup().send(agents.get(0).getNode(), response);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            });
    agents
        .get(0)
        .getEndpointGroup()
        .setCustomHandler(
            new CustomHandler() {
              @Override
              public void handleReceive(Object data) {
                assertTrue(
                    response.getInt(0) == ((ByteBuffer) data).getInt(0)
                        && response.getInt(4) == ((ByteBuffer) data).getInt(4)
                        && response.getInt(8) == ((ByteBuffer) data).getInt(8)
                        && response.getChar(12) == ((ByteBuffer) data).getChar(12));

                releasePermit();
              }

              @Override
              public void handleSendComplete(Object data) {
                assertTrue(
                    buffer.getInt(0) == ((ByteBuffer) data).getInt(0)
                        && buffer.getInt(4) == ((ByteBuffer) data).getInt(4)
                        && buffer.getInt(8) == ((ByteBuffer) data).getInt(8)
                        && buffer.getChar(12) == ((ByteBuffer) data).getChar(12));
                send.getAndIncrement();
              }
            });
    logger.info("Going to run send-receive test for 1 seconds");
    long end = System.currentTimeMillis() + 1 * 1000; // going to run this experiment for 20 secs
    while (System.currentTimeMillis() < end) {
      getPermit();
      agents
          .get(0)
          .getEndpointGroup()
          .send(agents.get(1).getNode().getGroupId(), agents.get(1).getNode().getNodeId(), buffer);
    }
    getPermit(); // to wait for the last response
    //        Thread.sleep(500); // wait for the remaining callback
    assertEquals(send.get(), receive.get());
    logger.info("Sent {} packages", send.get() + receive.get());
  }
}
