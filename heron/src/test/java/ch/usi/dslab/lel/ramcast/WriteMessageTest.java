package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.endpoint.CustomHandler;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class WriteMessageTest {
  static Logger logger = LoggerFactory.getLogger(WriteMessageTest.class);
  static RamcastConfig config = RamcastConfig.getInstance();
  static ByteBuffer buffer;
  static List<RamcastAgent> agents;
  Semaphore sendPermits;
  Semaphore lock;

  @BeforeAll
  public static void setUp() throws Exception {
    logger.info("Setting up for WriteMessageTest");
    int groups = 1;
    int nodes = 2;
    File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
    config = RamcastConfig.getInstance();
    config.loadConfig(configFile.getPath());

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

  @BeforeEach
  public void beforeEach() {
    sendPermits = new Semaphore(1);
    lock = new Semaphore(0);
  }

  @AfterEach
  public void afterEach() {
    logger.info("=============================================");
  }

  @Test
  @Order(1)
  @DisplayName("Should not be able to write big message (> SIZE_MESSAGE)")
  public void testWriteBigMessage() {
    buffer = ByteBuffer.allocateDirect(249);
    buffer.putInt(10);
    buffer.putInt(11);
    buffer.putInt(12);
    buffer.putChar('A');
    Exception exception =
        assertThrows(
            IOException.class,
            () -> agents.get(0).getEndpointGroup().writeMessage(agents.get(1).getNode(), buffer));
    String msg = exception.getMessage();
    assertEquals("Buffer size of [249] is too big. Only allow 248", msg);
  }

  @Test
  public void testPutToBuffer() {
    RamcastEndpoint endpoint = agents.get(1).getEndpointMap().get(agents.get(0).getNode());
    ByteBuffer buffer = ByteBuffer.allocateDirect(16);
    int length = 0;
    length += endpoint.putToBuffer(buffer, Integer.TYPE, 1);
    length += endpoint.putToBuffer(buffer, Character.TYPE, 'A');
    length += endpoint.putToBuffer(buffer, Long.TYPE, (long) 2);
    length += endpoint.putToBuffer(buffer, Short.TYPE, (short) 3);
    assertEquals(16, length);
    assertEquals(1, buffer.getInt(0));
    assertEquals('A', buffer.getChar(4));
    assertEquals(2, buffer.getLong(6));
    assertEquals(3, buffer.getShort(14));
  }

  @Test
  @Order(2)
  @DisplayName(
      "Should be able to write a message to remote side. Remote delivers/releases memory/updates back head pointer")
  public void testWriteMessage() throws IOException, InterruptedException {
    buffer = ByteBuffer.allocateDirect((RamcastConfig.SIZE_MESSAGE - RamcastConfig.SIZE_BUFFER_LENGTH));
    buffer.putInt(10);
    buffer.putInt(11);
    buffer.putInt(12);
    buffer.putChar('A');
    // no head update from remote side => -1
    assertEquals(
        RamcastConfig.getInstance().getQueueLength(),
        agents.get(1).getEndpointMap().get(agents.get(0).getNode()).getAvailableSlots());
    // nothing has been written here => 0
    assertEquals(
        0,
        agents
            .get(1)
            .getEndpointMap()
            .get(agents.get(0).getNode())
            .getRemoteCellBlock()
            .getTailOffset());

    final int[] tailOffset = {0};
    agents
        .get(1)
        .getEndpointGroup()
        .setCustomHandler(
            new CustomHandler() {
              @Override
              public void handleReceiveMessage(Object data) {
                ByteBuffer req = ((RamcastMessage) data).getBuffer();
                assertTrue(
                    buffer.getInt(0) == req.getInt(0)
                        && buffer.getInt(4) == req.getInt(4)
                        && buffer.getInt(8) == req.getInt(8)
                        && buffer.getChar(12) == req.getChar(12));
                tailOffset[0] =
                    agents
                        .get(1)
                        .getEndpointMap()
                        .get(agents.get(0).getNode())
                        .getSharedCellBlock()
                        .getTailOffset();
                try {
                  agents.get(1).getEndpointGroup().releaseMemory((RamcastMessage) data);
                  Thread.sleep(100);
                } catch (IOException | InterruptedException e) {
                  e.printStackTrace();
                }
                lock.release();
              }
            });
    agents.get(0).getEndpointGroup().writeMessage(agents.get(1).getNode(), buffer);

    // check the tail of the remote shared cell block
    assertEquals(
        1,
        agents
            .get(0)
            .getEndpointMap()
            .get(agents.get(1).getNode())
            .getRemoteCellBlock()
            .getTailOffset());

    lock.acquire();
    assertEquals(1, tailOffset[0]);
    assertEquals(
        RamcastConfig.getInstance().getQueueLength() - 1,
        agents.get(0).getEndpointMap().get(agents.get(1).getNode()).getAvailableSlots());
  }

//  @Test
//  @Order(3)
  public void testWriteMultiMessages() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocateDirect(248);
    buffer.putInt(10);
    buffer.putInt(11);
    buffer.putInt(12);
    buffer.putChar('A');
    ByteBuffer response = ByteBuffer.allocateDirect((RamcastConfig.SIZE_MESSAGE - RamcastConfig.SIZE_BUFFER_LENGTH));
    response.putInt(100);
    response.putInt(111);
    response.putInt(122);
    response.putChar('A');
    AtomicInteger send = new AtomicInteger(0);
    AtomicInteger receive = new AtomicInteger(0);
    sendPermits = new Semaphore(1);

    agents
        .get(1)
        .getEndpointGroup()
        .setCustomHandler(
            new CustomHandler() {
              @Override
              public void handleReceiveMessage(Object data) {
                ByteBuffer req = ((RamcastMessage) data).getBuffer();
                assertTrue(
                    buffer.getInt(0) == req.getInt(0)
                        && buffer.getInt(4) == req.getInt(4)
                        && buffer.getInt(8) == req.getInt(8)
                        && buffer.getChar(12) == req.getChar(12));
                response.putInt(0, req.getInt(0));
                receive.getAndIncrement();
                MDC.put(
                    "ROLE",
                    agents.get(1).getNode().getGroupId()
                        + "/"
                        + agents.get(1).getNode().getNodeId());
                try {
                  agents.get(1).getEndpointGroup().releaseMemory((RamcastMessage) data);
                  agents.get(1).getEndpointGroup().writeMessage(agents.get(0).getNode(), response);
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
              public void handleReceiveMessage(Object data) {
                ByteBuffer req = ((RamcastMessage) data).getBuffer();
                assertTrue(
                    response.getInt(0) == req.getInt(0)
                        && response.getInt(4) == req.getInt(4)
                        && response.getInt(8) == req.getInt(8)
                        && response.getChar(12) == req.getChar(12));
                MDC.put(
                    "ROLE",
                    agents.get(0).getNode().getGroupId()
                        + "/"
                        + agents.get(0).getNode().getNodeId());
                try {
                  agents.get(0).getEndpointGroup().releaseMemory((RamcastMessage) data);
                } catch (IOException e) {
                  e.printStackTrace();
                }
                releasePermit();
              }
            });

    logger.info("Going to run write test for 1 seconds");
    long end = System.currentTimeMillis() + 1 * 1000; // going to run this experiment for 20 secs
    while (System.currentTimeMillis() < end) {
      getPermit();
      MDC.put(
          "ROLE", agents.get(0).getNode().getGroupId() + "/" + agents.get(0).getNode().getNodeId());
      buffer.putInt(0, send.get());
      agents.get(0).getEndpointGroup().writeMessage(agents.get(1).getNode(), buffer);
      send.getAndIncrement();
    }
    getPermit(); // to wait for the last response
    assertEquals(send.get(), receive.get());
    logger.info("Write {} packages", send.get() + receive.get());
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
}
