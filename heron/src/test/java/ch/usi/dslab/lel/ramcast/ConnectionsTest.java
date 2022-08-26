package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.models.RamcastMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ConnectionsTest {
  static Logger logger = LoggerFactory.getLogger(ConnectionsTest.class);
  static RamcastConfig config = RamcastConfig.getInstance();
  static ByteBuffer buffer;

  static int groups = 1;
  static int nodes = 2;
  static Map<RamcastNode, RamcastAgent> agents;
  static List<Thread> threads;

  @BeforeAll
  public static void setUp() throws Exception {
    logger.info("Setting up for ConnectionsTest");
    File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
    config = RamcastConfig.getInstance();
    config.loadConfig(configFile.getPath());

    agents = new ConcurrentHashMap<>();
    threads = new ArrayList<>();

    for (int g = 0; g < groups; g++) {
      for (int p = 0; p < nodes; p++) {
        int finalP = p;
        int finalG = g;
        Thread t =
            new Thread(
                () -> {
                  try {
                    logger.info("Starting " + finalG + "/" + finalP);
                    RamcastAgent agent = new RamcastAgent(finalG, finalP);
                    agents.put(agent.getNode(), agent);
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

  @Test
  public void verifyConnections() {
    // checking connection count
    for (RamcastAgent agent : agents.values()) {
      // todo: find nicer way for -1
      assertEquals(groups * nodes, agent.getEndpointMap().keySet().size());
    }
  }

  @Test
  public void verifyDataExchange() {
    // checking exchanged data
    for (RamcastAgent agent : agents.values()) {
      for (Map.Entry<RamcastNode, RamcastEndpoint> connection : agent.getEndpointMap().entrySet()) {
        RamcastEndpoint endpoint = connection.getValue();
        RamcastNode remoteNode = connection.getKey();
        RamcastAgent remoteAgent = agents.get(remoteNode);
        logger.debug("Comparing of agent {} and remote agent {}", agent, remoteAgent);
        if (agent.getNode().equals(remoteNode)) continue;
        assertEquals(
            endpoint.getSharedCircularBlock(),
            remoteAgent.getEndpointMap().get(agent.getNode()).getRemoteCircularBlock());
        assertEquals(
            endpoint.getSharedServerHeadBlock(),
            remoteAgent.getEndpointMap().get(agent.getNode()).getRemoteServerHeadBlock());
        assertEquals(-1, endpoint.getSharedServerHeadBlock().getBuffer().get(0));
      }
    }
    for (int i = 0; i < groups; i++) {
      for (int j = 0; j < nodes; j++) {
        RamcastAgent agent = agents.get(RamcastNode.getNode(i, j));
        if (agent.isLeader()) {
          for (RamcastEndpoint endpoint : agent.getEndpointMap().values()) {
            if (agent.getNode().equals(endpoint.getNode())) continue;
            RamcastMemoryBlock remoteTsBlock =
                agents
                    .get(endpoint.getNode())
                    .getEndpointMap()
                    .get(RamcastNode.getNode(i, j))
                    .getSharedTimestampBlock();
            RamcastMemoryBlock dataOfRemoteTsBlock = endpoint.getRemoteTimeStampBlock();
            assertEquals(remoteTsBlock, dataOfRemoteTsBlock);
          }
        } else {
          for (RamcastEndpoint endpoint : agent.getEndpointMap().values()) {
            assertNull(endpoint.getRemoteTimeStampBlock());
          }
        }
      }
    }
  }
}
