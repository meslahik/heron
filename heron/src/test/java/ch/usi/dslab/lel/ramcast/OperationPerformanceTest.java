package ch.usi.dslab.lel.ramcast;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class OperationPerformanceTest {
  static Logger logger = LoggerFactory.getLogger(OperationPerformanceTest.class);


  @Test
  public void testBufferAllocationPerformance() {

    long end = System.currentTimeMillis() + 1000;
    int count = 0;
    while (System.currentTimeMillis() < end) {
      ByteBuffer buffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_SIGNAL);
//      buffer.putInt(1);
//      buffer.putInt(1);
//      buffer.putInt(1);
//      buffer.putInt(1);
      count++;
    }
    logger.info(
        "Number of {}-bytes buffer allocation in 1 seconds: {}", RamcastConfig.SIZE_SIGNAL, count);
  }
}
