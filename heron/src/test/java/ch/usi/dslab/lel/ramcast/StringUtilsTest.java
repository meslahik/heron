package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.utils.StringUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class StringUtilsTest {
  static Logger logger = LoggerFactory.getLogger(StringUtilsTest.class);

  @Test
  public void testStringChecksum() {
    ByteBuffer sampleBuffer1 = ByteBuffer.allocateDirect(RamcastConfig.SIZE_MESSAGE);
    sampleBuffer1.putInt(5);
    sampleBuffer1.putLong(10);
    sampleBuffer1.putChar('A');
    sampleBuffer1.position(0);
    long checksum1 = StringUtils.calculateCrc32(sampleBuffer1);
    ByteBuffer sampleBuffer2 = ByteBuffer.allocateDirect(RamcastConfig.SIZE_MESSAGE);
    sampleBuffer2.putInt(5);
    sampleBuffer2.putLong(10);
    sampleBuffer2.putChar('A');
    sampleBuffer2.position(0);
    long checksum2 = StringUtils.calculateCrc32(sampleBuffer2);
    assertEquals(checksum1, checksum2);
    ByteBuffer sampleBuffer3 = ByteBuffer.allocateDirect(RamcastConfig.SIZE_MESSAGE);
    sampleBuffer3.putInt(10);
    sampleBuffer3.putLong(10);
    sampleBuffer3.putChar('A');
    sampleBuffer3.position(0);
    long checksum3 = StringUtils.calculateCrc32(sampleBuffer3);
    assertNotEquals(checksum1, checksum3);
  }

  @Test
  public void testChecksumPerformance() {
    long end = System.currentTimeMillis() + 1000; // going to run this experiment for 20 secs
    ByteBuffer sampleBuffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_MESSAGE);
    sampleBuffer.putInt(5);
    sampleBuffer.putLong(10);
    sampleBuffer.putChar('A');
    int count = 0;
    while (System.currentTimeMillis() < end) {
      long checksum = StringUtils.calculateCrc32(sampleBuffer);
      count += 1;
    }
    logger.info("Performed [{}] checksum operations in 1 second", count);
  }
}
