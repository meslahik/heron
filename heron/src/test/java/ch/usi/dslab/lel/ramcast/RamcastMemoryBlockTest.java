package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.models.RamcastMemoryBlock;
import com.ibm.disni.util.MemoryUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

public class RamcastMemoryBlockTest {
  ByteBuffer buffer;
  RamcastMemoryBlock memoryBlock;
  long address;

  @BeforeEach
  public void setUp() {
    buffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_MESSAGE);
    address = MemoryUtils.getAddress(buffer);
    memoryBlock = new RamcastMemoryBlock(address, 0, RamcastConfig.SIZE_MESSAGE * 5, buffer);
  }

  @Test
  public void testMoveHeadTail() {

    // initially can't move head, since no msg has been written
    Exception exception =
        assertThrows(IllegalStateException.class, () -> memoryBlock.moveHeadOffset(1));
    String msg = exception.getMessage();
    assertEquals("Head can not pass tail. Current head=0 and tail=0 tail passed head=false", msg);

    // 2 message was written, move tail 2
    memoryBlock.moveTailOffset(2);
    assertEquals(2, memoryBlock.getTailOffset());
    assertEquals(address + RamcastConfig.SIZE_MESSAGE * 2, memoryBlock.getTail());

    // 3 message was written, move tail 3
    // tail reach the end of the buffer, should return to first slot
    memoryBlock.moveTailOffset(3);
    assertEquals(0, memoryBlock.getTailOffset());
    assertEquals(address, memoryBlock.getTail());
    assertTrue(memoryBlock.isTailPassedHead());

    // can't move tail any more, since tail reach head
    exception = assertThrows(IllegalStateException.class, () -> memoryBlock.moveTailOffset(1));
    msg = exception.getMessage();
    assertEquals("Tail can not pass head. Current head=0 and tail=0 tail passed head=true", msg);

    // deliver 1 message => move head 1
    memoryBlock.moveHeadOffset(1);
    assertEquals(address + RamcastConfig.SIZE_MESSAGE, memoryBlock.getHead());

    // now should be able to move tail
    memoryBlock.moveTailOffset(1);
    assertEquals(1, memoryBlock.getTailOffset());
    assertEquals(address + RamcastConfig.SIZE_MESSAGE, memoryBlock.getTail());

    // deliver 4 message => move head 4
    // head should return back to 0
    // head pass tail = false
    memoryBlock.moveHeadOffset(4);
    assertEquals(0, memoryBlock.getHeadOffset());
    assertEquals(address, memoryBlock.getHead());
    assertFalse(memoryBlock.isTailPassedHead());
  }

  @Test
  public void testFreeSlot() {
    // 1 message was written, move tail 1
    memoryBlock.advanceTail();
    assertEquals(1, memoryBlock.getTailOffset());
    assertEquals(address + RamcastConfig.SIZE_MESSAGE, memoryBlock.getTail());

    // deliver 1 message, free that slot
    int freed = memoryBlock.freeSlot(0);
    assertEquals(address + RamcastConfig.SIZE_MESSAGE, memoryBlock.getHead());
    assertEquals(1, memoryBlock.getHeadOffset());
    assertEquals(1, freed);

    // 3 message was written, move tail 3
    memoryBlock.moveTailOffset(3);
    assertEquals(4, memoryBlock.getTailOffset());
    assertEquals(address + RamcastConfig.SIZE_MESSAGE * 4, memoryBlock.getTail());
    assertEquals(1, memoryBlock.getHeadOffset());

    // deliver message in the middle (2), free that slot
    freed = memoryBlock.freeSlot(2);
    assertEquals(0, freed);
    // head should not advance
    assertEquals(1, memoryBlock.getHeadOffset());
    assertEquals(address + RamcastConfig.SIZE_MESSAGE, memoryBlock.getHead());

    // deliver another message at front (1).
    freed = memoryBlock.freeSlot(1);
    // head should move 2 positions
    assertEquals(3, memoryBlock.getHeadOffset());
    assertEquals(2, freed);
    assertEquals(address + RamcastConfig.SIZE_MESSAGE * 3, memoryBlock.getHead());
  }
}
