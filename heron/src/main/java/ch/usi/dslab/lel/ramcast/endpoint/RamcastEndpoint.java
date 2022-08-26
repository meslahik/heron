package ch.usi.dslab.lel.ramcast.endpoint;

import ch.usi.dslab.lel.dynastar.tpcc.tables.Tables;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.heron.HeronMemoryBlock;
import ch.usi.dslab.lel.ramcast.heron.HeronRecoveryMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
import ch.usi.dslab.lel.ramcast.models.RamcastTsMemoryBlock;
import ch.usi.dslab.lel.ramcast.utils.StringUtils;
import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.util.MemoryUtils;
import com.ibm.disni.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RamcastEndpoint extends RdmaEndpoint {
  private static final Logger logger = LoggerFactory.getLogger(RamcastEndpoint.class);
  //  IbvMr remoteHeadMr;
  //  IbvMr sharedCircularMr;
  //  IbvMr sharedTimestampMr;
  private final Object readTsSync = new Object();
  // shared memory cell (segment allocated for this endpoint) of the remote endpoint
  private RamcastMemoryBlock remoteCellBlock;
  // shared memory cell (segment allocated for this endpoint) at local
  private RamcastMemoryBlock sharedCellBlock;
  private RamcastConfig config = RamcastConfig.getInstance();
  private RamcastEndpointVerbCall verbCalls;
  // remote shared memory block (total) and timestamp of the remote endpoint
  // the actual memory buffer is created at group
  // the endpoints only store register data
  // todo: is the above comment correct? it doesn't seem so
  // this is registered buffer from group and is local buffer
  private RamcastMemoryBlock sharedCircularBlock;
  // local shared memory block for receiving message from clients
  // todo: this comment also does not seem correct!
  // this refers to the remote buffer stored on the other node
  private RamcastMemoryBlock remoteCircularBlock;
  // .. and shared memory block for receiving timestamp from leaders, similar to above
  private RamcastMemoryBlock sharedTimestampBlock;
  private RamcastTsMemoryBlock remoteTimeStampBlock;
  // shared buffer for client to receive server's header position
  // mojtaba: head stored locally that stores remote buffer's head
  // This head is updated by remote node to let this node about the current head position in the remote buffer
  private RamcastMemoryBlock sharedServerHeadBlock; // this one is of client, wait to be writen
  // this one is stored on server as target to write
  // mojtaba: this is the head of memory block stored in this node;
  // This node updates this remote head to let other node about the current head position of the local buffer
  private RamcastMemoryBlock remoteServerHeadBlock;

  //mojtaba: heron
//  private HeronMemoryBlock sharedHeronBlock;
  private HeronMemoryBlock remoteHeronBlock;
  private HeronRecoveryMemoryBlock remoteHeronRecoveryBlock;

  // storing node that this endpoint is connected to
  private RamcastNode node;
  // indicate if this endpoint is ready (connected, exchanged data)
  private boolean hasExchangedClientData = false;
  private boolean hasExchangedServerData = false;
  private boolean hasExchangedPermissionData = false;
  // Memory manager for timestamp buffer, put it here to revoke later
  private IbvMr sharedTimestampMr;
  private int unsignaledWriteCount = 0;
  private int unsignaledUpdateCount = 0;
  // mojtaba: this is a mismatch in naming buffers and blocks.
  // this buffer is stored in sharedServerHeadBlock (and not in remoteServerHeadBlock)
  private ByteBuffer
          remoteHeadBuffer; // shared buffer for client to receive server's header position. need to be
  // here to not being clean

  protected RamcastEndpoint(
          RdmaEndpointGroup<? extends RamcastEndpoint> group, RdmaCmId idPriv, boolean serverSide)
          throws IOException {
    super(group, idPriv, serverSide);
  }

  @Override
  protected synchronized void init() throws IOException {
    super.init();

    // allocate and register shared buffer for client to receive server's header position
    remoteHeadBuffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_REMOTE_HEAD);
    remoteHeadBuffer.putInt(0, -1);
    remoteHeadBuffer.putInt(4, -1);
    IbvMr remoteHeadMr = registerMemory(remoteHeadBuffer).execute().free().getMr();
    sharedServerHeadBlock =
            new RamcastMemoryBlock(
                    remoteHeadMr.getAddr(),
                    remoteHeadMr.getLkey(),
                    remoteHeadMr.getLength(),
                    remoteHeadBuffer);

    // register the shared buffer for receiving client message.
    // the buffer was created on group
    // mojtaba: register the buffer defined in EndpointGroup and create a MemoryBlock
    // it is the whole memory defined for exchanging messages, it gets segmented between processes
    ByteBuffer sharedCircularBuffer = ((RamcastEndpointGroup) this.group).getSharedCircularBuffer();
    IbvMr sharedCircularMr = registerMemory(sharedCircularBuffer).execute().free().getMr();
    sharedCircularBlock =
            new RamcastMemoryBlock(
                    sharedCircularMr.getAddr(),
                    sharedCircularMr.getLkey(),
                    sharedCircularMr.getLength(),
                    sharedCircularBuffer);

    // mojtaba: heron
//    ByteBuffer sharedHeronBuffer = ((RamcastEndpointGroup) this.group).getSharedHeronBuffer();
//    IbvMr mr = registerMemory(sharedHeronBuffer).execute().free().getMr();
//    sharedHeronBlock = new HeronMemoryBlock(
//            mr.getAddr(),
//            mr.getLkey(),
//            mr.getRkey(),
//            mr.getLength(),
//            sharedHeronBuffer);
//    logger.debug("heron block declaration block:{}, buffer: {}", sharedHeronBlock, sharedHeronBuffer);

    // extract to separated method
    //    registerTimestampWritePermission();

    //    ByteBuffer sharedTimestampBuffer =
    //        ((RamcastEndpointGroup) this.group).getSharedTimestampBuffer();
    //    // explicitly declare permission for this endpoint
    //    int access =
    //        IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE |
    // IbvMr.IBV_ACCESS_REMOTE_READ;
    //    sharedTimestampMr = getPd().regMr(sharedTimestampBuffer, access).execute().free().getMr();
    //    sharedTimestampBlock =
    //        new RamcastMemoryBlock(
    //            sharedTimestampMr.getAddr(),
    //            sharedTimestampMr.getLkey(),
    //            sharedTimestampMr.getLength(),
    //            sharedTimestampBuffer);

    verbCalls = new RamcastEndpointVerbCall(this);
  }

  public RamcastMemoryBlock registerTimestampReadPermission() throws IOException {
    // if the buffer has been register -> deregister it
    if (this.sharedTimestampMr != null) this.sharedTimestampMr.deregMr().execute().free();

    ByteBuffer sharedTimestampBuffer =
            ((RamcastEndpointGroup) this.group).getSharedTimestampBuffer();
    int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
    sharedTimestampMr = getPd().regMr(sharedTimestampBuffer, access).execute().free().getMr();
    sharedTimestampBlock =
            new RamcastMemoryBlock(
                    sharedTimestampMr.getAddr(),
                    sharedTimestampMr.getLkey(),
                    sharedTimestampMr.getLength(),
                    sharedTimestampBuffer);
    if (RamcastConfig.LOG_ENABLED)
      logger.info(
              "Shared timestamp block with READ permission for {}: {}",
              this.node,
              sharedTimestampBlock);
    return sharedTimestampBlock;
  }

  public RamcastMemoryBlock registerTimestampWritePermission() throws IOException {
    // if the buffer has been register -> deregister it
    if (this.sharedTimestampMr != null) this.sharedTimestampMr.deregMr().execute().free();
    ByteBuffer sharedTimestampBuffer =
            ((RamcastEndpointGroup) this.group).getSharedTimestampBuffer();
    // explicitly declare permission for this endpoint
    int access =
            IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
    sharedTimestampMr = getPd().regMr(sharedTimestampBuffer, access).execute().free().getMr();
    sharedTimestampBlock =
            new RamcastMemoryBlock(
                    sharedTimestampMr.getAddr(),
                    sharedTimestampMr.getLkey(),
                    sharedTimestampMr.getLength(),
                    sharedTimestampBuffer);
    if (RamcastConfig.LOG_ENABLED)
      logger.info(
              "Shared timestamp block with READ/WRITE permission for {}: {}",
              this.node,
              sharedTimestampBlock);
    return sharedTimestampBlock;
  }

  public void dispatchCqEvent(IbvWC wc) throws IOException {
    logger.trace("dispatchCqEvent status: {}, opcode: {}", wc.getStatus(), wc.getOpcode());
    if (wc.getStatus() == 5) {  // IBV_WC_WR_FLUSH_ERR
      return;
//      throw new IOException("Faulty operation! wc.status " + wc.getStatus());
    } else if (wc.getStatus() == 10) {  //IBV_WC_REM_ACCESS_ERR // permission error
      // happens when permission to write to this memory has been revoke
      // or just simply incorrect registration in the boostrap
      int index = (int) wc.getWr_id();
      dispatchPermissionError(verbCalls.updateBufs[index - verbCalls.updatePostIndexOffset]);
      return;
    } else if (wc.getStatus() != 0) { // 0 => IBV_WC_SUCCESS
      throw new IOException("Faulty operation! wc.status " + wc.getStatus());
    }

    if (wc.getOpcode() == 128) {
      // receiving a message
      int index = (int) wc.getWr_id();
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/{}] deliver RECEIVE event", this.endpointId, index);
      ByteBuffer recvBuffer = verbCalls.recvBufs[index];
      dispatchReceive(recvBuffer);
      verbCalls.postRecv(index);
    } else if (wc.getOpcode() == 0) {
      // send completion
      int index = (int) wc.getWr_id();
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/{}] deliver SEND COMPLETION event", this.endpointId, index);
      ByteBuffer sendBuffer = verbCalls.sendBufs[index];
      dispatchSendCompletion(sendBuffer);
      verbCalls.freeSend(index);
    } else if (wc.getOpcode() == 1) { // IBV_WC_RDMA_WRITE SIGNAL BACK
      // write completion
      int index = (int) wc.getWr_id();
      if (index >= verbCalls.updatePostIndexOffset) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace("[{}/{}] deliver WRITE SIGNAL COMPLETION event", this.endpointId, index);
        verbCalls.freeUpdate(index);
      } else {
        ByteBuffer buffer = verbCalls.writeBufs[index];
        if (RamcastConfig.LOG_ENABLED)
          logger.trace("[{}/{}] deliver WRITE MESSAGE COMPLETION event", this.endpointId, index);
        verbCalls.freeWrite(index);
        dispatchWriteCompletion(buffer);
      }
    } else if (wc.getOpcode() == 2) { // IBV_WC_RDMA_READ
      int index = (int) wc.getWr_id();
//      ByteBuffer readBuffer = verbCalls.readTsBuf;
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}/{}] deliver an event IBV_WC_RDMA_READ opcode {}, status {} ",
                this.endpointId,
                index,
                wc.getOpcode(),
                wc.getStatus());
      dispatchReadCompletion();
    } else {
      throw new IOException("Unkown opcode " + wc.getOpcode());
    }
  }

  protected boolean _send(ByteBuffer buffer) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("[{}/-1] perform SEND to {}", this.endpointId, this.node);
    SVCPostSend postSend = verbCalls.freeSendPostSend.poll();
    if (postSend != null) {
      buffer.clear();
      int index = (int) postSend.getWrMod(0).getWr_id();
      ByteBuffer sendBuf = verbCalls.sendBufs[index];
      sendBuf.clear();
      sendBuf.put(buffer);
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}/{}] sendBuff -- capacity:{} / limit:{} / remaining: {} / first Int {}",
                this.endpointId,
                index,
                sendBuf.capacity(),
                sendBuf.limit(),
                sendBuf.remaining(),
                sendBuf.getInt(0));
      postSend.getWrMod(0).getSgeMod(0).setLength(buffer.capacity());
      postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
      postSend.getWrMod(0).setSend_flags(postSend.getWrMod(0).getSend_flags());
      verbCalls.pendingSendPostSend.put(index, postSend);
      postSend.execute();
      return true;
    } else {
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/-1] don't have available postsend. Wait", this.endpointId);
      return false;
    }
  }

  public int putToBuffer(ByteBuffer buffer, Class type, Object value) {
    //    logger.trace("Wring to buffer {} class {} value {}", buffer, type, value);
    if (type == Integer.TYPE) {
      buffer.putInt((Integer) value);
      return Integer.BYTES;
    } else if (type == Long.TYPE) {
      buffer.putLong((Long) value);
      return Long.BYTES;
    } else if (type == Character.TYPE) {
      buffer.putChar((Character) value);
      return Character.BYTES;
    } else if (type == Short.TYPE) {
      buffer.putShort((Short) value);
      return Short.BYTES;
    } else if (type == Double.TYPE) {
      buffer.putDouble((Double) value);
      return Double.BYTES;
    } else if (type == Float.TYPE) {
      buffer.putFloat((Float) value);
      return Float.BYTES;
    } else if (type == Byte.TYPE) {
      buffer.put((Byte) value);
      return Byte.BYTES;
    }
    if (RamcastConfig.LOG_ENABLED)
      logger.error("putToBuffer no class type");
    return 0;
  }

  // data should be in the form of a pair of <type> <value>: Integer 4 long 2
  protected boolean _writeSignal(long address, int lkey, Object... data) throws IOException {
    SVCPostSend postSend = verbCalls.freeUpdatePostSend.poll();
    boolean signaled = unsignaledUpdateCount % config.getSignalInterval() == 0;
    if (postSend != null) {
      int index = (int) postSend.getWrMod(0).getWr_id();
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}/{}] perform WRITE SIGNAL {} to {} at address {} lkey {} values {}",
                this.endpointId,
                index,
                signaled ? "SIGNALED" : "UNSIGNALED",
                this.node,
                address,
                lkey,
                data);

      ByteBuffer updateBuffer = verbCalls.updateBufs[index - verbCalls.updatePostIndexOffset];
      updateBuffer.clear();
      int length = 0;
      for (int i = 0; i < data.length; i++) {
        length += putToBuffer(updateBuffer, (Class) data[i], data[++i]);
      }
      if (length > RamcastConfig.SIZE_SIGNAL) {
        throw new IOException("Buffer size of [" + length + "] is too big. Only allow " + RamcastConfig.SIZE_SIGNAL);
      }
      updateBuffer.clear();
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("issuing remote write: addr {}, lkey {}, len {}, buffer {}", address, lkey, length, updateBuffer);
      postSend.getWrMod(0).getSgeMod(0).setLength(length);
      postSend.getWrMod(0).getRdmaMod().setRemote_addr(address);
      postSend.getWrMod(0).getRdmaMod().setRkey(lkey);
      if (signaled) {
        postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED | IbvSendWR.IBV_SEND_INLINE);
        unsignaledUpdateCount = 1;
        verbCalls.pendingUpdatePostSend.put(index, postSend);
        postSend.execute();
      } else {
        postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_INLINE);
        unsignaledUpdateCount++;
        postSend.execute();
        verbCalls.freeUpdatePostSend.add(postSend);
      }
      return true;
    } else {
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/-1] don't have available postsend. Wait", this.endpointId);
      return false;
    }
  }

  protected boolean _writeMessage(ByteBuffer buffer, boolean signaled) throws IOException {
    //    logger.trace("[{}/-1] perform WRITE on SHARED BUFFER of {}", this.endpointId, this.node);
    if (buffer.capacity() > (RamcastConfig.SIZE_MESSAGE - RamcastConfig.SIZE_BUFFER_LENGTH)) {
      throw new IOException(
              "Buffer size of ["
                      + buffer.capacity()
                      + "] is too big. Only allow "
                      + (RamcastConfig.SIZE_MESSAGE - RamcastConfig.SIZE_BUFFER_LENGTH));
    }

    int availableSlots = this.getAvailableSlots();
    if (availableSlots <= 0) {
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
                "[{}/-1] Don't have available slot for writing message. {}",
                this.endpointId,
                availableSlots);
      return false;
    }
    SVCPostSend postSend = verbCalls.freeWritePostSend.poll();
    if (postSend != null) {
      buffer.clear();
      int index = (int) postSend.getWrMod(0).getWr_id();
      int written = 0;
      ByteBuffer writeBuf = verbCalls.writeBufs[index];
      writeBuf.clear();
      writeBuf.putInt(buffer.capacity() + 4);
      written += 4;
      writeBuf.put(buffer);
      written += buffer.capacity();
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}] [{}/{}] WRITE at position {} length {} buffer {}  write buffer capacity:{} / limit:{} / remaining: {} / first int {} /crc {}/ tail {}",
                writeBuf.getInt(4),
                this.endpointId,
                index,
                this.remoteCellBlock.getTailOffset(),
                writeBuf.getInt(0),
                buffer,
                writeBuf.capacity(),
                writeBuf.limit(),
                writeBuf.remaining(),
                writeBuf.getInt(4),
                buffer.getLong(buffer.capacity() - RamcastConfig.SIZE_CHECKSUM),
                this.remoteCellBlock.getTailOffset());
      postSend.getWrMod(0).getSgeMod(0).setLength(buffer.capacity() + 4);
      // todo: what is getSgeMod? modify scatter/gathere element?
      // todo: setAddr? local memory address?
      postSend.getWrMod(0).getSgeMod(0).setAddr(MemoryUtils.getAddress(writeBuf));
      // todo: setRemote_addr? remote address?
      postSend.getWrMod(0).getRdmaMod().setRemote_addr(this.remoteCellBlock.getTail());
      this.remoteCellBlock.advanceTail();
      postSend.getWrMod(0).getRdmaMod().setRkey(this.remoteCellBlock.getLkey());
      if (signaled) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
                  "[{}] [{}/{}] client Signaled remote write. Adding postSend to pending list. Remote mem {}",
                  writeBuf.getInt(4),
                  this.endpointId,
                  index,
                  this.remoteCellBlock);

        if (buffer.capacity() + 4 <= RamcastConfig.getInstance().getMaxinline())
          postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED | IbvSendWR.IBV_SEND_INLINE);
        else
          postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        verbCalls.pendingWritePostSend.put(index, postSend);
      } else {
        postSend.getWrMod(0).setSend_flags(0);
        if (buffer.capacity() + 4 <= RamcastConfig.getInstance().getMaxinline())
          postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_INLINE);
      }
      postSend.execute();
      if (!signaled) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
                  "[{}/{}] client Unsignaled remote write. Adding back postSend to available list. Remote mem {}",
                  this.endpointId,
                  index,
                  this.remoteCellBlock);
        verbCalls.freeWritePostSend.add(postSend);
        dispatchUnsignaledWriteCompletion(buffer);
      }
      return true;
    } else {
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}/-1] don't have available postsend to write message. Wait", this.endpointId);
      return false;
    }
  }

  protected boolean _writeRecoveryData(
          SVCPostSend postSend, long address, int lkey, int rkey, ByteBuffer buffer, int capacity, boolean signaled)
          throws IOException {
//    if (buffer.capacity() > (RamcastConfig.SIZE_MESSAGE - RamcastConfig.SIZE_BUFFER_LENGTH)) {

      buffer.clear();
    logger.trace(
            "Write message from buffer {}, ep {}",
            buffer);
      int index = (int) postSend.getWrMod(0).getWr_id();

//      int written = 0;
//      ByteBuffer writeBuf = verbCalls.writeBufs[index];
//      writeBuf.clear();
//      writeBuf.putInt(buffer.capacity() + 4);
//      written += 4;
//      writeBuf.put(buffer);
//      written += buffer.capacity();

//      if (RamcastConfig.LOG_ENABLED)
//        logger.trace(
//                "[{}] WRITE recovery data from buffer capacity:{} / limit:{} / remaining: {} / first int {}",
//                this.endpointId,
//                writeBuf.capacity(),
//                writeBuf.limit(),
//                writeBuf.remaining(),
//                writeBuf.getInt(0));

      // set length
      postSend.getWrMod(0).getSgeMod(0).setLength(capacity);
      // buffer address from which to write remotely
      postSend.getWrMod(0).getSgeMod(0).setAddr(MemoryUtils.getAddress(buffer));
    postSend.getWrMod(0).getSgeMod(0).setLkey(lkey);
      // address to write
      postSend.getWrMod(0).getRdmaMod().setRemote_addr(address);
      //set rkey
      postSend.getWrMod(0).getRdmaMod().setRkey(rkey);

      if (signaled) {
        if (buffer.capacity() + 4 <= RamcastConfig.getInstance().getMaxinline())
          postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED | IbvSendWR.IBV_SEND_INLINE);
        else
          postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        verbCalls.pendingWritePostSend.put(index, postSend);
      } else {
        // empty
        postSend.getWrMod(0).setSend_flags(0);
        // check if to inline data
        if (buffer.capacity() + 4 <= RamcastConfig.getInstance().getMaxinline())
          postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_INLINE);
      }

      postSend.execute();

      if (!signaled) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
                  "[{}/{}] client Unsignaled recovery data write. Adding back postSend to available list. Remote mem {}",
                  this.endpointId,
                  index,
                  address);
        verbCalls.freeWritePostSend.add(postSend);
        dispatchUnsignaledWriteCompletion(buffer);
      }

      return true;
  }

  // FOR TESTING ONLY
  protected boolean _writeTestResponse(long address, int lkey, ByteBuffer buffer) throws IOException {
    SVCPostSend postSend = verbCalls.freeUpdatePostSend.poll();
    boolean signaled = unsignaledUpdateCount % config.getSignalInterval() == 0;
    if (postSend != null) {
      int index = (int) postSend.getWrMod(0).getWr_id();
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}/{}] perform WRITE SIGNALED={} to {} at address {} lkey {} values {}, unsignaledUpdateCount {}/{}",
                this.endpointId,
                index,
                signaled,
                this.node,
                address,
                lkey,
                buffer, unsignaledUpdateCount, config.getSignalInterval());

      ByteBuffer updateBuffer = verbCalls.updateBufs[index - verbCalls.updatePostIndexOffset];
      updateBuffer.clear();
      buffer.clear();
      updateBuffer.put(buffer);
      postSend.getWrMod(0).getSgeMod(0).setLength(buffer.capacity());
      postSend.getWrMod(0).getRdmaMod().setRemote_addr(address);
      postSend.getWrMod(0).getRdmaMod().setRkey(lkey);
      if (signaled) {
        if (buffer.capacity() <= RamcastConfig.getInstance().getMaxinline())
          postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED | IbvSendWR.IBV_SEND_INLINE);
        else
          postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        unsignaledUpdateCount = 1;
        verbCalls.pendingUpdatePostSend.put(index, postSend);
        postSend.execute();
      } else {
        if (buffer.capacity() <= RamcastConfig.getInstance().getMaxinline())
          postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_INLINE);
        unsignaledUpdateCount++;
        postSend.execute();
        verbCalls.freeUpdatePostSend.add(postSend);
      }
      return true;
    } else {
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/-1] don't have available postsend. Wait", this.endpointId);
      return false;
    }
  }

  public ByteBuffer readRemoteMemory(long address, int rkey, ByteBuffer recvBuffer, int lkey, int length)
          throws IOException, InterruptedException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("waiting for freeReadPostSend, size {}", verbCalls.freeReadPostSend.size());
    SVCPostSend postSend = verbCalls.freeReadPostSend.poll();

//    if (RamcastConfig.LOG_ENABLED)
//      logger.debug(
//              "[{}/{}] client Perform remote READ size {}",
//              this.endpointId,
//              postSend.getWrMod(0).getWr_id(),
//              length);

    recvBuffer.clear();
    postSend.getWrMod(0).getRdmaMod().setRemote_addr(address);
    postSend.getWrMod(0).getRdmaMod().setRkey(rkey);
    postSend.getWrMod(0).getSgeMod(0).setAddr(MemoryUtils.getAddress(recvBuffer));
    postSend.getWrMod(0).getSgeMod(0).setLkey(lkey);
    postSend.getWrMod(0).getSgeMod(0).setLength(length);

    synchronized (Tables.isRemoteReadComplete) {
      Tables.isRemoteReadComplete = false;
    }

    postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
    postSend.execute();

    if (RamcastConfig.LOG_ENABLED)
      logger.trace("before call wait");
    synchronized (Tables.isRemoteReadComplete) {
      if (!Tables.isRemoteReadComplete)
        Tables.isRemoteReadComplete.wait();
    }

    verbCalls.freeReadPostSend.add(postSend);
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "[{}/{}] successful remote READ size {}",
              this.endpointId,
              postSend.getWrMod(0).getWr_id(),
              length);
    return recvBuffer;
  }

  protected RamcastTsMemoryBlock readTimestampMemorySpace()
          throws IOException, InterruptedException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "[{}/{}] client Perform remote TS READ size {}",
              this.endpointId,
              -1,
              this.remoteTimeStampBlock.getCapacity());
    SVCPostSend postSend = verbCalls.readTsCall;

    int index = (int) postSend.getWrMod(0).getWr_id();
    verbCalls.readTsBuf.position(0);
    postSend.getWrMod(0).getSgeMod(0).setAddr(MemoryUtils.getAddress(verbCalls.readTsBuf));
    postSend.getWrMod(0).getRdmaMod().setRemote_addr(this.remoteTimeStampBlock.getAddress());
    postSend.getWrMod(0).getRdmaMod().setRkey(this.remoteTimeStampBlock.getLkey());
    postSend.getWrMod(0).getSgeMod(0).setLength(this.remoteTimeStampBlock.getCapacity());

    postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
    postSend.execute();

    synchronized (verbCalls.readTsBuf) {
      verbCalls.readTsBuf.wait();
      return new RamcastTsMemoryBlock(remoteTimeStampBlock.getAddress(), remoteTimeStampBlock.getLkey(), remoteTimeStampBlock.getCapacity(), verbCalls.readTsBuf);
    }
  }

  /*private int getAvailableSlotOnRemote() {
    int freed = this.getRemoteHead();
    if (freed > 0) this.remoteSharedCellBlock.moveHeadOffset(freed);
    return this.remoteSharedCellBlock.getRemainingSlots();
  }*/

  private void dispatchPermissionError(ByteBuffer buffer) {
    ((RamcastEndpointGroup) this.group).handlePermissionError(buffer);
  }

  private void dispatchRemoteWrite(RamcastMessage message) throws IOException {
    ((RamcastEndpointGroup) this.group).handleReceiveMessage(message);
  }

  private void dispatchWriteCompletion(ByteBuffer buffer) {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "dispatch WriteCompletion of buffer [{} {} {}]",
              buffer.getInt(0),
              buffer.getInt(4),
              buffer.getInt(8));
  }

  private void dispatchUnsignaledWriteCompletion(ByteBuffer buffer) {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "dispatch UnsignaledWriteCompletion of buffer [{} {} {}]",
              buffer.getInt(0),
              buffer.getInt(4),
              buffer.getInt(8));
  }

  private void dispatchSendCompletion(ByteBuffer buffer) {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "dispatch SendCompletion of buffer [{} {} {}]",
              buffer.getInt(0),
              buffer.getInt(4),
              buffer.getInt(8));
    ((RamcastEndpointGroup) this.group).handleSendComplete(this, buffer);
  }

  private void dispatchReceive(ByteBuffer buffer) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "dispatch Receive of buffer [{} {} {}]",
              buffer.getInt(0),
              buffer.getInt(4),
              buffer.getInt(8));
    if (buffer.getInt(0) < 0) {
      ((RamcastEndpointGroup) this.group).handleProtocolMessage(this, buffer);
    } else {
      ((RamcastEndpointGroup) this.group).handleReceive(this, buffer);
    }
  }

  private void dispatchReadTs(ByteBuffer buffer) {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "dispatch Read of buffer [{} {} {}]",
              buffer.getInt(0),
              buffer.getInt(4),
              buffer.getInt(8));
    synchronized (verbCalls.readTsBuf) {
      verbCalls.readTsBuf.notify();
    }
  }

  private void dispatchReadCompletion() {
    ((RamcastEndpointGroup) this.group).handleReadCompletion();
  }

  public int getAvailableSlots() {
    int freed = this.remoteHeadBuffer.getInt(0);
    if (freed > 0) {
      this.remoteHeadBuffer.putInt(0, 0);
      this.remoteCellBlock.moveHeadOffset(freed);
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}/] CLIENT do SET HEAD of {} to move {} positions, memory: {}",
                this.endpointId,
                this.node,
                freed,
                this.remoteCellBlock);
    }
    return this.remoteCellBlock.getRemainingSlots();
  }

  @Override
  public String toString() {
    return "{epId:" + this.getEndpointId() + "@" + this.node + "}";
  }

  public RamcastMemoryBlock getSharedCellBlock() {
    return sharedCellBlock;
  }

  public void setSharedCellBlock(RamcastMemoryBlock sharedCellBlock) {
    this.sharedCellBlock = sharedCellBlock;
  }

  public RamcastMemoryBlock getSharedCircularBlock() {
    return sharedCircularBlock;
  }

  public RamcastMemoryBlock getSharedTimestampBlock() {
    return sharedTimestampBlock;
  }

  public RamcastMemoryBlock getSharedServerHeadBlock() {
    return sharedServerHeadBlock;
  }

  public void setClientMemoryBlockOfRemoteHead(long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteServerHeadBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public void setRemoteSharedMemoryCellBlock(long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteCellBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public void setRemoteCircularBlock(long remoteAddr, int remoteLKey, int remoteLength) {
    // mojtaba: the buffer is null because this is only stores meta data of buffer stored on remote node
    this.remoteCircularBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public void setRemoteSharedTimestampMemoryBlock(
          long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteTimeStampBlock =
            new RamcastTsMemoryBlock(this.node, remoteAddr, remoteLKey, remoteLength, null);
  }

  public RamcastMemoryBlock getRemoteServerHeadBlock() {
    return remoteServerHeadBlock;
  }

  public RamcastMemoryBlock getRemoteCircularBlock() {
    return remoteCircularBlock;
  }

  public RamcastTsMemoryBlock getRemoteTimeStampBlock() {
    return remoteTimeStampBlock;
  }

  public RamcastMemoryBlock getRemoteCellBlock() {
    return remoteCellBlock;
  }

  public RamcastMemoryBlock getRemoteSharedMemoryCellBlock() {
    return remoteCellBlock;
  }

  public boolean isReady() {
    return hasExchangedClientData && hasExchangedServerData;
  }

  public boolean hasExchangedClientData() {
    return hasExchangedClientData;
  }

  public void setHasExchangedClientData(boolean hasExchangedClientData) {
    this.hasExchangedClientData = hasExchangedClientData;
  }

  public boolean hasExchangedServerData() {
    return hasExchangedServerData;
  }

  public void setHasExchangedServerData(boolean hasExchangedServerData) {
    this.hasExchangedServerData = hasExchangedServerData;
  }

  public boolean hasExchangedPermissionData() {
    // if the remote side of this endpoint is not leader, and this is not from a leader node =>
    // do'nt need to exchange data
    if (!this.getNode().isLeader() && !((RamcastEndpointGroup) this.group).getAgent().isLeader())
      return true;
    // or this is leader && remote side is leader => this connect to itself
    if (this.getNode().equals(((RamcastEndpointGroup) group).getAgent().getNode())) return true;
    return hasExchangedPermissionData;
  }

  public void setHasExchangedPermissionData(boolean hasExchangedPermissionData) {
    this.hasExchangedPermissionData = hasExchangedPermissionData;
  }

  @Override
  public synchronized void close() throws IOException, InterruptedException {
    super.close();
    verbCalls.close();
  }

  public void send(ByteBuffer buffer) throws IOException {
    while (!_send(buffer)) {
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void writeSignal(long address, int lkey, Object... values) throws IOException {
    while (!_writeSignal(address, lkey, values)) {
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  // FOR TESTING ONLY
  public void writeTestResponse(long address, int lkey, ByteBuffer buffer) throws IOException {
    while (!_writeTestResponse(address, lkey, buffer)) {
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void writeMessage(ByteBuffer buffer) throws IOException {
    boolean signaled = unsignaledWriteCount % config.getSignalInterval() == 0;
    if (signaled) {
      unsignaledWriteCount = 1;
    } else {
      unsignaledWriteCount += 1;
    }
    while (!_writeMessage(buffer, signaled)) {
      // todo: what is this? different from thread.yield?
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public SVCPostSend getSVCPostSend() {
    SVCPostSend postSend;
    while (true) {
      postSend = verbCalls.freeWritePostSend.poll();
      if (postSend != null)
        break;
//      else {
//        if (RamcastConfig.LOG_ENABLED)
//          logger.trace(
//                  "[{}/-1] don't have available postsend to write recovery data. Wait", this.endpointId);
//      }

      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    return postSend;
  }

  public void writeRecoveryData(
          SVCPostSend postSend, long address, int lkey, int rkey, ByteBuffer buffer, int capacity)
          throws IOException {
    if (buffer.capacity() > (RamcastConfig.SIZE_MESSAGE)) {
      throw new IOException(
              "Buffer size of [" + buffer.capacity() + "] is too big. Only allow " + RamcastConfig.SIZE_MESSAGE);
    }

    boolean signaled = unsignaledWriteCount % config.getSignalInterval() == 0;
    if (signaled) {
      unsignaledWriteCount = 1;
    } else {
      unsignaledWriteCount += 1;
    }

    _writeRecoveryData(postSend, address, lkey, rkey, buffer, capacity, signaled);
  }

  public void pollForData() {
    ByteBuffer recvBuffer = this.sharedCellBlock.getBuffer();
    try {
      recvBuffer.clear();
      int start = this.sharedCellBlock.getTailOffset() * RamcastConfig.SIZE_MESSAGE;
      recvBuffer.position(start);

      int msgLength = recvBuffer.getInt(start + RamcastConfig.POS_LENGTH_BUFFER);
      if (msgLength == 0) {
        return;
      }

      long crc = recvBuffer.getLong(start + msgLength - RamcastConfig.SIZE_CHECKSUM);
      if (crc == 0) {
        return;
      }

      recvBuffer.position(start + RamcastConfig.SIZE_BUFFER_LENGTH);
      recvBuffer.limit(start + msgLength - RamcastConfig.SIZE_CHECKSUM);

      long checksum = StringUtils.calculateCrc32(recvBuffer);
      recvBuffer.clear();

      if (checksum != crc) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
                  "Message is not completed. Length {} Calculated CRC {} vs Read CRC {}. Buffer [{} {} {}]",
                  msgLength,
                  checksum,
                  crc,
                  recvBuffer.getInt(start),
                  recvBuffer.getInt(start + 4),
                  recvBuffer.getInt(start + 8));
        return;
      }
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
                "[{}] dispatch local recvBuffer at position {}, Length {} CRC {}, buffer [{} {} {}], calculated CRC {}, memory {}",
                this.endpointId,
                this.sharedCellBlock.getTailOffset(),
                msgLength,
                crc,
                recvBuffer.getInt(start),
                recvBuffer.getInt(start + 4),
                recvBuffer.getInt(start + 8),
                checksum,
                this.sharedCellBlock);

      RamcastMessage message =
              new RamcastMessage(
                      ((ByteBuffer)
                              recvBuffer
                                      .position(start + RamcastConfig.SIZE_BUFFER_LENGTH)
                                      .limit(start + RamcastConfig.SIZE_MESSAGE))
                              .slice(),
                      this.node,
                      this.sharedCellBlock);

      // reset recvBuffer
      recvBuffer.clear();
      recvBuffer.putInt(start + RamcastConfig.POS_LENGTH_BUFFER, 0);
      // update remote head
      this.sharedCellBlock.advanceTail();
      this.dispatchRemoteWrite(message);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("Error pollForData1", e);
      System.exit(1);
    }
  }

  public RamcastNode getNode() {
    return node;
  }

  public void setNode(RamcastNode node) {
    this.node = node;
  }

  public RamcastEndpointGroup getEndpointGroup() {
    return (RamcastEndpointGroup) this.group;
  }

  public int getGroupId() {
    return this.node.getGroupId();
  }

  public int getCompletionSignal() {
    return this.remoteHeadBuffer.getInt(4);
  }

  @Override
  public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent) throws IOException {
    if (RamcastConfig.LOG_ENABLED) logger.trace("dispatch connection event");
    super.dispatchCmEvent(cmEvent);
    try {
      int eventType = cmEvent.getEvent();
      if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace("RPC disconnection, eid {}", this.getEndpointId());
        ((RamcastEndpointGroup) group).disconnect(this);
      }
    } catch (Exception e) {
      logger.error("Error dispatchCmEvent", e);
      e.printStackTrace();
    }
  }

//  public HeronMemoryBlock getSharedHeronBlock() {
//    return sharedHeronBlock;
//  }
//
//  public void setSharedHeronBlock(HeronMemoryBlock sharedHeronBlock) {
//    this.sharedHeronBlock = sharedHeronBlock;
//  }

  public HeronMemoryBlock getRemoteHeronBlock() {
    return remoteHeronBlock;
  }

  public void setRemoteHeronBlock(long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteHeronBlock = new HeronMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public HeronRecoveryMemoryBlock getRemoteHeronRecoveryBlock() {
    return remoteHeronRecoveryBlock;
  }

  public void setRemoteHeronRecoveryBlock(long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteHeronRecoveryBlock = new HeronRecoveryMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }
}
