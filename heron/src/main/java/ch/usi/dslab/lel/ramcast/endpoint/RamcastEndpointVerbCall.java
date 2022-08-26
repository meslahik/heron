package ch.usi.dslab.lel.ramcast.endpoint;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import com.ibm.disni.util.MemoryUtils;
import com.ibm.disni.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class RamcastEndpointVerbCall {
  private static final Logger logger = LoggerFactory.getLogger(RamcastEndpoint.class);

  protected final int updatePostIndexOffset =
      100; // wr_id of the update post should be >> wr_id of the other. for differentiation

  protected ByteBuffer[] recvBufs;
  protected ByteBuffer[] sendBufs;
  protected ByteBuffer[] writeBufs;
  protected ByteBuffer[] readBufs;
  protected ByteBuffer[] updateBufs;
  protected ByteBuffer readTsBuf;
  protected SVCPostRecv[] recvCall;
  protected SVCPostSend[] sendCall;
  protected SVCPostSend[] writeCall;
  protected SVCPostSend[] readCall;
  protected SVCPostSend[] updateCall;
  protected SVCPostSend readTsCall;
  protected LinkedBlockingQueue<SVCPostSend> freeSendPostSend;
  protected LinkedBlockingQueue<SVCPostSend> freeWritePostSend;
  protected LinkedBlockingQueue<SVCPostSend> freeUpdatePostSend;
  protected LinkedBlockingQueue<SVCPostSend> freeReadPostSend;
  protected Map<Integer, SVCPostSend> pendingSendPostSend;
  protected Map<Integer, SVCPostSend> pendingWritePostSend;
  protected Map<Integer, SVCPostSend> pendingUpdatePostSend;
  protected Map<Integer, SVCPostSend> pendingReadPostSend;
  RamcastEndpoint endpoint;
  private RamcastConfig config = RamcastConfig.getInstance();
  private IbvMr dataMr;
  private int queueLength;
  private int packageSize;
  private int signalSize;
  private int tsBlockSize;

  public RamcastEndpointVerbCall(RamcastEndpoint endpoint) throws IOException {
    this.endpoint = endpoint;
    this.queueLength = config.getQueueLength();
    this.packageSize = RamcastConfig.SIZE_MESSAGE;
    this.signalSize = RamcastConfig.SIZE_SIGNAL;
    this.tsBlockSize = endpoint.getEndpointGroup().getTimestampBlock().getCapacity();

    this.recvBufs = new ByteBuffer[queueLength];
    this.sendBufs = new ByteBuffer[queueLength];
    this.recvCall = new SVCPostRecv[queueLength];
    this.sendCall = new SVCPostSend[queueLength];
    this.readBufs = new ByteBuffer[queueLength];
    this.readCall = new SVCPostSend[queueLength];
    this.writeBufs = new ByteBuffer[queueLength];
    this.writeCall = new SVCPostSend[queueLength];
    this.updateBufs = new ByteBuffer[queueLength];
    this.updateCall = new SVCPostSend[queueLength];

    this.freeSendPostSend = new LinkedBlockingQueue<>(queueLength);
    this.freeWritePostSend = new LinkedBlockingQueue<>(queueLength);
    this.freeUpdatePostSend = new LinkedBlockingQueue<>(queueLength);
    this.freeReadPostSend = new LinkedBlockingQueue<>(queueLength);
    this.pendingSendPostSend = new ConcurrentHashMap<>();
    this.pendingWritePostSend = new ConcurrentHashMap<>();
    this.pendingUpdatePostSend = new ConcurrentHashMap<>();

    // setting up buffer for RDMA verbs:
    ByteBuffer sendBuffer;
    ByteBuffer receiveBuffer;
    ByteBuffer writeBuffer;
    ByteBuffer readBuffer;
    ByteBuffer updateBuffer;
    ByteBuffer readTsBuffer;

    // we use only one memory buffer for RDMA verbs send/recv/read/write and update
    ByteBuffer dataBuffer =
        ByteBuffer.allocateDirect(packageSize * queueLength * 4 + queueLength * signalSize + tsBlockSize);
    /* Only do one memory registration with the IB card. */
    dataMr = endpoint.registerMemory(dataBuffer).execute().free().getMr();

    /* Receive memory region is the first part of the main buffer. */
    receiveBuffer = dataBuffer.slice();

    /* Send memory region is the second part of the main buffer. */
    int sendBufferOffset = queueLength * packageSize;
    dataBuffer.position(sendBufferOffset);
    dataBuffer.limit(dataBuffer.position() + queueLength * packageSize);
    sendBuffer = dataBuffer.slice();

    /* Read memory region is the third part of the main buffer. */
    int readBufferOffset = sendBufferOffset + (queueLength * packageSize);
    dataBuffer.position(readBufferOffset);
    dataBuffer.limit(dataBuffer.position() + queueLength * packageSize);
    readBuffer = dataBuffer.slice();

    /* Write memory region is the fourth part of the main buffer. */
    int writeBufferOffset = readBufferOffset + (queueLength * packageSize);
    dataBuffer.position(writeBufferOffset);
    dataBuffer.limit(dataBuffer.position() + queueLength * packageSize);
    writeBuffer = dataBuffer.slice();

    /* Update memory region is the last part of the main buffer. */
    int updateBufferOffset = writeBufferOffset + (queueLength * packageSize);
    dataBuffer.position(updateBufferOffset);
    dataBuffer.limit(dataBuffer.position() + queueLength * signalSize);
    updateBuffer = dataBuffer.slice();

    /* Special memory region for reading Timestamp block for leader election */
    int readTsBufferOffset = updateBufferOffset + (queueLength * signalSize);
    dataBuffer.position(readTsBufferOffset);
    dataBuffer.limit(
        dataBuffer.position() + tsBlockSize);
    readTsBuf = dataBuffer.slice();
    readTsCall = setupReadTsTask();

    for (int i = 0; i < queueLength; i++) {
      /* Create single receive buffers within the receive region in form of slices. */
      receiveBuffer.position(i * packageSize);
      receiveBuffer.limit(receiveBuffer.position() + packageSize);
      recvBufs[i] = receiveBuffer.slice();

      /* Create single send buffers within the send region in form of slices. */
      sendBuffer.position(i * packageSize);
      sendBuffer.limit(sendBuffer.position() + packageSize);
      sendBufs[i] = sendBuffer.slice();

      readBuffer.position(i * packageSize);
      readBuffer.limit(readBuffer.position() + packageSize);
      readBufs[i] = readBuffer.slice();

      writeBuffer.position(i * packageSize);
      writeBuffer.limit(writeBuffer.position() + packageSize);
      writeBufs[i] = writeBuffer.slice();

      updateBuffer.position(i * signalSize);
      updateBuffer.limit(updateBuffer.position() + signalSize);
      updateBufs[i] = updateBuffer.slice();

      this.recvCall[i] = setupRecvTask(i);
      this.sendCall[i] = setupSendTask(i);
      this.readCall[i] = setupReadTask(i);
      this.writeCall[i] = setupWriteTask(i);
      this.updateCall[i] = setupUpdateTask(i);

      freeSendPostSend.add(sendCall[i]);
      freeWritePostSend.add(writeCall[i]);
      freeUpdatePostSend.add(updateCall[i]);
      freeReadPostSend.add(readCall[i]);

      recvCall[i].execute();
    }
  }

  private SVCPostSend setupUpdateTask(int wrid) throws IOException {
    ArrayList<IbvSendWR> updateWRs = new ArrayList<>(1);
    LinkedList<IbvSge> sgeList = new LinkedList<>();

    IbvSge sge = new IbvSge();
    sge.setAddr(MemoryUtils.getAddress(updateBufs[wrid]));
    sge.setLength(signalSize);
    sge.setLkey(dataMr.getLkey());
    sgeList.add(sge);

    IbvSendWR updateWR = new IbvSendWR();
    updateWR.setSg_list(sgeList);
    updateWR.setWr_id(updatePostIndexOffset + wrid);
    updateWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_RDMA_WRITE.ordinal());
    updateWRs.add(updateWR);
    return this.endpoint.postSend(updateWRs);
  }

  private SVCPostSend setupWriteTask(int wrid) throws IOException {
    ArrayList<IbvSendWR> writeWRs = new ArrayList<>(1);
    LinkedList<IbvSge> sgeList = new LinkedList<>();

    IbvSge sge = new IbvSge();
    sge.setAddr(MemoryUtils.getAddress(writeBufs[wrid]));
    sge.setLength(packageSize);
    sge.setLkey(dataMr.getLkey());
    sgeList.add(sge);

    IbvSendWR writeWR = new IbvSendWR();
    writeWR.setSg_list(sgeList);
    writeWR.setWr_id(wrid);
    writeWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_RDMA_WRITE.ordinal());
    writeWRs.add(writeWR);
    return this.endpoint.postSend(writeWRs);
  }

  private SVCPostSend setupReadTask(int wrid) throws IOException {
    ArrayList<IbvSendWR> readWRs = new ArrayList<>(1);
    LinkedList<IbvSge> sgeList = new LinkedList<>();

    IbvSge sge = new IbvSge();

    sge.setAddr(MemoryUtils.getAddress(readBufs[wrid]));
    sge.setLength(packageSize);
    sge.setLkey(dataMr.getLkey());
    sgeList.add(sge);

    IbvSendWR readWR = new IbvSendWR();
    readWR.setSg_list(sgeList);
    readWR.setWr_id(wrid);
    readWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_RDMA_READ.ordinal());
    readWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
    readWRs.add(readWR);

    return this.endpoint.postSend(readWRs);
  }

  private SVCPostSend setupReadTsTask() throws IOException {
    ArrayList<IbvSendWR> readWRs = new ArrayList<>(1);
    LinkedList<IbvSge> sgeList = new LinkedList<>();

    IbvSge sge = new IbvSge();

    sge.setAddr(MemoryUtils.getAddress(readTsBuf));
    sge.setLength(tsBlockSize);
    sge.setLkey(dataMr.getLkey());
    sgeList.add(sge);

    IbvSendWR readWR = new IbvSendWR();
    readWR.setSg_list(sgeList);
    readWR.setWr_id(1000);
    readWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_RDMA_READ.ordinal());
    readWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
    readWRs.add(readWR);

    return this.endpoint.postSend(readWRs);
  }

  private SVCPostSend setupSendTask(int wrid) throws IOException {
    ArrayList<IbvSendWR> sendWRs = new ArrayList<>(1);
    LinkedList<IbvSge> sgeList = new LinkedList<>();

    IbvSge sge = new IbvSge();
    sge.setAddr(MemoryUtils.getAddress(sendBufs[wrid]));
    sge.setLength(packageSize);
    sge.setLkey(dataMr.getLkey());
    sgeList.add(sge);

    IbvSendWR sendWR = new IbvSendWR();
    sendWR.setSg_list(sgeList);
    sendWR.setWr_id(wrid);
    sendWRs.add(sendWR);
    sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
    sendWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());

    return this.endpoint.postSend(sendWRs);
  }

  private SVCPostRecv setupRecvTask(int wrid) throws IOException {
    ArrayList<IbvRecvWR> recvWRs = new ArrayList<>(1);
    LinkedList<IbvSge> sgeList = new LinkedList<>();

    IbvSge sge = new IbvSge();
    sge.setAddr(MemoryUtils.getAddress(recvBufs[wrid]));
    sge.setLength(packageSize);
    sge.setLkey(dataMr.getLkey());
    sgeList.add(sge);

    IbvRecvWR recvWR = new IbvRecvWR();
    recvWR.setWr_id(wrid);
    recvWR.setSg_list(sgeList);
    recvWRs.add(recvWR);

    return this.endpoint.postRecv(recvWRs);
  }

  public void freeSend(int index) throws IOException {
    SVCPostSend sendOperation = pendingSendPostSend.remove(index);
    if (sendOperation == null) {
      throw new IOException(this.endpoint.getNode() + " no pending index " + index);
    }
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("[{}/{}] adding back send postsend", this.endpoint.getEndpointId(), index);
    this.freeSendPostSend.add(sendOperation);
  }

  public void freeWrite(int index) throws IOException {
    SVCPostSend sendOperation = pendingWritePostSend.remove(index);
    if (sendOperation == null) {
      throw new IOException(this.endpoint.getNode() + " no pending index " + index);
    }
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("[{}/{}] adding back write postsend", this.endpoint.getEndpointId(), index);
    this.freeWritePostSend.add(sendOperation);
  }

  public void freeUpdate(int index) throws IOException {
    SVCPostSend sendOperation = pendingUpdatePostSend.remove(index);
    if (sendOperation == null) {
      return;
//      throw new IOException(this.endpoint.getNode() + " no pending index " + index);
    }
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("[{}/{}] adding back update postsend", this.endpoint.getEndpointId(), index);
    this.freeUpdatePostSend.add(sendOperation);
    //    }
  }

  protected void postRecv(int index) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("[{}/{}] execute postrecv", this.endpoint.getEndpointId(), index);
    recvCall[index].execute();
  }

  public void close() {
    dataMr.close();
  }
}
