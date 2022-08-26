package ch.usi.dslab.lel.ramcast.benchmark.rdma;

import ch.usi.dslab.lel.ramcast.benchmark.ServerCmdLineCommon;
import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaPassiveEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;

public class SendReceiveBenchServer implements RdmaEndpointFactory<SendReceiveBenchServer.SendRecvEndpoint> {
  private RdmaPassiveEndpointGroup<SendRecvEndpoint> group;
  private String host;
  private int port;
  private int size;
  private int recvQueueSize;

  public SendReceiveBenchServer(String host, int port, int size) throws IOException {
    this.recvQueueSize = 1;
    this.group = new RdmaPassiveEndpointGroup<>(1, this.recvQueueSize, 1, this.recvQueueSize * 2);
    this.group.init(this);
    this.host = host;
    this.port = port;
    this.size = size;
  }

  public static void main(String[] args) throws Exception {
    ServerCmdLineCommon options = new ServerCmdLineCommon("RdmaSendReceiveBenchServer");

    try {
      options.parse(args);
    } catch (ParseException e) {
      options.printHelp();
      System.exit(-1);
    }

    SendReceiveBenchServer server = new SendReceiveBenchServer(options.getServerAddress(),
            options.getServerPort(), options.getPayloadSize());
    server.run();
  }

  public SendReceiveBenchServer.SendRecvEndpoint createEndpoint(RdmaCmId id, boolean serverSide)
          throws IOException {
    return new SendRecvEndpoint(group, id, serverSide, size, recvQueueSize);
  }

  private void run() throws Exception {
    System.out.println("RdmaSendReceiveBenchServer, size " + size);

    RdmaServerEndpoint<SendReceiveBenchServer.SendRecvEndpoint> serverEndpoint = group.createServerEndpoint();
    InetAddress ipAddress = InetAddress.getByName(host);
    InetSocketAddress address = new InetSocketAddress(ipAddress, port);
    serverEndpoint.bind(address, 10);
    SendReceiveBenchServer.SendRecvEndpoint endpoint = serverEndpoint.accept();
    System.out.println("RdmaSendReceiveBenchServer, client connected, address " + address.toString());

    int opCount = 0;
    while (!Thread.interrupted()) {
      endpoint.pollRecvs();
      int actualRecvs = endpoint.installRecvs();
      int actualSends = endpoint.replySends(actualRecvs);
      opCount += actualSends;
    }

    endpoint.awaitRecvs();
    endpoint.awaitSends();

    //close everything
    endpoint.close();
    serverEndpoint.close();
    group.close();
  }

  public static class SendRecvEndpoint extends RdmaEndpoint {
    private int bufferSize;
    private int pipelineLength;
    private ByteBuffer[] recvBufs;
    private ByteBuffer[] sendBufs;
    private IbvMr[] recvMRs;
    private IbvMr[] sendMRs;
    private SVCPostRecv[] recvCall;
    private SVCPostSend[] sendCall;
    private IbvWC[] wcList;
    private SVCPollCq poll;
    private int recvIndex;
    private int sendIndex;
    private int sendBudget;
    private int recvBudget;
    private int sendPending;
    private int recvPending;

    protected SendRecvEndpoint(RdmaPassiveEndpointGroup<? extends RdmaEndpoint> group, RdmaCmId idPriv, boolean serverSide, int bufferSize, int recvQueueSize) throws IOException {
      super(group, idPriv, serverSide);
      this.bufferSize = bufferSize;
      this.pipelineLength = recvQueueSize;
      this.recvBufs = new ByteBuffer[pipelineLength];
      this.sendBufs = new ByteBuffer[pipelineLength];
      this.recvCall = new SVCPostRecv[pipelineLength];
      this.sendCall = new SVCPostSend[pipelineLength];
      this.recvMRs = new IbvMr[pipelineLength];
      this.sendMRs = new IbvMr[pipelineLength];
      this.recvIndex = 0;
      this.sendIndex = 0;
      this.sendBudget = pipelineLength;
      this.recvBudget = pipelineLength;
      this.sendPending = 0;
      this.recvPending = 0;
    }

    public int replySends(int res) throws IOException {
      int ready = Math.min(res, sendBudget);
      for (int i = 0; i < ready; i++) {
        int index = sendIndex % pipelineLength;
        sendIndex++;
//				System.out.println("sending response, wrid " + sendCall[index].getWrMod(0).getWr_id());
        sendCall[index].execute();
        this.sendBudget--;
        this.sendPending++;
      }
      return ready;
    }

    public int installRecvs() throws IOException {
      int ready = recvBudget;
      for (int i = 0; i < ready; i++) {
        int index = recvIndex % pipelineLength;
        recvIndex++;
//				System.out.println("installing recv, wrid " + recvCall[index].getWrMod(0).getWr_id());
        recvCall[index].execute();
        recvBudget--;
        recvPending++;
      }
      return ready;
    }

    private int pollRecvs() throws IOException {
      if (recvPending == 0) {
        return 0;
      }

      int recvCount = 0;
      while (recvCount == 0) {
        int res = 0;
        while (res == 0) {
          res = poll.execute().getPolls();
        }
        for (int i = 0; i < res; i++) {
          if (wcList[i].getOpcode() == 128) {
//						System.out.println("recv from wrid " + wcList[i].getWr_id());
            recvCount++;
            recvBudget++;
            recvPending--;
          } else {
            sendBudget++;
            sendPending--;
          }
        }
      }
      return recvCount;
    }

    private void awaitRecvs() throws IOException {
      while (recvPending > 0) {
        int res = poll.execute().getPolls();
        if (res > 0) {
          for (int i = 0; i < res; i++) {
            if (wcList[i].getOpcode() == 128) {
//							System.out.println("recv from wrid " + wcList[i].getWr_id());
              recvBudget++;
              recvPending--;
            } else {
              sendBudget++;
              sendPending--;
            }
          }
        }
      }
    }

    private void awaitSends() throws IOException {
      while (sendPending > 0) {
        int res = poll.execute().getPolls();
        if (res > 0) {
          for (int i = 0; i < res; i++) {
            if (wcList[i].getOpcode() == 128) {
//							System.out.println("recv from wrid " + wcList[i].getWr_id());
              recvBudget++;
              recvPending--;
            } else {
              sendBudget++;
              sendPending--;
            }
          }
        }
      }
    }

    @Override
    protected synchronized void init() throws IOException {
      super.init();

      IbvCQ cq = getCqProvider().getCQ();
      this.wcList = new IbvWC[getCqProvider().getCqSize()];
      for (int i = 0; i < wcList.length; i++) {
        wcList[i] = new IbvWC();
      }
      this.poll = cq.poll(wcList, wcList.length);

      for (int i = 0; i < pipelineLength; i++) {
        recvBufs[i] = ByteBuffer.allocateDirect(bufferSize);
        sendBufs[i] = ByteBuffer.allocateDirect(bufferSize);
        this.recvCall[i] = setupRecvTask(recvBufs[i], i);
        this.sendCall[i] = setupSendTask(sendBufs[i], i);
      }

      this.installRecvs();
    }

    public synchronized void close() throws IOException, InterruptedException {
      super.close();
      for (int i = 0; i < pipelineLength; i++) {
        deregisterMemory(recvMRs[i]);
        deregisterMemory(sendMRs[i]);
      }
    }

    private SVCPostSend setupSendTask(ByteBuffer sendBuf, int wrid) throws IOException {
      ArrayList<IbvSendWR> sendWRs = new ArrayList<IbvSendWR>(1);
      LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();

      IbvMr mr = registerMemory(sendBuf).execute().free().getMr();
      sendMRs[wrid] = mr;
      IbvSge sge = new IbvSge();
      sge.setAddr(mr.getAddr());
      sge.setLength(mr.getLength());
      int lkey = mr.getLkey();
      sge.setLkey(lkey);
      sgeList.add(sge);

      IbvSendWR sendWR = new IbvSendWR();
      sendWR.setSg_list(sgeList);
      sendWR.setWr_id(wrid);
      sendWRs.add(sendWR);
      sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
      sendWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());

      return postSend(sendWRs);
    }

    private SVCPostRecv setupRecvTask(ByteBuffer recvBuf, int wrid) throws IOException {
      ArrayList<IbvRecvWR> recvWRs = new ArrayList<IbvRecvWR>(1);
      LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();

      IbvMr mr = registerMemory(recvBuf).execute().free().getMr();
      recvMRs[wrid] = mr;
      IbvSge sge = new IbvSge();
      sge.setAddr(mr.getAddr());
      sge.setLength(mr.getLength());
      int lkey = mr.getLkey();
      sge.setLkey(lkey);
      sgeList.add(sge);

      IbvRecvWR recvWR = new IbvRecvWR();
      recvWR.setWr_id(wrid);
      recvWR.setSg_list(sgeList);
      recvWRs.add(recvWR);

      return postRecv(recvWRs);
    }
  }
}