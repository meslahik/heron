/*
 * DiSNI: Direct Storage and Networking Interface
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016-2018, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package ch.usi.dslab.lel.ramcast.benchmark.rdma;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyDistributionPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.ramcast.benchmark.ClientCmdLineCommon;
import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.RdmaPassiveEndpointGroup;
import com.ibm.disni.verbs.*;
import org.apache.commons.cli.ParseException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;

public class ReadBenchClient implements RdmaEndpointFactory<ReadBenchClient.ReadClientEndpoint> {
  private RdmaPassiveEndpointGroup<ReadClientEndpoint> group;
  private String host;
  private int port;
  private int size;

  private ThroughputPassiveMonitor tpMonitor;
  private LatencyPassiveMonitor latMonitor;
  private LatencyDistributionPassiveMonitor cdfMonitor;


  public ReadBenchClient(String host, int port, int size) throws IOException {
    this.group = new RdmaPassiveEndpointGroup<ReadClientEndpoint>(1, 10, 4, 40);
    this.group.init(this);
    this.host = host;
    this.port = port;
    this.size = size;

    tpMonitor = new ThroughputPassiveMonitor(1, "client_overall", true);
    latMonitor = new LatencyPassiveMonitor(1, "client_overall", true);
    cdfMonitor = new LatencyDistributionPassiveMonitor(1, "client_overall", true);
  }

  public static void main(String[] args) throws Exception {
    ClientCmdLineCommon options = new ClientCmdLineCommon("RdmaSendReceiveBenchClient");
    try {
      options.parse(args);
    } catch (ParseException e) {
      options.printHelp();
      System.exit(-1);
    }
    DataGatherer.configure(options.getExperimentDuration(), options.getFileDirectory(), options.getGathererHost(), options.getGathererPort(), options.getWarmUpTime());


    ReadBenchClient client = new ReadBenchClient(options.getServerAddress(),
            options.getServerPort(), options.getPayloadSize());
    client.run();
  }

  public ReadBenchClient.ReadClientEndpoint createEndpoint(RdmaCmId id, boolean serverSide)
          throws IOException {
    return new ReadClientEndpoint(group, id, serverSide, size);
  }

  private void run() throws Exception {
    System.out.println("ReadClient, size " + size);

    ReadBenchClient.ReadClientEndpoint endpoint = group.createEndpoint();
    InetAddress ipAddress = InetAddress.getByName(host);
    InetSocketAddress address = new InetSocketAddress(ipAddress, port);
    endpoint.connect(address, 1000);
    System.out.println("ReadClient, client connected, address " + host + ", port " + port);

    //in our custom endpoints we make sure CQ events get stored in a queue, we now query that queue for new CQ events.
    //in this case a new CQ event means we have received some data, i.e., a message from the server
    endpoint.pollUntil();
    ByteBuffer recvBuf = endpoint.getRecvBuf();
    //the message has been received in this buffer
    //it contains some RDMA information sent by the server
    recvBuf.clear();
    long addr = recvBuf.getLong();
    int length = recvBuf.getInt();
    int lkey = recvBuf.getInt(); // TODO: what is lkey? local key in remote machine, identifies memory registered?
    recvBuf.clear();
    System.out.println("ReadClient, receiving rdma information, addr " + addr + ", length " + length + ", key " + lkey);
    System.out.println("ReadClient, preparing read operation...");

    //the RDMA information above identifies a RDMA buffer at the server side
    //let's issue a one-sided RDMA read opeation to fetch the content from that buffer
    IbvSendWR sendWR = endpoint.getSendWR();
    sendWR.setWr_id(1001);
    sendWR.setOpcode(IbvSendWR.IBV_WR_RDMA_READ);
    sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
    sendWR.getRdma().setRemote_addr(addr);
    sendWR.getRdma().setRkey(lkey);
//		sendWR.getSge(0).setLength(length);

    //post the operation on the endpoint
    SVCPostSend postSend = endpoint.newPostSend();
    long sent, now;
    while (!Thread.interrupted()) {
      sent = System.nanoTime();
      postSend.execute();
      endpoint.pollUntil();
      now = System.nanoTime();
      tpMonitor.incrementCount();
      latMonitor.logLatency(sent, now);
      cdfMonitor.logLatencyForDistribution(sent, now);
      if (tpMonitor.getCount() % 50000 == 0) System.out.println("Total messages read: " + tpMonitor.getCount());
    }

    //close everything
    endpoint.close();
    group.close();

  }

  public static class ReadClientEndpoint extends RdmaEndpoint {
    private ByteBuffer buffers[];
    private IbvMr mrlist[];
    private int buffersize;

    private ByteBuffer dataBuf;
    private IbvMr dataMr;
    private ByteBuffer sendBuf;
    private ByteBuffer recvBuf;
    private IbvMr recvMr;

    private LinkedList<IbvSendWR> wrList_send;
    private IbvSge sgeSend;
    private LinkedList<IbvSge> sgeList;
    private IbvSendWR sendWR;

    private LinkedList<IbvRecvWR> wrList_recv;
    private IbvSge sgeRecv;
    private LinkedList<IbvSge> sgeListRecv;
    private IbvRecvWR recvWR;

    private IbvWC[] wcList;
    private SVCPollCq poll;

    protected ReadClientEndpoint(RdmaEndpointGroup<? extends RdmaEndpoint> group, RdmaCmId idPriv, boolean serverSide, int size) throws IOException {
      super(group, idPriv, serverSide);

      this.buffersize = size;
      buffers = new ByteBuffer[3];
      this.mrlist = new IbvMr[3];

      for (int i = 0; i < 3; i++) {
        buffers[i] = ByteBuffer.allocateDirect(buffersize);
      }

      this.wrList_send = new LinkedList<IbvSendWR>();
      this.sgeSend = new IbvSge();
      this.sgeList = new LinkedList<IbvSge>();
      this.sendWR = new IbvSendWR();

      this.wrList_recv = new LinkedList<IbvRecvWR>();
      this.sgeRecv = new IbvSge();
      this.sgeListRecv = new LinkedList<IbvSge>();
      this.recvWR = new IbvRecvWR();
    }

    public ByteBuffer getDataBuf() {
      return dataBuf;
    }

    public SVCPostSend newPostSend() throws IOException {
      return this.postSend(wrList_send);
    }

    public IbvSendWR getSendWR() {
      return sendWR;
    }

    public ByteBuffer getRecvBuf() {
      return recvBuf;
    }

    public void init() throws IOException {
      super.init();

      IbvCQ cq = getCqProvider().getCQ();
      this.wcList = new IbvWC[getCqProvider().getCqSize()];
      for (int i = 0; i < wcList.length; i++) {
        wcList[i] = new IbvWC();
      }
      this.poll = cq.poll(wcList, wcList.length);

      for (int i = 0; i < 3; i++) {
        mrlist[i] = registerMemory(buffers[i]).execute().free().getMr();
      }

      this.dataBuf = buffers[0];
      this.dataMr = mrlist[0];
      this.sendBuf = buffers[1];
      this.recvBuf = buffers[2];
      this.recvMr = mrlist[2];

      dataBuf.clear();
      sendBuf.clear();

      sgeSend.setAddr(dataMr.getAddr());
      sgeSend.setLength(dataMr.getLength());
      sgeSend.setLkey(dataMr.getLkey());
      sgeList.add(sgeSend);
      sendWR.setWr_id(2000);
      sendWR.setSg_list(sgeList);
      sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
      sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
      wrList_send.add(sendWR);


      sgeRecv.setAddr(recvMr.getAddr());
      sgeRecv.setLength(recvMr.getLength());
      int lkey = recvMr.getLkey();
      sgeRecv.setLkey(lkey);
      sgeListRecv.add(sgeRecv);
      recvWR.setSg_list(sgeListRecv);
      recvWR.setWr_id(2001);
      wrList_recv.add(recvWR);

      this.postRecv(wrList_recv).execute().free();
    }

    private int pollUntil() throws IOException {
      int res = 0;
      while (res == 0) {
        res = poll.execute().getPolls();
      }
      return res;
    }
  }
}
