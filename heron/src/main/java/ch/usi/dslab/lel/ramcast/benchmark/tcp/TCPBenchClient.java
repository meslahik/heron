package ch.usi.dslab.lel.ramcast.benchmark.tcp;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPConnection;
import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyDistributionPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.ramcast.benchmark.ClientCmdLineCommon;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

public class TCPBenchClient {

  private ThroughputPassiveMonitor tpMonitor;
  private LatencyPassiveMonitor latMonitor;
  private LatencyDistributionPassiveMonitor cdfMonitor;

  public static void main(String[] args) throws IOException, ParseException, InterruptedException {

    TCPBenchClient app = new TCPBenchClient();
    app.bench(args);
  }

  public void bench(String[] args) throws IOException, ParseException {
    ClientCmdLineCommon options = new ClientCmdLineCommon("TCPBenchClient");
    try {
      options.parse(args);
    } catch (ParseException e) {
      options.printHelp();
      System.exit(-1);
    }

    DataGatherer.configure(options.getExperimentDuration(), options.getFileDirectory(), options.getGathererHost(), options.getGathererPort(), options.getWarmUpTime());
    byte[] payload = new byte[options.getPayloadSize()];
    int i = 0;
    while (i < payload.length) payload[i++] = 1;

    Message message = new Message(payload);
    System.out.println("Payload size: " + options.getPayloadSize() + " Message size: " + message.getByteBufferWithLengthHeader().capacity());
    TCPConnection serverConnection = new TCPConnection(options.getServerAddress(), options.getServerPort());

    long now;
    long sent;
    tpMonitor = new ThroughputPassiveMonitor(1, "client_overall", true);
    latMonitor = new LatencyPassiveMonitor(1, "client_overall", true);
    cdfMonitor = new LatencyDistributionPassiveMonitor(1, "client_overall", true);
    while (!Thread.interrupted()) {
      sent = System.nanoTime();
      serverConnection.sendBusyWait(message);
      serverConnection.receiveBusyWait();
      now = System.nanoTime();
      tpMonitor.incrementCount();
      latMonitor.logLatency(sent, now);
      cdfMonitor.logLatencyForDistribution(sent, now);
      if (tpMonitor.getCount() % 50000 == 0)
        System.out.println("Total TCP messages sent: " + tpMonitor.getCount());
    }
  }
}
