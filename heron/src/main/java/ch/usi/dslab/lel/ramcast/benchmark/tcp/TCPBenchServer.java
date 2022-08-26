package ch.usi.dslab.lel.ramcast.benchmark.tcp;

import ch.usi.dslab.bezerra.netwrapper.tcp.TCPMessage;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPReceiver;
import ch.usi.dslab.bezerra.netwrapper.tcp.TCPSender;
import ch.usi.dslab.lel.ramcast.benchmark.ServerCmdLineCommon;
import org.apache.commons.cli.ParseException;

public class TCPBenchServer {
  public static void main(String[] args) throws ParseException {
    ServerCmdLineCommon options = new ServerCmdLineCommon("TCPBenchServer");
    try {
      options.parse(args);
    } catch (ParseException e) {
      options.printHelp();
      System.exit(-1);
    }
    TCPReceiver receiver = new TCPReceiver(options.getServerPort());
    TCPSender sender = new TCPSender();

    while (!Thread.interrupted()) {
      TCPMessage msg = receiver.receive();
      sender.send(msg.getContents(), msg.getConnection());
    }
  }
}
