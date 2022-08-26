package ch.usi.dslab.lel.ramcast.heron;

import ch.usi.dslab.bezerra.sense.DataGatherer;
import ch.usi.dslab.bezerra.sense.monitors.LatencyDistributionPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.LatencyPassiveMonitor;
import ch.usi.dslab.bezerra.sense.monitors.ThroughputPassiveMonitor;
import ch.usi.dslab.lel.ramcast.MessageDeliveredCallback;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class BenchAgent {

    private static final Logger logger = LoggerFactory.getLogger(BenchAgent.class);

    HeronAgent agent;
    private RamcastConfig config;

    int nodeId;
    int groupId;
    private int clientId;
    private boolean isClient;
    private int destinationFrom;
    private int destinationCount;

    private ThroughputPassiveMonitor tpMonitor;
    private LatencyPassiveMonitor latMonitor;
    private LatencyDistributionPassiveMonitor cdfMonitor;

    public static void main(String[] args) throws Exception {
        BenchAgent benchAgent = new BenchAgent();
        benchAgent.launch(args);
    }

    void launch(String[] args) throws Exception {

        Option nIdOption = Option.builder("nid").desc("node id").hasArg().build();
        Option gIdOption = Option.builder("gid").desc("group id").hasArg().build();
        Option cIdOption = Option.builder("cid").desc("client id").hasArg().build();
        Option configOption = Option.builder("c").required().desc("config file").hasArg().build();
        Option packageSizeOption = Option.builder("s").required().desc("sample package size").hasArg().build();
        Option gathererHostOption = Option.builder("gh").required().desc("gatherer host").hasArg().build();
        Option gathererPortOption = Option.builder("gp").required().desc("gatherer port").hasArg().build();
        Option gathererDirectoryOption = Option.builder("gd").required().desc("gatherer directory").hasArg().build();
        Option warmUpTimeOption = Option.builder("gw").required().desc("gatherer warmup time").hasArg().build();
        Option durationOption = Option.builder("d").required().desc("benchmark duration").hasArg().build();
        Option destinationFromOption = Option.builder("df").desc("destination from group (include)").hasArg().build();
        Option destinationCountOption = Option.builder("dc").desc("destination count").hasArg().build();
        Option isClientOption = Option.builder("isClient").desc("is client").hasArg().build();

        Options options = new Options();
        options.addOption(nIdOption);
        options.addOption(gIdOption);
        options.addOption(cIdOption);
        options.addOption(configOption);
        options.addOption(packageSizeOption);
        options.addOption(gathererHostOption);
        options.addOption(gathererPortOption);
        options.addOption(warmUpTimeOption);
        options.addOption(durationOption);
        options.addOption(destinationFromOption);
        options.addOption(destinationCountOption);
        options.addOption(gathererDirectoryOption);
        options.addOption(isClientOption);

        CommandLineParser parser = new DefaultParser();
        CommandLine line = parser.parse(options, args);

        nodeId = Integer.parseInt(line.getOptionValue(nIdOption.getOpt()));
        groupId = Integer.parseInt(line.getOptionValue(gIdOption.getOpt()));
        clientId = Integer.parseInt(line.getOptionValue(cIdOption.getOpt()));
        String configFile = line.getOptionValue(configOption.getOpt());
        int payloadSize = Integer.parseInt(line.getOptionValue(packageSizeOption.getOpt()));
        String gathererHost = line.getOptionValue(gathererHostOption.getOpt());
        int gathererPort = Integer.parseInt(line.getOptionValue(gathererPortOption.getOpt()));
        String fileDirectory = line.getOptionValue(gathererDirectoryOption.getOpt());
        int experimentDuration = Integer.parseInt(line.getOptionValue(durationOption.getOpt()));
        int warmUpTime = Integer.parseInt(line.getOptionValue(warmUpTimeOption.getOpt()));
        destinationFrom = line.getOptionValue(destinationFromOption.getOpt()) != null ? Integer.parseInt(line.getOptionValue(destinationFromOption.getOpt())) : 0;
        destinationCount = line.getOptionValue(destinationCountOption.getOpt()) != null ? Integer.parseInt(line.getOptionValue(destinationCountOption.getOpt())) : 0;
        isClient = line.getOptionValue(isClientOption.getOpt()) != null && Integer.parseInt(line.getOptionValue(isClientOption.getOpt())) == 1;

        // the full message size must be agreed on all processes otherwise Ramcast stop working.
        // todo: why? Isn't ot easier for overhead to be the same for messges with diff destination counts?
        RamcastConfig.SIZE_MESSAGE = payloadSize;
//        logger.info("overheads: {}, {}, {}, {}, {}",
//                RamcastMessage.calculateOverhead(1),
//                RamcastMessage.calculateOverhead(2),
//                RamcastMessage.calculateOverhead(4),
//                RamcastMessage.calculateOverhead(8),
//                RamcastMessage.calculateOverhead(16));

        config = RamcastConfig.getInstance();
        config.loadConfig(configFile);
        config.setPayloadSize(payloadSize);


        agent = new HeronAgent(groupId, nodeId, onExecute);

        boolean callbackMonitored = false;
        if (groupId >= destinationFrom && groupId < destinationFrom + destinationCount) callbackMonitored = true;

        DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
        if (isClient) {
            this.tpMonitor = new ThroughputPassiveMonitor(this.clientId, "client_overall", true);
            if (callbackMonitored) {
                if (RamcastConfig.LOG_ENABLED)
                    logger.info("Start extra monitor for latency");
                this.latMonitor = new LatencyPassiveMonitor(this.clientId, "client_overall", true);
                this.cdfMonitor = new LatencyDistributionPassiveMonitor(this.clientId, "client_overall", true);
                // create more datapoint. Default: 10k=> less point
                LatencyDistributionPassiveMonitor.setBucketWidthNano(100);
            }
        }

        if (isClient)
            startBenchmark();
    }

    // run by servers after delivery, a server is also a client to measure latency
    MessageDeliveredCallback onExecute = (data) -> {
        if (isClient && ((RamcastMessage) data).getMessage().getInt(0) == clientId) {
            long time = System.nanoTime();
            long startTime = ((RamcastMessage) data).getMessage().getLong(4);
            latMonitor.logLatency(startTime, time);
            cdfMonitor.logLatencyForDistribution(startTime, time);
        }

        RamcastMessage message = (RamcastMessage) data;
        if (message.getId() % 50000 == 0)
            logger.info("sent messages: {}", message.getId());
//            releasePermit();
    };

    void startBenchmark() throws Exception {
        Thread.sleep(2000); // wait for connections to establish

        ArrayList<RamcastGroup> dest = new ArrayList<>();
        for (int i = 0; i < destinationCount; i++)
            dest.add(RamcastGroup.getGroup(destinationFrom + i));

        int payloadSize = RamcastConfig.SIZE_MESSAGE - RamcastMessage.calculateOverhead(destinationCount);
        if (RamcastConfig.LOG_ENABLED)
            logger.info("payload size: {}, destinations: {}, overhead: {}, real payload size {}",
                    RamcastConfig.SIZE_MESSAGE,
                    destinationCount,
                    RamcastMessage.calculateOverhead(destinationCount),
                    payloadSize);
        ByteBuffer buffer = ByteBuffer.allocateDirect(payloadSize);
        buffer.putInt(clientId);
        while (buffer.remaining() > 0) buffer.put((byte) 1);

        int msgeId = 0;

        while (true) {
            long startTime = System.nanoTime();
            buffer.putLong(4, startTime);

            agent.execute(++msgeId, buffer, dest);
            tpMonitor.incrementCount();
            if (msgeId % 10000 == 0)
                logger.info("sent messages: {}", msgeId);

            if (RamcastConfig.DELAY)
                try {
                    Thread.sleep(50000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }
}
