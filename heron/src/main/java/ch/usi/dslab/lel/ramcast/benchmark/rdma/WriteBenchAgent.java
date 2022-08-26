/*
 * Ramcast: Data Center Remote Procedure Call
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
import ch.usi.dslab.lel.ramcast.MessageDeliveredCallback;
import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;

public class WriteBenchAgent {
  private static final Logger logger = LoggerFactory.getLogger(WriteBenchAgent.class);
  Semaphore sendPermits;
  private ThroughputPassiveMonitor tpMonitor;
  private LatencyPassiveMonitor latMonitor;
  private LatencyDistributionPassiveMonitor cdfMonitor;

  private RamcastConfig config;
  private RamcastAgent agent;
  private int clientId;
  private boolean isClient;
  private long startTime;

  private boolean isRunning = true;

  private MessageDeliveredCallback onDeliverAmcast = data -> {
  };

  public static void main(String[] args) throws Exception {
    WriteBenchAgent benchAgent = new WriteBenchAgent();
    benchAgent.launch(args);
  }

  public void launch(String[] args) throws Exception {
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
    options.addOption(gathererDirectoryOption);
    options.addOption(isClientOption);

    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);

    int nodeId = Integer.parseInt(line.getOptionValue(nIdOption.getOpt()));
    int groupId = Integer.parseInt(line.getOptionValue(gIdOption.getOpt()));
    clientId = Integer.parseInt(line.getOptionValue(cIdOption.getOpt()));
    String configFile = line.getOptionValue(configOption.getOpt());
    int payloadSize = Integer.parseInt(line.getOptionValue(packageSizeOption.getOpt()));
    String gathererHost = line.getOptionValue(gathererHostOption.getOpt());
    int gathererPort = Integer.parseInt(line.getOptionValue(gathererPortOption.getOpt()));
    String fileDirectory = line.getOptionValue(gathererDirectoryOption.getOpt());
    int experimentDuration = Integer.parseInt(line.getOptionValue(durationOption.getOpt()));
    int warmUpTime = Integer.parseInt(line.getOptionValue(warmUpTimeOption.getOpt()));
    isClient = line.getOptionValue(isClientOption.getOpt()) != null && Integer.parseInt(line.getOptionValue(isClientOption.getOpt())) == 1;

    RamcastConfig.SIZE_MESSAGE = payloadSize;
    RamcastConfig.SIZE_REMOTE_HEAD = payloadSize;
    RamcastConfig.SIZE_SIGNAL = payloadSize;
    config = RamcastConfig.getInstance();
    config.loadConfig(configFile);
    config.setPayloadSize(payloadSize);

    this.agent = new RamcastAgent(groupId, nodeId, onDeliverAmcast);

    boolean callbackMonitored = false;
    if (isClient) callbackMonitored = true;

    DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
//    this.tpMonitorServer = new ThroughputPassiveMonitor(this.clientId, "server_overall", true);
    if (isClient) {
      this.tpMonitor = new ThroughputPassiveMonitor(this.clientId, "client_overall", true);
      logger.info("Start extra monitor for latency");
      this.latMonitor = new LatencyPassiveMonitor(this.clientId, "client_overall", true);
      this.cdfMonitor = new LatencyDistributionPassiveMonitor(this.clientId, "client_overall", true);
      // create more datapoint. Default: 10k=> less point
      LatencyDistributionPassiveMonitor.setBucketWidthNano(100);
    }

    this.agent.bind();
    Thread.sleep(2000);
    this.agent.establishConnections();
    logger.info("NODE READY");
    Thread.sleep(2000);

    this.startBenchmarkSync();
  }

  private void startBenchmarkSync() throws IOException, InterruptedException {
    logger.info("Node {} start benchmarking", this.agent.getNode());

    if (isClient) {
      // todo: what the heck is 50? message overhead?
      int payloadSize = RamcastConfig.SIZE_MESSAGE - 50;

      // allocate buffer/payload
      ByteBuffer buffer = ByteBuffer.allocateDirect(payloadSize);
      buffer.putInt(clientId);
      while (buffer.remaining() > 0) buffer.put((byte) 1);

      // todo: only one dest group?
      // todo: one message is defined to be reused
      List<RamcastGroup> dest = new ArrayList<>();
      dest.add(RamcastGroup.getGroup(0));
      RamcastMessage sampleMessage = this.agent.createMessage(0, buffer, dest);
      int i = 0;

      logger.info("Client node {} sending request to destination {} with payload size={}", this.agent.getNode(), dest, payloadSize);
      while (isRunning) {
        if (RamcastConfig.DELAY)
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

        // assign a new hash ID each time to the message to make it different
        // todo: why hash (unique id) of message is needed?
        int id = Objects.hash(i++, this.clientId);
        sampleMessage.setId(id);

        // find out free slots in all remote processes to write this message (todo: remote circular buffer?)
        this.agent.setSlot(sampleMessage, dest);

        // include start time in payload.
        startTime = System.nanoTime();
        buffer.putLong(4, startTime);

        if (RamcastConfig.LOG_ENABLED) {
          logger.debug("Client {} start new request {} msgId [{}]", clientId, i, id);
        }

        // write message asynchronously
        // todo: how does it async?
        agent.writeSync(sampleMessage, dest);

        // update lat, tp, cdf
        long time = System.nanoTime();
        latMonitor.logLatency(startTime, time);
        cdfMonitor.logLatencyForDistribution(startTime, time);
        tpMonitor.incrementCount();
        if (tpMonitor.getCount() % 50000 == 0)
          logger.info("Total messages sent: {}", tpMonitor.getCount());

      }
    }
  }
}
