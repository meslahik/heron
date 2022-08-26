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

package ch.usi.dslab.lel.ramcast.benchmark;

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

public class BenchAgent {
  private static final Logger logger = LoggerFactory.getLogger(BenchAgent.class);
  Semaphore sendPermits;
  private ThroughputPassiveMonitor tpMonitor;
  //  private ThroughputPassiveMonitor tpMonitorServer;
  private LatencyPassiveMonitor latMonitor;
  private LatencyDistributionPassiveMonitor cdfMonitor;

  private RamcastConfig config;
  private RamcastAgent agent;
  private int clientId;
  private boolean isClient;
  private int destinationFrom;
  private int destinationCount;
  private long startTime;

  private boolean isRunning = true;

  private MessageDeliveredCallback onDeliverAmcast =
          new MessageDeliveredCallback() {
            @Override
            public void call(Object data) {
              if (isClient && ((RamcastMessage) data).getMessage().getInt(0) == clientId) {
                releasePermit();
                long time = System.nanoTime();
                long startTime = ((RamcastMessage) data).getMessage().getLong(4);
                latMonitor.logLatency(startTime, time);
                cdfMonitor.logLatencyForDistribution(startTime, time);
              }
            }
          };

  public static void main(String[] args) throws Exception {
    BenchAgent benchAgent = new BenchAgent();
    benchAgent.launch(args);
  }

  void getPermit() {
    try {
      sendPermits.acquire();
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void releasePermit() {
    sendPermits.release();
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
    destinationFrom = line.getOptionValue(destinationFromOption.getOpt()) != null ? Integer.parseInt(line.getOptionValue(destinationFromOption.getOpt())) : 0;
    destinationCount = line.getOptionValue(destinationCountOption.getOpt()) != null ? Integer.parseInt(line.getOptionValue(destinationCountOption.getOpt())) : 0;
    isClient = line.getOptionValue(isClientOption.getOpt()) != null && Integer.parseInt(line.getOptionValue(isClientOption.getOpt())) == 1;

    RamcastConfig.SIZE_MESSAGE = payloadSize;
    config = RamcastConfig.getInstance();
    config.loadConfig(configFile);
    config.setPayloadSize(payloadSize);

    this.agent = new RamcastAgent(groupId, nodeId, onDeliverAmcast);

    // if a client in a server group, it measures latency.
    // for other clients there is no latency measurement
    boolean callbackMonitored = false;
    if (groupId >= destinationFrom && groupId < destinationFrom + destinationCount) callbackMonitored = true;

    DataGatherer.configure(experimentDuration, fileDirectory, gathererHost, gathererPort, warmUpTime);
//    this.tpMonitorServer = new ThroughputPassiveMonitor(this.clientId, "server_overall", true);
    if (isClient) {
      this.tpMonitor = new ThroughputPassiveMonitor(this.clientId, "client_overall", true);
      if (callbackMonitored) {
        logger.info("Start extra monitor for latency");
        this.latMonitor = new LatencyPassiveMonitor(this.clientId, "client_overall", true);
        this.cdfMonitor = new LatencyDistributionPassiveMonitor(this.clientId, "client_overall", true);
        // create more datapoint. Default: 10k=> less point
        LatencyDistributionPassiveMonitor.setBucketWidthNano(100);
      }
    }

    this.agent.bind();
    Thread.sleep(2000);
    this.agent.establishConnections();
    logger.info("NODE READY");
    Thread.sleep(2000);

//    new Thread(() -> {
//      Scanner scanner = new Scanner(System.in);
//      String readString = scanner.nextLine();
//      while (readString != null) {
//        System.out.println(readString);
//        if (readString.equals("")) {
//          System.out.println("Toggle execution.");
//          isRunning = !isRunning;
//          try {
//            releasePermit();
//          } catch (Exception e) {
//          }
//        }
//        if (scanner.hasNextLine())
//          readString = scanner.nextLine();
//        else
//          readString = null;
//      }
//    }).start();

//    this.startBenchmark();
    this.startBenchmarkSync();
  }

  private void startBenchmark() throws IOException, InterruptedException {
    logger.info("Node {} start benchmarking", this.agent.getNode());

    if (isClient) {
      sendPermits = new Semaphore(1);
//      int payloadSize = RamcastConfig.SIZE_MESSAGE - RamcastMessage.calculateOverhead(destinationCount);
      int payloadSize = 16;

      ByteBuffer buffer = ByteBuffer.allocateDirect(payloadSize);
      buffer.putInt(clientId);
      while (buffer.remaining() > 0) buffer.put((byte) 1);

      List<RamcastGroup> dest = new ArrayList<>();
      for (int i = 0; i < destinationCount; i++) {
        dest.add(RamcastGroup.getGroup(destinationFrom + i));
      }

      RamcastMessage sampleMessage = this.agent.createMessage(0, buffer, dest);
      int i = 0;
      int lastMsgId = -1;

      logger.info("Client node {} sending request to destination {} with payload size={}", this.agent.getNode(), dest, payloadSize);
      while (isRunning) {
        getPermit();

        tpMonitor.incrementCount();
        if (tpMonitor.getCount() % 50000 == 0)
          logger.info("Total messages sent: {}", tpMonitor.getCount());

        if (RamcastConfig.DELAY)
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        int id = Objects.hash(i, this.clientId);
        sampleMessage.setId(id);
        this.agent.setSlot(sampleMessage, dest);

        startTime = System.nanoTime();
        buffer.putLong(4, startTime);
        while (!agent.isAllDestReady(dest, lastMsgId)) {
          Thread.yield();
        }
        if (RamcastConfig.LOG_ENABLED) {
          logger.debug("Client {} start new request {} msgId [{}]", clientId, i, id);
        }
        agent.multicast(sampleMessage, dest);
        lastMsgId = id;
        i++;
      }
    }
  }

  private void startBenchmarkSync() throws IOException, InterruptedException {
    logger.info("Node {} start benchmarking", this.agent.getNode());

    if (isClient) {
      sendPermits = new Semaphore(1);

      // todo: what are these magic numbers?
      int payloadSize = RamcastConfig.SIZE_MESSAGE - RamcastMessage.calculateOverhead(destinationCount);
      if (destinationCount == 4) payloadSize -= 128;
      if (destinationCount == 8) payloadSize -= 230;
//      int payloadSize = 16;

      // define the buffer, put client id in the first 4 bytes
      // it later checks if client id matches in callback function to log latency
      ByteBuffer buffer = ByteBuffer.allocateDirect(payloadSize);
      buffer.putInt(clientId);
      while (buffer.remaining() > 0) buffer.put((byte) 1);

      // specify destination groups
      List<RamcastGroup> dest = new ArrayList<>();
      for (int i = 0; i < destinationCount; i++) {
        dest.add(RamcastGroup.getGroup(destinationFrom + i));
      }

      // create the sample message with the defined buffer and destinations
      RamcastMessage sampleMessage = this.agent.createMessage(0, buffer, dest);
      int i = 0;
      int lastMsgId = -1;

      logger.info("Client node {} sending request to destination {} with payload size={}", this.agent.getNode(), dest, payloadSize);
      while (isRunning) {
        if (RamcastConfig.DELAY)
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }

        // todo: necessary to have new message ids each time?
        int id = Objects.hash(i++, this.clientId);
        sampleMessage.setId(id);

        // set slots for this message, in remote circular buffers, in each remote process, in each destination group
        this.agent.setSlot(sampleMessage, dest);

        startTime = System.nanoTime();
        buffer.putLong(4, startTime);

        if (RamcastConfig.LOG_ENABLED) {
          logger.debug("Client {} start new request {} msgId [{}]", clientId, i, id);
        }

        // todo ??
        // TODO: 2021.07.29 is there any response?
        // update: not in general, there is no response.
        // but in this function, the server, writes back some info to let client know the message is delivered
        agent.multicastSync(sampleMessage, dest);

        // todo: why tpmonitor here? in different place from latency and cdf
        // update: because clients calculate tp but latency calculated only by clients in server groups
        tpMonitor.incrementCount();
        if (tpMonitor.getCount() % 50000 == 0)
          logger.info("Total messages sent: {}", tpMonitor.getCount());

      }
    }
  }
}
