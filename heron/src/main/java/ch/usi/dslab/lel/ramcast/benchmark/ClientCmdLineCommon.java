/*
 * DiSNI: Direct Storage and Networking Interface
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

import org.apache.commons.cli.*;

public class ClientCmdLineCommon {

  private static final String SERVER_ADDR_KEY = "sa";
  private static final String SERVER_PORT_KEY = "sp";
  private static final String PACKAGE_SIZE_KEY = "s";
  private static final String GATHERER_HOST_KEY = "gh";
  private static final String GATHERER_PORT_KEY = "gp";
  private static final String GATHERER_DIR_KEY = "gd";
  private static final String GATHERER_WARMUP_KEY = "gw";
  private static final String DURATION_KEY = "d";
  private final String appName;
  private final Options options;

  private String serverAddress;
  private int serverPort;
  private int payloadSize;
  private String gathererHost;
  private int gathererPort;
  private String fileDirectory;
  private int experimentDuration;
  private int warmUpTime;

  public ClientCmdLineCommon(String appName) {
    this.appName = appName;

    this.options = new Options();

    Option serverAddressOption = Option.builder(SERVER_ADDR_KEY).required().desc("server address size").hasArg().build();
    Option serverPortOption = Option.builder(SERVER_PORT_KEY).required().desc("server port size").hasArg().build();
    Option packageSizeOption = Option.builder(PACKAGE_SIZE_KEY).required().desc("sample package size").hasArg().build();
    Option gathererHostOption = Option.builder(GATHERER_HOST_KEY).required().desc("gatherer host").hasArg().build();
    Option gathererPortOption = Option.builder(GATHERER_PORT_KEY).required().desc("gatherer port").hasArg().build();
    Option gathererDirectoryOption = Option.builder(GATHERER_DIR_KEY).required().desc("gatherer directory").hasArg().build();
    Option warmUpTimeOption = Option.builder(GATHERER_WARMUP_KEY).required().desc("gatherer warmup time").hasArg().build();
    Option durationOption = Option.builder(DURATION_KEY).required().desc("benchmark duration").hasArg().build();

    options.addOption(serverAddressOption);
    options.addOption(serverPortOption);
    options.addOption(packageSizeOption);
    options.addOption(gathererHostOption);
    options.addOption(gathererPortOption);
    options.addOption(warmUpTimeOption);
    options.addOption(durationOption);
    options.addOption(gathererDirectoryOption);
  }

  protected Options addOption(Option option) {
    return options.addOption(option);
  }

  public void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(appName, options);
  }

  protected void getOptionsValue(CommandLine line) throws ParseException {

    serverAddress = line.getOptionValue(SERVER_ADDR_KEY);
    serverPort = Integer.parseInt(line.getOptionValue(SERVER_PORT_KEY));
    payloadSize = Integer.parseInt(line.getOptionValue(PACKAGE_SIZE_KEY));
    gathererHost = line.getOptionValue(GATHERER_HOST_KEY);
    gathererPort = Integer.parseInt(line.getOptionValue(GATHERER_PORT_KEY));
    fileDirectory = line.getOptionValue(GATHERER_DIR_KEY);
    experimentDuration = Integer.parseInt(line.getOptionValue(DURATION_KEY));
    warmUpTime = Integer.parseInt(line.getOptionValue(GATHERER_WARMUP_KEY));
  }

  public void parse(String[] args) throws ParseException {
    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);
    getOptionsValue(line);
  }

  public String getServerAddress() {
    return serverAddress;
  }

  public int getServerPort() {
    return serverPort;
  }

  public int getPayloadSize() {
    return payloadSize;
  }

  public String getGathererHost() {
    return gathererHost;
  }

  public int getGathererPort() {
    return gathererPort;
  }

  public String getFileDirectory() {
    return fileDirectory;
  }

  public int getExperimentDuration() {
    return experimentDuration;
  }

  public int getWarmUpTime() {
    return warmUpTime;
  }
}
