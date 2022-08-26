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

public class ServerCmdLineCommon {

  private static final String SERVER_ADDR_KEY = "sa";
  private static final String SERVER_PORT_KEY = "sp";
  private static final String PACKAGE_SIZE_KEY = "s";

  private final String appName;
  private final Options options;
  private String serverAddress;
  private int serverPort;
  private int payloadSize;

  public ServerCmdLineCommon(String appName) {
    this.appName = appName;

    this.options = new Options();

    Option serverPortOption = Option.builder(SERVER_PORT_KEY).required().desc("server port size").hasArg().build();
    Option serverAddressOption = Option.builder(SERVER_ADDR_KEY).required().desc("server address size").hasArg().build();
    Option packageSizeOption = Option.builder(PACKAGE_SIZE_KEY).required().desc("sample package size").hasArg().build();

    options.addOption(serverPortOption);
    options.addOption(serverAddressOption);
    options.addOption(packageSizeOption);
  }

  protected Options addOption(Option option) {
    return options.addOption(option);
  }

  public void printHelp() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(appName, options);
  }

  protected void getOptionsValue(CommandLine line) throws ParseException {
    serverPort = Integer.parseInt(line.getOptionValue(SERVER_PORT_KEY));
    serverAddress = line.getOptionValue(SERVER_ADDR_KEY);
    payloadSize = Integer.parseInt(line.getOptionValue(PACKAGE_SIZE_KEY));
  }

  public void parse(String[] args) throws ParseException {
    CommandLineParser parser = new DefaultParser();
    CommandLine line = parser.parse(options, args);
    getOptionsValue(line);
  }

  public int getServerPort() {
    return serverPort;
  }

  public String getServerAddress() {
    return serverAddress;
  }

  public int getPayloadSize() {
    return payloadSize;
  }
}
