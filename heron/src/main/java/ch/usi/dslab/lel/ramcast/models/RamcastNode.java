package ch.usi.dslab.lel.ramcast.models;

import ch.usi.dslab.lel.ramcast.RamcastConfig;

import java.net.InetSocketAddress;
import java.util.HashMap;

public class RamcastNode {

  public static HashMap<Integer, RamcastGroup> shadowGroupMap =
      new HashMap<>(); // keep nodes thatt are not servers
  private int nodeId;
  private int groupId;
  private int clientId;
  private String address;
  private int port;
  private boolean isLeader = false;
  private boolean isClient = false;
  private int roleId;
  private InetSocketAddress inetAddress;

  public RamcastNode(String address, int port, int groupId, int nodeId, int roleId) {
    this.nodeId = nodeId;
    this.groupId = groupId;
    this.address = address;
    this.port = port;
    this.roleId = roleId;
  }

  public static void removeLeader(int groupId) {
    RamcastGroup g = RamcastGroup.groupMap.get(groupId);
    for (RamcastNode node : g.getMembers()) node.setLeader(false);
  }

  public static RamcastNode getNode(int groupId, int nodeId) {
    RamcastGroup g = RamcastGroup.groupMap.get(groupId);
    if (g == null) {
      return null;
    }
    return g.getNode(nodeId);
  }

  public InetSocketAddress getInetAddress() {
    if (this.inetAddress == null) this.inetAddress = new InetSocketAddress(address, port);
    return this.inetAddress;
  }

  public int getClientId() {
    return clientId;
  }

  public void setClientId(int clientId) {
    this.clientId = clientId;
  }

  public boolean hasClientRole() {
    return roleId == RamcastConfig.ROLE_BOTH || roleId == RamcastConfig.ROLE_CLIENT;
  }

  public boolean hasServerRole() {
    return roleId == RamcastConfig.ROLE_BOTH || roleId == RamcastConfig.ROLE_SERVER;
  }

  public void setClient(boolean client) {
    isClient = client;
  }

  public boolean isLeader() {
    return isLeader;
  }

  public void setLeader(boolean leader) {
    isLeader = leader;
    if (isLeader) this.getGroup().setLeader(this);
  }

  public int getNodeId() {
    return this.nodeId;
  }

  public int getGroupId() {
    return groupId;
  }

  public String getAddress() {
    return address;
  }

  public int getPort() {
    return port;
  }

  public String toString() {
    return "[node " + this.groupId + "/" + this.nodeId + "]";
  }

  public RamcastGroup getGroup() {
    return RamcastGroup.groupMap.get(this.groupId);
  }

  public int getOrderId() {
    return this.groupId * RamcastConfig.getInstance().getNodePerGroup() + this.nodeId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RamcastNode that = (RamcastNode) o;

    if (nodeId != that.nodeId) return false;
    return groupId == that.groupId;
  }

  @Override
  public int hashCode() {
    int result = nodeId;
    result = 31 * result + groupId;
    return result;
  }
}
