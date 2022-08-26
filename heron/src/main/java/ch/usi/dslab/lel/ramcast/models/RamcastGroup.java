package ch.usi.dslab.lel.ramcast.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RamcastGroup implements Comparable<RamcastGroup>{
  static ArrayList<RamcastGroup> groupList;
  static Map<Integer, RamcastGroup> groupMap;

  static {
    groupList = new ArrayList<>();
    groupMap = new ConcurrentHashMap<>();
  }

  int groupId;
  private HashMap<Integer, RamcastNode> nodeMap;
  private boolean isShadow;
  private RamcastNode leader;
  private int followers;

  public RamcastGroup(int groupId) {
    this.groupId = groupId;
    groupList.add(this);
    this.nodeMap = new HashMap<>();
  }

  public static int getGroupCount() {
    return groupList.size();
  }

  public static RamcastGroup getOrCreateGroup(int id) {
    RamcastGroup g = groupMap.get(id);
    if (g == null) {
      g = new RamcastGroup(id);
      groupMap.put(id, g);
    }
    return g;
  }

  public static RamcastGroup getGroup(int id) throws RuntimeException {
    RamcastGroup g = groupMap.get(id);
    assert g != null;
    return g;
  }

  // get members of all group
  public static List<RamcastNode> getAllNodes() {
    return groupList.stream()
        .map(RamcastGroup::getMembers)
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  public static int getQuorum(short groupId) {
    return groupMap.get((int) groupId).getFollowers();
  }

  public int getLeaderId() {
    return nodeMap.values().stream().mapToInt(RamcastNode::getNodeId).min().orElse(-1);
  }

  public RamcastNode getNode(int nodeId) {
    return nodeMap.get(nodeId);
  }

  public void addNode(RamcastNode node) {
    nodeMap.put(node.getNodeId(), node);
  }

  public void removeNode(RamcastNode node) {
    nodeMap.remove(node.getNodeId());
    this.followers = 0;
  }

  public static int getTotalNodeCount() {
    return groupList.stream().mapToInt(RamcastGroup::getNodeCount).sum();
  }

  public int getNodeCount() {
    return nodeMap.keySet().size();
  }

  //    public List<Integer> getMembers() {
  //        return nodeMap.entrySet().stream().map(entry ->
  // entry.getValue().getNodeId()).collect(Collectors.toList());
  //    }
  // get member of single group
  public List<RamcastNode> getMembers() {
    return new ArrayList<>(nodeMap.values());
  }

  public int getFollowers() {
    if (this.followers == 0) {
      this.followers = (int) nodeMap.values().stream().filter(node -> !node.isLeader()).count();
    }
    return this.followers;
  }

  public int getId() {
    return groupId;
  }

  public RamcastNode getLeader() {
    return this.leader;
  }

  public void setLeader(RamcastNode leader) {
    this.leader = leader;
  }

  @Override
  public String toString() {
    return "[group " + this.groupId + "]";
  }

  public static void close() {
    groupList = new ArrayList<>();
    groupMap = new ConcurrentHashMap<>();
  }

  @Override
  public int compareTo(RamcastGroup group) {
    return Integer.compare(groupId, group.groupId);
  }
}
