/*
 * Nest - A library for developing DSSMR-based services
 * Copyright (C) 2015, University of Lugano
 *
 *  This file is part of Nest.
 *
 *  Nest is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

/**
 * @author Eduardo Bezerra - carlos.bezerra@usi.ch
 */

package ch.usi.dslab.lel.dynastar.tpcc.command;

import ch.usi.dslab.bezerra.netwrapper.Message;
//import ch.usi.dslab.lel.dynastarv2.Partition;
//import ch.usi.dslab.lel.dynastarv2.messages.MessageType;
//import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
//import ch.usi.dslab.lel.dynastarv2.probject.PRObject;
import ch.usi.dslab.lel.dynastar.tpcc.objects.ObjId;
import ch.usi.dslab.lel.dynastar.tpcc.objects.PRObject;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
//import org.apache.commons.lang.SerializationUtils;

import java.util.*;
import java.util.stream.Collectors;

public class Command extends Message {
    private static final long serialVersionUID = 1L;
    public List<Integer> destinations;
    transient Set<RamcastGroup> destinationPartitions = null;
    private CmdId id;
    private int partitioningVersion = 0;
    private Set<ObjId> involvedObj = null;
    private Command followedCommand = null;
    private Map<Integer, Set<ObjId>> objectMap;
    private boolean isGathering = false;
    private boolean isRepartitioning = false;
    private boolean isSSMRExecution = false;

    // special command, let application reserve object on oracle's memory
    private Set<PRObject> reservedObjects = null;
    private int destinationPartitionToExecute = -1;
    private boolean serverDecidesMove = false;
    transient boolean isInvalid = false;
    public boolean skipped=false;

    public Command() {

    }

    public boolean isServerDecidesMove() {
        return serverDecidesMove;
    }

    public void setServerDecidesMove(boolean serverDecidesMove) {
        this.serverDecidesMove = serverDecidesMove;
    }

    public int getDestinationPartitionToExecute() {
        return destinationPartitionToExecute;
    }

    public void setDestinationPartitionToExecute(int destinationPartitionToExecute) {
        this.destinationPartitionToExecute = destinationPartitionToExecute;
    }

    public boolean isInvalid() {
        return isInvalid;
    }

    public void setInvalid(boolean invalid) {
        isInvalid = invalid;
    }

    public Command(Object... objs) {
        super(objs);
    }

    public Set<PRObject> getReservedObjects() {
        if (reservedObjects == null || reservedObjects.size() == 0) return null;
        return reservedObjects;
    }

    public void setReservedObjects(Set<PRObject> reservedObjects) {
        this.reservedObjects = reservedObjects;
    }

    public boolean isRepartitioning() {
        return isRepartitioning;
    }

    public void setRepartitioning(boolean repartitioning) {
        isRepartitioning = repartitioning;
    }

    public boolean isGathering() {
        return isGathering;
    }

    public void setGathering(boolean gathering) {
        isGathering = gathering;
    }

    public Command getFollowedCommand() {
        return this.followedCommand;
    }

    public void setFollowedCommand(Command command) {
        this.followedCommand = command;
    }

    public boolean hasFollowedCommand() {
        return followedCommand != null;
    }

    @Override
    public String toString() {
        if (this.getId() != null) return this.getId() + "";
        String str = super.toString();
        String id = this.getId() == null ? "()" : this.getId().toString();
        return id + "-" + str;
    }

    public String toFullString() {
        String str = super.toString();
        return this.getId() + "-" + str;
    }

    public CmdId getId() {
        return id;
    }

    public void setId(CmdId id) {
        this.id = id;
    }

    public void setId(int clientId, long cliCmdSeq) {
        this.id = new CmdId(clientId, cliCmdSeq);
    }

    public Set<ObjId> getInvolvedObjects() {
        return involvedObj;
    }

    public void setInvolvedObjects(Set<ObjId> involvedObj) {
        this.involvedObj = involvedObj;
    }

    public void addInvolvedObjects(Set<ObjId> involvedObj) {
        this.involvedObj.addAll(involvedObj);
    }

    public void cleanInvolvedObjects() {
        this.involvedObj = null;
    }

    public int getSourceClientId() {
        return id.clientId;
    }

    public int getPartitioningVersion() {
        return partitioningVersion;
    }

    public void setPartitioningVersion(int partitioningVersion) {
        this.partitioningVersion = partitioningVersion;
    }

    public Map<Integer, Set<ObjId>> getObjectMap() {
        return objectMap;
    }

    public void setObjectMap(Map<Integer, Set<ObjId>> objectMap) {
        this.objectMap = objectMap;
    }

//    synchronized public Set<Partition> getDestinations() {
//        if (destinationPartitions == null) {
//            destinationPartitions = new HashSet<>(destinations.size());
//            for (int i : destinations) {
//                destinationPartitions.add(Partition.getPartition(i));
//            }
//        }
//        return destinationPartitions;
//    }
//
//    synchronized public void setDestinations(Set<Partition> dests) {
//        destinationPartitions = new HashSet<>(dests);
//        destinations = new ArrayList<>(dests.size());
//        destinations.addAll(dests.stream().map(partition -> partition.getId()).collect(Collectors.toList()));
//    }

    public Set<Integer> getDestinationIds() {
        return new HashSet<>(destinations);
    }

    @Override
    public boolean equals(Object other) {
        return this.id.equals(((Command) other).id);
    }

    public Command clone() {
//        return (Command) SerializationUtils.clone(this);
        return new Command();
    }

    public MessageType getType() {
        if (this.getItem(0) instanceof MessageType) return (MessageType) this.getItem(0);
        return null;
    }

    public boolean isSSMRExecution() {
        return isSSMRExecution;
    }

    public void setSSMRExecution(boolean isSSMRExecution) {
        this.isSSMRExecution = isSSMRExecution;
    }
}
