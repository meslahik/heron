package ch.usi.dslab.lel.dynastar.tpcc.objects;
/*
 * ScalableSMR - A library for developing Scalable services based on SMR
 * Copyright (C) 2017, University of Lugano
 *
 *  This file is part of ScalableSMR.
 *
 *  ScalableSMR is free software; you can redistribute it and/or
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

//import ch.usi.dslab.lel.dynastarv2.AppProcedure;
//import ch.usi.dslab.lel.dynastarv2.StateMachine;
//import org.junit.Assert;
import org.slf4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by longle on 17.07.17.
 */
public class PRObjectMap implements Serializable {
    //    protected Map<String, Set<ObjId>> objectIdMapping = new ConcurrentHashMap<>();
//    AppProcedure appProcedure;
    transient private Logger logger;
    private Map<ObjId, PRObject> objectMapping = new ConcurrentHashMap<>();
    private int partitionId;

    public PRObjectMap() {

    }

//    public PRObjectMap(AppProcedure appProcedure, int partitionId) {
//        this.partitionId = partitionId;
//        this.appProcedure = appProcedure;
//    }

    public static int getNodesCount(Map<ObjId, PRObject> objectMapping) {
        int count = 0;
        for (Map.Entry<ObjId, PRObject> entry : objectMapping.entrySet()) {
            count++;
        }
        return count;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public PRObject getPRObject(ObjId objId) {
        PRObject obj = objectMapping.get(objId);
        if (obj != null) return obj;
        return null;
    }

    public PRObject getNode(ObjId objId) {
        return objectMapping.get(objId);
    }

    public void addNode(PRObject prObject) {
        objectMapping.put(prObject.getId(), prObject);
    }

    public void removeObject(ObjId objId) {
        objectMapping.remove(objId);
    }

    public void updatePRObject(ObjId objId, PRObject node) {
        objectMapping.put(objId, node);
    }

    public void invalidate() {
        objectMapping = new ConcurrentHashMap<>();
    }


    public Set<ObjId> getMissingObjects(Set<ObjId> objIds) {
        return objIds.stream().filter(objId -> getPRObject(objId) == null).collect(Collectors.toSet());
    }

//    public Set<ObjId> getMissingNodes(Set<ObjId> objIds) {
//        return objIds.stream().filter(objId -> getNode(objId) == null &&
//                (appProcedure.genParentObjectId(objId) == null
//                        ? true
//                        : getNode(appProcedure.genParentObjectId(objId)) == null)).collect(Collectors.toSet());
//    }

//    private PRObjectNode addOrUpdateNode(ObjId objId, Integer partitionId) {
//        PRObjectNode node = getNode(objId);
//        if (node == null) {
//            logger.debug("obj {} doesn't exist in memory, going to create one for partition {}", objId, partitionId);
//            node = new PRObjectNode(objId, partitionId);
//            addNode(node);
//        }
//        logger.debug("obj {} is in memory, going to update to partition {}", objId, partitionId);
//        node.setPartitionId(partitionId);
//        return node;
//    }

//    private PRObjectNode addOrUpdateNode(PRObjectNode other, Integer partitionId) {
//        PRObjectNode node = addOrUpdateNode(other.getId(), partitionId);
//        node.setParentObjId(other.getParentObjId());
//        node.setTransient(other.isTransient());
//        node.setReplicated(other.isReplicated());
//        return node;
//    }

    public Map<ObjId, PRObject> getObjectMapping() {
        return objectMapping;
    }

//    public void setObjectMapping(Map<ObjId, PRObject> objectMapping) {
//        this.objectMapping = objectMapping;
//    }
//
//    // todo: after changing PRObjectNode to PRObject, need to add partitionId to PRObject?
//    public Set<ObjId> getLocalObjects() {
//        Set<ObjId> ret = new HashSet<>();
//        for (Map.Entry<ObjId, PRObject> entry : this.objectMapping.entrySet()) {
//            ObjId objId = entry.getKey();
//            if (entry.getValue().getPartitionId() == this.partitionId) ret.add(objId);
//        }
//        return ret;
//
//    }
//
//    public PRObject getLocalObjects(ObjId objId) {
//        if (this.getNode(objId) != null && this.getPRObject(objId) != null && this.getNode(objId).getPartitionId() == this.partitionId) {
//            return this.getNode(objId);
//        }
//        return null;
//    }
}
