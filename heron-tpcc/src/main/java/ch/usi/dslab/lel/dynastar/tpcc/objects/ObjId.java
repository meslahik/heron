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

import java.io.Serializable;

/**
 * Created by longle on 17.07.17.
 */

public class ObjId implements Serializable, Comparable<ObjId> {
    private static final long serialVersionUID = 1L;

    public int value;
    public String sId;

    // TODO: this should not be the place to put this. find another proper place
    public boolean includeDependencies=false;

    public ObjId() {
    }

    public ObjId(ObjId other) {
        this.value = other.value;
    }

    public ObjId(int value) {
        this.value = value;
    }

    public ObjId(String sId) {
        this.sId = sId;
    }

    public String getSId() {
        return sId;
    }

    public void setSId(String sId) {
        this.sId = sId;
    }

    @Override
    public int hashCode() {
        if (this.sId!=null) return this.sId.hashCode();
        return Long.hashCode(this.value);
    }

    @Override
    public boolean equals(Object other_) {
        try {
            ObjId other = ((ObjId) other_);
            if (this.sId != null && other.sId != null) return this.sId.equals(other.sId);
            return this.value == other.value;
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public String toString() {
        if (this.sId != null) return "(" + value + "/" + sId + ")";
        return "(" + value + ")";
    }

    @Override
    public int compareTo(ObjId other) {
        return Integer.compare(this.value, other.value);
    }

}
