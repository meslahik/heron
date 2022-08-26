/*
 * DynaStar - A library for developing Scalable services based on SMR
 * Copyright (C) 2017, University of Lugano
 *
 *  This file is part of DynaStar.
 *
 *  DynaStar is free software; you can redistribute it and/or
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

package ch.usi.dslab.lel.dynastar.tpcc.objects;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.bezerra.netwrapper.codecs.Codec;
import ch.usi.dslab.bezerra.netwrapper.codecs.CodecUncompressedKryo;

import java.io.Serializable;

/**
 * Created by longle on 17.07.17.
 */
public abstract class PRObject implements Serializable {
    public ObjId getId() {
        return id;
    }

    public void setId(ObjId id) {
        this.id = id;
    }

    private ObjId id;

    public PRObject() {}

    public PRObject(ObjId id) {
        this.id = id;
    }

    public abstract void updateFromDiff(Message objectDiff);
//    public abstract void updateFromDiff(PRObject objectDiff);

    public abstract Message getSuperDiff();

    public PRObject deepClone() {
        Codec codec = new CodecUncompressedKryo();
        return (PRObject) codec.deepDuplicate(this);
    }

}
