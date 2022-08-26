package ch.usi.dslab.lel.dynastar.tpcc.command;
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
public class CmdId implements Serializable {
    public int clientId;
    public long clientRequestSequence;

    public CmdId() {
    }

    public CmdId(CmdId other) {
        this.clientId = other.clientId;
        this.clientRequestSequence = other.clientRequestSequence;
    }

    public CmdId(int clientId, long clientRequestSequence) {
        this.clientId = clientId;
        this.clientRequestSequence = clientRequestSequence;
    }

    @Override
    public boolean equals(Object other_) {
        CmdId other = ((CmdId) other_);
        return this.clientId == other.clientId && this.clientRequestSequence == other.clientRequestSequence;
    }

    @Override
    public int hashCode() {
        return (int) ((clientRequestSequence << 16) + clientId);
    }

    @Override
    public String toString() {
        return "(" + clientId + "." + clientRequestSequence + ")";
    }
}
