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

package ch.usi.dslab.lel.dynastar.tpcc.rows;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastar.tpcc.objects.ObjId;
//import ch.usi.dslab.lel.dynastarv2.probject.ObjId;

/**
 * Created by longle on 18/02/16.
 */
public class RecentPurchasedItem extends Row {
    public final MODEL model = MODEL.RECENT_PURCHASED_ITEM;
    public int rpi_d_id;
    public int rpi_w_id;
    public int rpi_i_id;

    public RecentPurchasedItem() {

    }

    public RecentPurchasedItem(ObjId id) {
        this.setId(id);
    }

    public RecentPurchasedItem(int rpi_w_id, int rpi_d_id, int rpi_i_id) {
        this.rpi_w_id = rpi_w_id;
        this.rpi_d_id = rpi_d_id;
        this.rpi_i_id = rpi_i_id;
//        this.setStrObjId();
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":rpi_w_id=" + rpi_w_id + ":rpi_d_id=" + rpi_d_id + ":rpi_i_id=" + rpi_i_id;
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
        this.rpi_w_id = (int) objectDiff.getNext();
        this.rpi_d_id = (int) objectDiff.getNext();
        this.rpi_i_id = (int) objectDiff.getNext();
    }

    @Override
    public Message getSuperDiff() {
        return new Message(this.rpi_w_id, this.rpi_d_id, this.rpi_i_id);
    }

    @Override
    public String toString() {
        return (
                "\n***************** RecentPurchasedItem ********************" +
                        "\n*         rpi_w_id = " + rpi_w_id +
                        "\n*       rpi_d_id = " + rpi_d_id +
                        "\n*       rpi_i_id = " + rpi_i_id +
                        "\n**********************************************"
        );
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"rpi_w_id", "rpi_d_id", "rpi_i_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
        return null;
    }
}
