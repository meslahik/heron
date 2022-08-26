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
public class Order extends Row {
    public final MODEL model = MODEL.ORDER;
    public int o_id;
    public int o_d_id;
    public int o_w_id;
    public int o_c_id;
    public long o_entry_d;
    public int o_carrier_id;
    public int o_ol_cnt;
    public int o_all_local;

    public Order() {

    }

    public Order(ObjId id) {
        this.setId(id);
    }

    public Order(int o_w_id, int o_d_id, int o_c_id, int o_id) {
        this.o_id = o_id;
        this.o_w_id = o_w_id;
        this.o_d_id = o_d_id;
        this.o_c_id = o_c_id;
//        this.setStrObjId();
    }

    public void setWarehouse (int warehouse) {
        o_w_id = warehouse;
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":o_w_id=" + o_w_id + ":o_d_id=" + o_d_id + ":o_c_id=" + o_c_id + ":o_id=" + o_id;
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
        this.o_d_id = (int) objectDiff.getNext();
        this.o_w_id = (int) objectDiff.getNext();
        this.o_c_id = (int) objectDiff.getNext();
        this.o_entry_d = (long) objectDiff.getNext();
        this.o_carrier_id = (int) objectDiff.getNext();
        this.o_ol_cnt = (int) objectDiff.getNext();
        this.o_all_local = (int) objectDiff.getNext();
    }

    @Override
    public Message getSuperDiff() {
        return new Message(this.o_d_id, this.o_w_id, this.o_c_id, this.o_entry_d, this.o_carrier_id, this.o_ol_cnt, this.o_all_local);
    }

    @Override
    public String toString() {
        java.sql.Timestamp entry_d = new java.sql.Timestamp(o_entry_d);

        return (
                "\n***************** Oorder ********************" +
                        "\n*         o_id = " + o_id +
                        "\n*       o_w_id = " + o_w_id +
                        "\n*       o_d_id = " + o_d_id +
                        "\n*       o_c_id = " + o_c_id +
                        "\n* o_carrier_id = " + o_carrier_id +
                        "\n*     o_ol_cnt = " + o_ol_cnt +
                        "\n*  o_all_local = " + o_all_local +
                        "\n*    o_entry_d = " + entry_d +
                        "\n**********************************************"
        );
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"o_w_id", "o_d_id", "o_c_id", "o_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
        return "o_w_id,o_d_id,o_c_id,o_id,o_carrier_id,o_all_local,o_entry_d,o_ol_cnt";
//        return "o_w_id,o_d_id,o_c_id,o_id,o_carrier_id,o_all_local,o_entry_d,model,o_ol_cnt";
    }

    public static String getHeader() {
        return "Header,Order,o_w_id,o_d_id,o_c_id,o_id,o_carrier_id,o_all_local,o_entry_d,o_ol_cnt";
    }
}
