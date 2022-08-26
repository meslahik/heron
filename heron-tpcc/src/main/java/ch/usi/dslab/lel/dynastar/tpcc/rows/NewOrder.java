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
public class NewOrder extends Row {
    public final MODEL model = MODEL.NEWORDER;
    public int no_o_id;
    public int no_d_id;
    public int no_w_id;

    public NewOrder() {

    }

    public NewOrder(ObjId id) {
        this.setId(id);
    }

    public NewOrder(int no_w_id, int no_d_id, int no_o_id) {
        this.no_o_id = no_o_id;
        this.no_d_id = no_d_id;
        this.no_w_id = no_w_id;
//        this.setStrObjId();
    }

    public void setWarehouse (int warehouse) {
        no_w_id = warehouse;
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
        this.no_d_id = (int) objectDiff.getNext();
        this.no_w_id = (int) objectDiff.getNext();
    }

    @Override
    public Message getSuperDiff() {
        return new Message(this.no_d_id, this.no_w_id);
    }

    @Override
    public String toString() {
        return (
                "\n***************** NewOrder ********************" +
                        "\n*      no_w_id = " + no_w_id +
                        "\n*      no_d_id = " + no_d_id +
                        "\n*      no_o_id = " + no_o_id +
                        "\n**********************************************"
        );
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":no_w_id=" + no_w_id + ":no_d_id=" + no_d_id + ":no_o_id=" + no_o_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"no_w_id", "no_d_id", "no_o_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
        return "no_w_id,no_d_id,no_o_id";
//        return "no_w_id,no_d_id,no_o_id,model";
    }

    public static String getHeader() {
        return "Header,NewOrder,no_w_id,no_d_id,no_o_id";
    }
}
