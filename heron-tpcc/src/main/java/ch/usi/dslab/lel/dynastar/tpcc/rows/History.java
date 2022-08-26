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
public class History extends Row {
    public final MODEL model = MODEL.HISTORY;
    //    public int hist_id;
    public int h_c_id;
    public int h_c_d_id;
    public int h_c_w_id;
    public int h_d_id;
    public int h_w_id;
    public long h_date;
    public double h_amount;
    public String h_data;

    public History() {

    }

    public History(ObjId id) {
        this.setId(id);
    }

    public History(int h_c_id, int h_c_d_id, int h_c_w_id) {
        this.h_c_id = h_c_id;
        this.h_c_w_id = h_c_w_id;
        this.h_c_d_id = h_c_d_id;
//        this.setStrObjId();
    }

    public void setWarehouse (int warehouse) {
        h_c_w_id = warehouse;
    }


    @Override
    public void updateFromDiff(Message objectDiff) {
        this.h_c_id = (int) objectDiff.getNext();
        this.h_c_d_id = (int) objectDiff.getNext();
        this.h_c_w_id = (int) objectDiff.getNext();
        this.h_d_id = (int) objectDiff.getNext();
        this.h_w_id = (int) objectDiff.getNext();
        this.h_date = (long) objectDiff.getNext();
        this.h_amount = (double) objectDiff.getNext();
        this.h_data = (String) objectDiff.getNext();
    }

    @Override
    public Message getSuperDiff() {
        return new Message(this.h_c_id, this.h_c_d_id, this.h_c_w_id, this.h_d_id, this.h_w_id, this.h_date, this.h_amount, this.h_data);
    }

    @Override
    public String toString() {
        return (
                "\n***************** History ********************" +
                        "\n*   h_c_id = " + h_c_id +
                        "\n* h_c_d_id = " + h_c_d_id +
                        "\n* h_c_w_id = " + h_c_w_id +
                        "\n*   h_d_id = " + h_d_id +
                        "\n*   h_w_id = " + h_w_id +
                        "\n*   h_date = " + h_date +
                        "\n* h_amount = " + h_amount +
                        "\n*   h_data = " + h_data +
                        "\n**********************************************"
        );
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":h_c_w_id=" + h_c_w_id + ":h_c_d_id=" + h_c_d_id + ":h_c_id=" + h_c_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"h_c_w_id", "h_c_d_id", "h_c_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
        return "h_w_id,h_d_id,h_c_id,h_c_w_id,h_c_d_id,h_data,h_date,h_amount";
//        return "h_w_id,h_d_id,h_c_id,h_c_w_id,h_c_d_id,h_data,h_date,model,h_amount";
    }

    public static String getHeader() {
        return "Header,History,h_w_id,h_d_id,h_c_id,h_c_w_id,h_c_d_id,h_data,h_date,h_amount";
    }
}
