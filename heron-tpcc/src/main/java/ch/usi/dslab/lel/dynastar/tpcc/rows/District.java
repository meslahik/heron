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
public class District extends Row {
    public final MODEL model = MODEL.DISTRICT;
    public int d_id; //PRIMARY KEY 1
    public int d_w_id; //PRIMARY KEY 2
    public String d_name;
    public String d_state;
    public String d_street_1;
    public String d_street_2;
    public String d_city;
    public int d_zip;
    public double d_tax;
    public double d_ytd;
    public int d_next_o_id;
    public int oldest_new_order;

    public District() {

    }

    public District(ObjId id) {
        this.setId(id);
    }


    public District(int d_id, int d_w_id) {
        this.d_id = d_id;
        this.d_w_id = d_w_id;
//        this.setStrObjId();
    }


    @Override
    public void updateFromDiff(Message objectDiff) {
        this.d_w_id = (int) objectDiff.getNext();
        this.d_name = (String) objectDiff.getNext();
        this.d_state = (String) objectDiff.getNext();
        this.d_street_1 = (String) objectDiff.getNext();
        this.d_street_2 = (String) objectDiff.getNext();
        this.d_city = (String) objectDiff.getNext();
        this.d_zip = (int) objectDiff.getNext();
        this.d_tax = (double) objectDiff.getNext();
        this.d_ytd = (int) objectDiff.getNext();
        this.d_next_o_id = (int) objectDiff.getNext();
    }


    @Override
    public Message getSuperDiff() {
        return new Message(this.d_w_id, this.d_name, this.d_state, this.d_street_1, this.d_street_2, this.d_city, this.d_zip, this.d_tax, this.d_ytd, this.d_next_o_id);
    }

    @Override
    public String toString() {
        return (
                "\n***************** District ********************" +
                        "\n*        d_id = " + d_id +
                        "\n*      d_w_id = " + d_w_id +
                        "\n*       d_ytd = " + d_ytd +
                        "\n*       d_tax = " + d_tax +
                        "\n* d_next_o_id = " + d_next_o_id +
                        "\n*      d_name = " + d_name +
                        "\n*  d_street_1 = " + d_street_1 +
                        "\n*  d_street_2 = " + d_street_2 +
                        "\n*      d_city = " + d_city +
                        "\n*     d_state = " + d_state +
                        "\n*       d_zip = " + d_zip +
                        "\n**********************************************"
        );
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":d_w_id=" + d_w_id + ":d_id=" + d_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"d_w_id", "d_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
//        return "d_w_id,d_id,d_state,d_ytd,d_name,d_street_1,d_street_2,d_zip,model,d_city,d_next_o_id,d_tax";
        return "d_w_id,d_id,d_state,d_ytd,d_name,d_street_1,d_street_2,d_zip,d_city,d_next_o_id,d_tax";
    }

    public static String getHeader() {
        return "Header,District,d_w_id,d_id,d_state,d_ytd,d_name,d_street_1,d_street_2,d_zip,d_city,d_next_o_id,d_tax";
    }
}
