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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Created by longle on 18/02/16.
 */
public class Warehouse extends Row {
    public final MODEL model = MODEL.WAREHOUSE;
    public int w_id;
    public String w_name;
    public String w_street_1;
    public String w_street_2;
    public String w_city;
    public String w_state;
    public int w_zip;
    public double w_tax;
    public double w_ytd;


    public Warehouse() {

    }

//    public Warehouse(ObjId id) {
//        this.setId(id);
//    }

    public Warehouse(int w_id) {
        this.w_id = w_id;
//        this.setStrObjId();
    }


    @Override
    public void updateFromDiff(Message objectDiff) {
        w_name = (String) objectDiff.getNext();
        w_street_1 = (String) objectDiff.getNext();
        w_street_2 = (String) objectDiff.getNext();
        w_city = (String) objectDiff.getNext();
        w_state = (String) objectDiff.getNext();
        w_zip = (int) objectDiff.getNext();
        w_tax = (double) objectDiff.getNext();
        w_ytd = (int) objectDiff.getNext();
    }

    @Override
    public Message getSuperDiff() {
        return new Message(this.w_name, this.w_street_1, this.w_street_2, this.w_city, this.w_state, this.w_zip, this.w_tax, this.w_ytd);
    }

    @Override
    public String toString() {
        return (
                "\n***************** Warehouse ********************" +
                        "\n*       w_id = " + w_id +
                        "\n*      w_ytd = " + w_ytd +
                        "\n*      w_tax = " + w_tax +
                        "\n*     w_name = " + w_name +
                        "\n* w_street_1 = " + w_street_1 +
                        "\n* w_street_2 = " + w_street_2 +
                        "\n*     w_city = " + w_city +
                        "\n*    w_state = " + w_state +
                        "\n*      w_zip = " + w_zip +
                        "\n**********************************************"
        );
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":w_id="; // + w_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"w_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
        return "w_id,w_name,w_zip,w_street_2,w_state,w_street_1,w_ytd,w_city,w_tax";
//        return "w_id,w_name,w_zip,w_street_2,w_state,w_street_1,w_ytd,w_city,w_tax,model";
    }

    public static String getHeader() {
        return "Header,Warehouse,w_id,w_name,w_zip,w_street_2,w_state,w_street_1,w_ytd,w_city,w_tax";
    }
}
