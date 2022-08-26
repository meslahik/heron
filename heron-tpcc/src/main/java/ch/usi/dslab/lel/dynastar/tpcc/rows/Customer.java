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
import java.util.ArrayList;


/**
 * Created by longle on 18/02/16.
 */
public class Customer extends Row {
    public int c_req_id;

    public final MODEL model = MODEL.CUSTOMER;
    public int c_id; //PRIMARY KEY 1
    public int c_d_id; //PRIMARY KEY 2
    public int c_w_id; //PRIMARY KEY 3
//    public String c_first;  // size: 16
//    public String c_middle;  // size: 2
//    public String c_last;  // size: 16
//    public String c_street_1;  // size: 20
//    public String c_street_2;  // size: 20
//    public String c_city;  // size: 20
//    public String c_state;  // size: 2
    public ByteBuffer c_first = ByteBuffer.allocateDirect(16);
    public ByteBuffer c_middle = ByteBuffer.allocateDirect(2);
    public ByteBuffer c_last = ByteBuffer.allocateDirect(16);
    public ByteBuffer c_street_1 = ByteBuffer.allocateDirect(20);
    public ByteBuffer c_street_2 = ByteBuffer.allocateDirect(20);
    public ByteBuffer c_city = ByteBuffer.allocateDirect(20);
    public ByteBuffer c_state = ByteBuffer.allocateDirect(2);
    public int c_zip;
//    public String c_phone;  // size: 16
    public ByteBuffer c_phone = ByteBuffer.allocateDirect(16);
    public long c_since;
//    public String c_credit;  // size: 2
    public ByteBuffer c_credit = ByteBuffer.allocateDirect(2);
    public int c_credit_lim;
    public double c_discount;
    public double c_balance;
    public double c_ytd_payment;
    public int c_payment_cnt;
    public int c_delivery_cnt;
    public int c_last_order;
//    public String c_data;  // size: 500
    public ByteBuffer c_data = ByteBuffer.allocateDirect(500);

    public void setWarehouse (int warehouse) {
        c_w_id = warehouse;
    }

    public void toBuffer(ByteBuffer buffer) {
        int startPos = buffer.position();
        buffer.putInt(c_req_id);
        buffer.putInt(c_id);
        buffer.putInt(c_d_id);
        buffer.putInt(c_w_id);
//        buffer.put(c_first.getBytes(StandardCharsets.UTF_8));
        buffer.put(c_first.clear());

        buffer.position(startPos+32);  // 16+16
        buffer.put(c_middle.clear());

        buffer.position(startPos+34);  // 16+16+2
        buffer.put(c_last.clear());

        buffer.position(startPos+50);  // 16+16+2+16
        buffer.put(c_street_1.clear());

        buffer.position(startPos+70);  // 16+16+2+16+20
        buffer.put(c_street_2.clear());

        buffer.position(startPos+90);  // 16+16+2+16+20+20
        buffer.put(c_city.clear());

        buffer.position(startPos+110);  // 16+16+2+16+20+20+20
        buffer.put(c_state.clear());

        buffer.position(startPos+112);  // 16+16+2+16+20+20+20+2
        buffer.putInt(c_zip);
        buffer.put(c_phone.clear());

        buffer.position(startPos+132);  // 16+16+2+16+20+20+20+2+4+16
        buffer.putLong(c_since);
        buffer.put(c_credit.clear());

        buffer.position(startPos+142);  // 16+16+2+16+20+20+20+2+4+16+8+2
        buffer.putInt(c_credit_lim);
        buffer.putDouble(c_discount);
        buffer.putDouble(c_balance);
        buffer.putDouble(c_ytd_payment);
        buffer.putInt(c_payment_cnt);
        buffer.putInt(c_delivery_cnt);
        buffer.putInt(c_last_order);
        buffer.put(c_data.clear());
    }

    public Customer() {
    }

    public void fromBuffer(ByteBuffer buffer) {
        c_req_id = buffer.getInt();
        c_id = buffer.getInt();
        c_d_id = buffer.getInt();
        c_w_id = buffer.getInt();

        int nextPos = buffer.position() + 16;
        c_first = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 2;
        c_middle = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 16;
        c_last = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 20;
        c_street_1 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 20;
        c_street_2 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 20;
        c_city = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 2;
        c_state = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        c_zip = buffer.getInt();

        nextPos = buffer.position() + 16;
        c_phone = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        c_since = buffer.getLong();

        nextPos = buffer.position() + 2;
        c_credit = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        c_credit_lim = buffer.getInt();
        c_discount = buffer.getDouble();
        c_balance = buffer.getDouble();
        c_ytd_payment = buffer.getDouble();
        c_payment_cnt = buffer.getInt();
        c_delivery_cnt = buffer.getInt();
        c_last_order = buffer.getInt();

        nextPos = buffer.position() + 500;
        c_data = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);
    }

    public Customer(int c_id, int c_d_id, int c_w_id) {
        this.c_id = c_id;
        this.c_w_id = c_w_id;
        this.c_d_id = c_d_id;
    }

    @Override
    public Message getSuperDiff() {
        return new Message(this.c_d_id, this.c_w_id, this.c_first, this.c_middle, this.c_last, this.c_street_1, this.c_street_2, this.c_city, this.c_state, this.c_zip, this.c_phone, this.c_since, this.c_credit, this.c_credit_lim, this.c_discount, this.c_balance, this.c_ytd_payment, this.c_payment_cnt, this.c_delivery_cnt, this.c_data);
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
//        this.c_d_id = (int) objectDiff.getNext();
//        this.c_w_id = (int) objectDiff.getNext();
//        this.c_first = (String) objectDiff.getNext();
//        this.c_middle = (String) objectDiff.getNext();
//        this.c_last = (String) objectDiff.getNext();
//        this.c_street_1 = (String) objectDiff.getNext();
//        this.c_street_2 = (String) objectDiff.getNext();
//        this.c_city = (String) objectDiff.getNext();
//        this.c_state = (String) objectDiff.getNext();
//        this.c_zip = (int) objectDiff.getNext();
//        this.c_phone = (String) objectDiff.getNext();
//        this.c_since = (long) objectDiff.getNext();
//        this.c_credit = (String) objectDiff.getNext();
//        this.c_credit_lim = (int) objectDiff.getNext();
//        this.c_discount = (double) objectDiff.getNext();
//        this.c_balance = (double) objectDiff.getNext();
//        this.c_ytd_payment = (double) objectDiff.getNext();
//        this.c_payment_cnt = (int) objectDiff.getNext();
//        this.c_delivery_cnt = (int) objectDiff.getNext();
//        this.c_data = (String) objectDiff.getNext();
    }

    @Override
    public String toString() {
        c_first.clear();
        c_middle.clear();
        c_last.clear();
        c_street_1.clear();
        c_street_2.clear();
        c_city.clear();
        c_state.clear();
        c_phone.clear();
        c_credit.clear();
        c_data.clear();

        return (
                "\n***************** Customer ********************" +
                        "\n*         req_id = " + c_req_id +
                        "\n*           c_id = " + c_id +
                        "\n*         c_d_id = " + c_d_id +
                        "\n*         c_w_id = " + c_w_id +
                        "\n*     c_discount = " + c_discount +
                        "\n*       c_credit = " + StandardCharsets.ISO_8859_1.decode(c_credit) +
                        "\n*         c_last = " + StandardCharsets.ISO_8859_1.decode(c_last) +
                        "\n*        c_first = " + StandardCharsets.ISO_8859_1.decode(c_first) +
                        "\n*   c_credit_lim = " + c_credit_lim +
                        "\n*      c_balance = " + c_balance +
                        "\n*  c_ytd_payment = " + c_ytd_payment +
                        "\n*  c_payment_cnt = " + c_payment_cnt +
                        "\n* c_delivery_cnt = " + c_delivery_cnt +
                        "\n*     c_last_req = " + c_last_order +
                        "\n*     c_street_1 = " + StandardCharsets.ISO_8859_1.decode(c_street_1) +
                        "\n*     c_street_2 = " + StandardCharsets.ISO_8859_1.decode(c_street_2) +
                        "\n*         c_city = " + StandardCharsets.ISO_8859_1.decode(c_city) +
                        "\n*        c_state = " + StandardCharsets.ISO_8859_1.decode(c_state) +
                        "\n*          c_zip = " + c_zip +
                        "\n*        c_phone = " + StandardCharsets.ISO_8859_1.decode(c_phone) +
                        "\n*        c_since = " + c_since +
                        "\n*       c_middle = " + StandardCharsets.ISO_8859_1.decode(c_middle) +
                        "\n*         c_data = " + StandardCharsets.ISO_8859_1.decode(c_data) +
                        "\n**********************************************"
        );
    }

    @Override
    public String toCSVString() {
        c_first.flip();
        c_middle.flip();
        c_last.flip();
        c_street_1.flip();
        c_street_2.flip();
        c_city.flip();
        c_state.flip();
        c_phone.flip();
        c_credit.flip();
        c_data.flip();

        ArrayList<String> ret = new ArrayList<>();
        ret.add("Customer");
        ret.add(String.valueOf(c_w_id));
        ret.add(String.valueOf(c_d_id));
        ret.add(String.valueOf(c_id));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_last)));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_first)));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_state)));
        ret.add(String.valueOf(c_zip));
        ret.add(String.valueOf(c_discount));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_middle)));
        ret.add(String.valueOf(c_ytd_payment));
        ret.add(String.valueOf(c_balance));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_phone)));
        ret.add(String.valueOf(c_since));
        ret.add(String.valueOf(c_credit_lim));
        ret.add(String.valueOf(c_delivery_cnt));
        ret.add(String.valueOf(c_payment_cnt));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_street_1)));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_city)));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_street_2)));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_credit)));
        ret.add(String.valueOf(StandardCharsets.ISO_8859_1.decode(c_data)));

        return String.join(",", ret);

//        return ("Customer," +
//                c_w_id + "," +
//                c_d_id + "," +
//                c_id + "," +
//                StandardCharsets.ISO_8859_1.decode(c_last) + "," +
//                StandardCharsets.ISO_8859_1.decode(c_first) + "," +
//                StandardCharsets.ISO_8859_1.decode(c_state) + "," +
//                c_zip + "," +
//                c_discount + "," +
//                StandardCharsets.ISO_8859_1.decode(c_middle) + "," +
//                c_ytd_payment + "," +
//                c_balance + "," +
//                StandardCharsets.ISO_8859_1.decode(c_phone) + "," +
//                c_since + "," +
//                c_credit_lim + "," +
//                c_delivery_cnt + "," +
//                c_payment_cnt + "," +
//                StandardCharsets.ISO_8859_1.decode(c_street_1) + "," +
//                StandardCharsets.ISO_8859_1.decode(c_city) + "," +
//                StandardCharsets.ISO_8859_1.decode(c_street_2) + "," +
//                StandardCharsets.ISO_8859_1.decode(c_credit) + "," +
//                StandardCharsets.ISO_8859_1.decode(c_data)
//
//        );
    }

//    @Override
//    public String toString() {
//        return (
//                "\n***************** Warehouse ********************" +
//                        "\n*       w_id = " + buffer.getInt(startPos) +
//                        "\n*      w_ytd = " + buffer.getDouble(startPos + 4) +
//                        "\n*      w_tax = " + buffer.getDouble(startPos + 14) +
//                        "\n*     w_name = " + buffer.getInt(startPos + 34) +
//                        "\n* w_street_1 = " + buffer.getInt(startPos + 54) +
//                        "\n* w_street_2 = " + buffer.getInt(startPos + 56) +
//                        "\n*     w_city = " + buffer.getInt(startPos + 65) +
//                        "\n*    w_state = " + buffer.getInt(startPos + 73) +
//                        "\n*      w_zip = " + buffer.getInt(startPos + 81) +
//                        "\n**********************************************"
//        );
//    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":c_w_id=" + c_w_id + ":c_d_id=" + c_d_id + ":c_id=" + c_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"c_w_id", "c_d_id", "c_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
//        return "c_w_id,c_d_id,c_id,c_last,c_first,c_state,c_zip,c_discount,c_middle,c_ytd_payment,c_balance,c_phone,c_since,c_credit_lim,c_delivery_cnt,c_payment_cnt,model,c_street_1,c_city,c_street_2,c_credit,c_data";
        return "c_w_id,c_d_id,c_id,c_last,c_first,c_state,c_zip,c_discount,c_middle,c_ytd_payment,c_balance,c_phone,c_since,c_credit_lim,c_delivery_cnt,c_payment_cnt,c_street_1,c_city,c_street_2,c_credit,c_data";
    }

    public static String getHeader() {
        return "Header,Customer,c_w_id,c_d_id,c_id,c_last,c_first,c_state,c_zip,c_discount,c_middle,c_ytd_payment,c_balance,c_phone,c_since,c_credit_lim,c_delivery_cnt,c_payment_cnt,c_street_1,c_city,c_street_2,c_credit,c_data";
    }
}
