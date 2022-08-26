package ch.usi.dslab.lel.dynastar.tpcc.rows;

import ch.usi.dslab.bezerra.netwrapper.Message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


public class Stock extends Row {
    public int s_req_id;

    public final MODEL model = MODEL.STOCK;
    public int s_w_id;  //PRIMARY KEY 1
    public int s_i_id;  //PRIMARY KEY 2
    public int s_order_cnt;
    public int s_remote_cnt;
    public int s_quantity;
    public double s_ytd;
    public ByteBuffer s_data = ByteBuffer.allocate(50);  // size: 50
    public ByteBuffer s_dist_01 = ByteBuffer.allocate(24);  // size: 24
    public ByteBuffer s_dist_02 = ByteBuffer.allocate(24);  // size: 24
    public ByteBuffer s_dist_03 = ByteBuffer.allocate(24);  // size: 24
    public ByteBuffer s_dist_04 = ByteBuffer.allocate(24);  // size: 24
    public ByteBuffer s_dist_05 = ByteBuffer.allocate(24);  // size: 24
    public ByteBuffer s_dist_06 = ByteBuffer.allocate(24);  // size: 24
    public ByteBuffer s_dist_07 = ByteBuffer.allocate(24);  // size: 24
    public ByteBuffer s_dist_08 = ByteBuffer.allocate(24);  // size: 24
    public ByteBuffer s_dist_09 = ByteBuffer.allocate(24);  // size: 24
    public ByteBuffer s_dist_10 = ByteBuffer.allocate(24);  // size: 24

//    public static transient ByteBuffer buffer = ByteBuffer.allocateDirect(MODEL.STOCK.rowSize);

    public Stock() {

    }

    public Stock(int s_w_id, int s_i_id) {
        this.s_w_id = s_w_id;
        this.s_i_id = s_i_id;
//        this.setStrObjId();
    }

    public void setWarehouse (int warehouse) {
        s_w_id = warehouse;
    }

    public void toBuffer(ByteBuffer buffer) {
        int startPos = buffer.position();
        buffer.putInt(s_req_id);
        buffer.putInt(s_w_id);
        buffer.putInt(s_i_id);
        buffer.putInt(s_order_cnt);
        buffer.putInt(s_remote_cnt);
        buffer.putInt(s_quantity);
        buffer.putDouble(s_ytd);
        buffer.put(s_data.clear());

        buffer.position(startPos+82);  // 32+50
        buffer.put(s_dist_01.clear());

        buffer.position(startPos+106);  // 32+50+24
        buffer.put(s_dist_02.clear());

        buffer.position(startPos+130);  // 32+50+24*2
        buffer.put(s_dist_03.clear());

        buffer.position(startPos+154);  // 32+50+24*3
        buffer.put(s_dist_04.clear());

//        buffer.put(stock.s_dist_05.getBytes(StandardCharsets.ISO_8859_1));
        buffer.position(startPos+178);  // 32+50+24*4
        buffer.put(s_dist_05.clear());

        buffer.position(startPos+202);  // 32+50+24*5
        buffer.put(s_dist_06.clear());

        buffer.position(startPos+226);  // 32+50+24*6
        buffer.put(s_dist_07.clear());

        buffer.position(startPos+250);  // 32+50+24*7
        buffer.put(s_dist_08.clear());

        buffer.position(startPos+274);  // 32+50+24*8
        buffer.put(s_dist_09.clear());

        buffer.position(startPos+298);  // 32+50+24*9
        buffer.put(s_dist_10.clear());
//        buffer.clear();
    }

    public void fromBuffer(ByteBuffer buffer) {
        s_req_id = buffer.getInt();
        s_w_id = buffer.getInt();
        s_i_id = buffer.getInt();
        s_order_cnt = buffer.getInt();
        s_remote_cnt = buffer.getInt();
        s_quantity = buffer.getInt();
        s_ytd = buffer.getDouble();

        int nextPos = buffer.position() + 50;
        s_data = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_01 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_02 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_03 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_04 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_05 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_06 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_07 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_08 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_09 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);

        nextPos = buffer.position() + 24;
        s_dist_10 = buffer.limit(nextPos).slice();
        buffer.clear().position(nextPos);
    }

    public void setReqId(int id) {
        s_req_id = id;
    }

    @Override
    public void updateFromDiff(Message objectDiff) {
//        this.s_order_cnt = (int) objectDiff.getNext();
//        this.s_remote_cnt = (int) objectDiff.getNext();
//        this.s_quantity = (int) objectDiff.getNext();
//        this.s_ytd = (double) objectDiff.getNext();
//        this.s_data = (String) objectDiff.getNext();
//        this.s_dist_01 = (String) objectDiff.getNext();
//        this.s_dist_02 = (String) objectDiff.getNext();
//        this.s_dist_03 = (String) objectDiff.getNext();
//        this.s_dist_04 = (String) objectDiff.getNext();
//        this.s_dist_05 = (String) objectDiff.getNext();
//        this.s_dist_06 = (String) objectDiff.getNext();
//        this.s_dist_07 = (String) objectDiff.getNext();
//        this.s_dist_08 = (String) objectDiff.getNext();
//        this.s_dist_09 = (String) objectDiff.getNext();
//        this.s_dist_10 = (String) objectDiff.getNext();
    }


    @Override
    public Message getSuperDiff() {
        return new Message(this.s_order_cnt, this.s_remote_cnt, this.s_quantity, this.s_ytd, this.s_data, this.s_dist_01, this.s_dist_02, this.s_dist_03, this.s_dist_04, this.s_dist_05, this.s_dist_06, this.s_dist_07, this.s_dist_08, this.s_dist_09, this.s_dist_10);
    }

    public String toString() {
        s_data.clear();
        s_dist_01.clear();
        s_dist_02.clear();
        s_dist_03.clear();
        s_dist_04.clear();
        s_dist_05.clear();
        s_dist_06.clear();
        s_dist_07.clear();
        s_dist_08.clear();
        s_dist_09.clear();
        s_dist_10.clear();

        return (
                "\n***************** Stock ********************" +
                        "\n*       req_id = " + s_req_id +
                        "\n*       s_i_id = " + s_i_id +
                        "\n*       s_w_id = " + s_w_id +
                        "\n*   s_quantity = " + s_quantity +
                        "\n*        s_ytd = " + s_ytd +
                        "\n*  s_order_cnt = " + s_order_cnt +
                        "\n* s_remote_cnt = " + s_remote_cnt +
                        "\n*       s_data = " + StandardCharsets.ISO_8859_1.decode(s_data) +
                        "\n*    s_dist_01 = " + StandardCharsets.ISO_8859_1.decode(s_dist_01) +
                        "\n*    s_dist_02 = " + StandardCharsets.ISO_8859_1.decode(s_dist_02) +
                        "\n*    s_dist_03 = " + StandardCharsets.ISO_8859_1.decode(s_dist_03) +
                        "\n*    s_dist_04 = " + StandardCharsets.ISO_8859_1.decode(s_dist_04) +
                        "\n*    s_dist_05 = " + StandardCharsets.ISO_8859_1.decode(s_dist_05) +
                        "\n*    s_dist_06 = " + StandardCharsets.ISO_8859_1.decode(s_dist_06) +
                        "\n*    s_dist_07 = " + StandardCharsets.ISO_8859_1.decode(s_dist_07) +
                        "\n*    s_dist_08 = " + StandardCharsets.ISO_8859_1.decode(s_dist_08) +
                        "\n*    s_dist_09 = " + StandardCharsets.ISO_8859_1.decode(s_dist_09) +
                        "\n*    s_dist_10 = " + StandardCharsets.ISO_8859_1.decode(s_dist_10) +
                        "\n**********************************************"
        );

//        return (
//
//                "\n***************** Stock ********************" +
//                        "\n*       s_i_id = " + s_i_id +
//                        "\n*       s_w_id = " + s_w_id +
//                        "\n*   s_quantity = " + s_quantity +
//                        "\n*        s_ytd = " + s_ytd +
//                        "\n*  s_order_cnt = " + s_order_cnt +
//                        "\n* s_remote_cnt = " + s_remote_cnt +
//                        "\n*       s_data = " + s_data +
//                        "\n*    s_dist_01 = " + s_dist_01 +
//                        "\n*    s_dist_02 = " + s_dist_02 +
//                        "\n*    s_dist_03 = " + s_dist_03 +
//                        "\n*    s_dist_04 = " + s_dist_04 +
//                        "\n*    s_dist_05 = " + s_dist_05 +
//                        "\n*    s_dist_06 = " + s_dist_06 +
//                        "\n*    s_dist_07 = " + s_dist_07 +
//                        "\n*    s_dist_08 = " + s_dist_08 +
//                        "\n*    s_dist_09 = " + s_dist_09 +
//                        "\n*    s_dist_10 = " + s_dist_10 +
//                        "\n**********************************************"
//        );
    }

    @Override
    public String toCSVString() {
        s_data.flip();
        s_dist_01.flip();
        s_dist_02.flip();
        s_dist_03.flip();
        s_dist_04.flip();
        s_dist_05.flip();
        s_dist_06.flip();
        s_dist_07.flip();
        s_dist_08.flip();
        s_dist_09.flip();
        s_dist_10.flip();

        return "Stock," +
                s_w_id + "," +
                s_i_id + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_01) + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_02) + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_03) + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_04) + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_05) + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_06) + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_07) + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_08) + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_09) + "," +
                StandardCharsets.ISO_8859_1.decode(s_dist_10) + "," +
                s_order_cnt + "," +
                StandardCharsets.ISO_8859_1.decode(s_data) + "," +
                s_quantity + "," +
                s_ytd + "," +
                s_remote_cnt + ",";
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":s_w_id=" + s_w_id + ":s_i_id=" + s_i_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"s_w_id", "s_i_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
        return "s_w_id,s_i_id,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_order_cnt,s_data,s_quantity,s_ytd,s_remote_cnt";
//        return "s_w_id,s_i_id,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_order_cnt,s_data,model,s_quantity,s_ytd,s_remote_cnt";
    }

    public static String getHeader() {
        return "Header,Stock,s_w_id,s_i_id,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_order_cnt,s_data,s_quantity,s_ytd,s_remote_cnt";
    }

}  // end Stock