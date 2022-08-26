package ch.usi.dslab.lel.dynastar.tpcc.rows;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastar.tpcc.objects.ObjId;
//import ch.usi.dslab.lel.dynastarv2.probject.ObjId;


public class OrderLine extends Row {
    public final MODEL model = MODEL.ORDERLINE;
    public int ol_w_id;
    public int ol_d_id;
    public int ol_o_id;
    public int ol_number;
    public int ol_i_id;
    public int ol_supply_w_id;
    public int ol_quantity;
    public long ol_delivery_d;
    public double ol_amount;
    public String ol_dist_info;

    public OrderLine() {

    }


    public OrderLine(ObjId id) {
        this.setId(id);
    }

    public OrderLine(int ol_w_id, int ol_d_id, int ol_o_id, int ol_number) {
        this.ol_w_id = ol_w_id;
        this.ol_o_id = ol_o_id;
        this.ol_d_id = ol_d_id;
        this.ol_number = ol_number;
//        this.setStrObjId();
    }

    public void setWarehouse (int warehouse) {
        ol_w_id = warehouse;
    }


    @Override
    public void updateFromDiff(Message objectDiff) {
        this.ol_w_id = (int) objectDiff.getNext();
        this.ol_d_id = (int) objectDiff.getNext();
        this.ol_o_id = (int) objectDiff.getNext();
        this.ol_number = (int) objectDiff.getNext();
        this.ol_i_id = (int) objectDiff.getNext();
        this.ol_supply_w_id = (int) objectDiff.getNext();
        this.ol_quantity = (int) objectDiff.getNext();
        this.ol_delivery_d = (long) objectDiff.getNext();
        this.ol_amount = (double) objectDiff.getNext();
        this.ol_dist_info = (String) objectDiff.getNext();
    }

    @Override
    public Message getSuperDiff() {
        return new Message(this.ol_d_id, this.ol_o_id, this.ol_number, this.ol_i_id, this.ol_supply_w_id, this.ol_quantity, this.ol_delivery_d, this.ol_amount, this.ol_dist_info);
    }

    public String toString() {
        return (
                "\n***************** OrderLine ********************" +
                        "\n*        ol_w_id = " + ol_w_id +
                        "\n*        ol_d_id = " + ol_d_id +
                        "\n*        ol_o_id = " + ol_o_id +
                        "\n*      ol_number = " + ol_number +
                        "\n*        ol_i_id = " + ol_i_id +
                        "\n*  ol_delivery_d = " + ol_delivery_d +
                        "\n*      ol_amount = " + ol_amount +
                        "\n* ol_supply_w_id = " + ol_supply_w_id +
                        "\n*    ol_quantity = " + ol_quantity +
                        "\n*   ol_dist_info = " + ol_dist_info +
                        "\n**********************************************"
        );
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":ol_w_id=" + ol_w_id + ":ol_d_id=" + ol_d_id + ":ol_o_id=" + ol_o_id + ":ol_number=" + ol_number;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"ol_w_id", "ol_d_id", "ol_o_id", "ol_number"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
        return "ol_w_id,ol_d_id,ol_o_id,ol_i_id,ol_supply_w_id,ol_dist_info,ol_delivery_d,ol_number,ol_quantity,ol_amount";
//        return "ol_w_id,ol_d_id,ol_o_id,ol_i_id,ol_supply_w_id,ol_dist_info,ol_delivery_d,ol_number,model,ol_quantity,ol_amount";
    }

    public static String getHeader() {
        return "Header,OrderLine,ol_w_id,ol_d_id,ol_o_id,ol_i_id,ol_supply_w_id,ol_dist_info,ol_delivery_d,ol_number,ol_quantity,ol_amount";
    }
}  // end OrderLine