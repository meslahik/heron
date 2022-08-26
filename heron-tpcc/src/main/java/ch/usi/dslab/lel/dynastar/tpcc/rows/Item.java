package ch.usi.dslab.lel.dynastar.tpcc.rows;

import ch.usi.dslab.bezerra.netwrapper.Message;
import ch.usi.dslab.lel.dynastar.tpcc.objects.ObjId;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
//import ch.usi.dslab.lel.dynastarv2.probject.ObjId;


public class Item extends Row {

    public final MODEL model = MODEL.ITEM;
    public int i_id; // PRIMARY KEY
    public int i_im_id;
    public double i_price;
    public String i_name; // size: 24
    public String i_data; // size: 50

    transient ByteBuffer buffer;

    public Item() {
        buffer = ByteBuffer.allocateDirect(model.getRowSize());
    }

    public Item(ObjId id) {
        this.setId(id);
    }

    public Item(int i_id) {
        this.i_id = i_id;
//        this.setStrObjId();
    }

    public ByteBuffer toBuffer() {
        buffer.clear();

        buffer.putInt(i_id);
        buffer.putInt(i_im_id);
        buffer.putDouble(i_price);
        buffer.put(i_name.getBytes(StandardCharsets.ISO_8859_1));
        buffer.put(i_data.getBytes(StandardCharsets.ISO_8859_1));

        return buffer;
    }

    public void fromBuffer(ByteBuffer _buffer) {
        _buffer.clear();

        i_id = _buffer.getInt();
        i_im_id = _buffer.getInt();
        i_price = _buffer.getDouble();

        ByteBuffer bufferName = _buffer.position(16).limit(40).slice();
        i_name = String.valueOf(StandardCharsets.ISO_8859_1.decode(bufferName));

        ByteBuffer bufferData = _buffer.position(40).limit(90).slice();
        i_data = String.valueOf(StandardCharsets.ISO_8859_1.decode(bufferData));

    }


    @Override
    public void updateFromDiff(Message objectDiff) {
        this.i_im_id = (int) objectDiff.getNext();
        this.i_price = (double) objectDiff.getNext();
        this.i_name = (String) objectDiff.getNext();
        this.i_data = (String) objectDiff.getNext();
    }

    @Override
    public Message getSuperDiff() {
         return new Message(this.i_im_id, this.i_price, this.i_name, this.i_data);
    }

    public String toString() {
        return (
                "\n***************** Item ********************" +
                        "\n*    i_id = " + i_id +
                        "\n*  i_name = " + i_name +
                        "\n* i_price = " + i_price +
                        "\n*  i_data = " + i_data +
                        "\n* i_im_id = " + i_im_id +
                        "\n**********************************************"
        );
    }

    @Override
    public String getObjIdString() {
        return model.getName() + ":i_id=" + i_id;
    }

    @Override
    public String[] getPrimaryKeys() {
        return new String[]{"i_id"};
    }

    @Override
    public String getModelName() {
        return this.model.getName();
    }

    @Override
    public String getCSVHeader() {
        return "i_id,i_im_id,i_data,i_price,i_name";
//        return "i_id,i_im_id,i_data,i_price,i_name,model";
    }

    public static String getHeader() {
        return "Header,Item,i_id,i_im_id,i_data,i_price,i_name";
    }
}  // end Item