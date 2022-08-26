import ch.usi.dslab.lel.dynastar.tpcc.rows.Row;
import ch.usi.dslab.lel.dynastarv2.probject.ObjId;
import org.junit.Assert;
import org.junit.Test;

public class TestObjIdGenerator {

    @Test
    public void TestObjIdGenerator() {
        ObjId id = Row.genObjId("Warehouse", 1);
        Assert.assertEquals(id, new ObjId((100000000)));

        id = Row.genObjId("Warehouse", 2);
        Assert.assertEquals(id, new ObjId((200000000)));

        id = Row.genObjId("Warehouse", 64);
        Assert.assertEquals(id, new ObjId(6400000000L));

        id = Row.genObjId("Warehouse", 256);
        Assert.assertEquals(id, new ObjId(25600000000L));

        id = Row.genObjId("District", 256, 1);
        Assert.assertEquals(id, new ObjId(25601000000L));

        id = Row.genObjId("District", 256, 10);
        Assert.assertEquals(id, new ObjId(25610000000L));

        id = Row.genObjId("Customer", 256, 10, 1);
        Assert.assertEquals(id, new ObjId(25610000001L));

        id = Row.genObjId("Customer", 256, 10, 10);
        Assert.assertEquals(id, new ObjId(25610000010L));

        id = Row.genObjId("Customer", 256, 10, 3000);
        Assert.assertEquals(id, new ObjId(25610003000L));

        id = Row.genObjId("Item", 10);
        Assert.assertEquals(id, new ObjId(101100010L));

        id = Row.genObjId("Item", 99999);
        Assert.assertEquals(id, new ObjId(101199999L));

        id = Row.genObjId("Item", 100000);
        Assert.assertEquals(id, new ObjId(101200000L));


        id = Row.genObjId("Stock", 256, 1);
        Assert.assertEquals(id, new ObjId(25601200001L));

        id = Row.genObjId("Stock", 256, 10);
        Assert.assertEquals(id, new ObjId(25601200010L));

        id = Row.genObjId("Stock", 256, 100000);
        Assert.assertEquals(id, new ObjId(25601300000L));

        id = Row.genObjId("NewOrder", 256, 10, 1);
        Assert.assertEquals(id, new ObjId(25610010001L));

        id = Row.genObjId("NewOrder", 256, 10, 1000);
        Assert.assertEquals(id, new ObjId(25610011000L));

        id = Row.genObjId("History", 256, 5, 1);
        Assert.assertEquals(id, new ObjId(25605400001L));
        id = Row.genObjId("History", 256, 5, 100000);
        Assert.assertEquals(id, new ObjId(25605500000L));

        id = Row.genObjId("Order", 256, 5, 10, 9);
        Assert.assertEquals(id, new ObjId(25605500109L));
        id = Row.genObjId("Order", 256, 5, 3000,9);
        Assert.assertEquals(id, new ObjId(25605530009L));
    }
}
