package adv.clickhouse;

import adv.clickhouse.model.DbEvent;

import static adv.clickhouse.ChType.TUPLE;
import static adv.clickhouse.ChType.UINT32;

@ChTable("TestTupleEvent")
public class TestTupleEvent implements DbEvent {

    @ChColumn(name = "columnUInt32", type = UINT32)
    private Integer columnUInt32;

    @ChColumn(name = "columnTuple", type = TUPLE)
    private TestTuple columnTuple;

    public Integer getColumnUInt32() {
        return columnUInt32;
    }

    public void setColumnUInt32(Integer columnUInt32) {
        this.columnUInt32 = columnUInt32;
    }

    public TestTuple getColumnTuple() {
        return columnTuple;
    }

    public void setColumnTuple(TestTuple columnTuple) {
        this.columnTuple = columnTuple;
    }
}
