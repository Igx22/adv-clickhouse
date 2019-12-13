package adv.clickhouse;

import static adv.clickhouse.ChType.STRING;
import static adv.clickhouse.ChType.UINT32;

@ChTable("")
public class NestedEvent {
    @ChColumn(name = "value1", type = UINT32)
    private Integer value1;

    @ChColumn(name = "value2", type = UINT32)
    private Integer value2;

    @ChColumn(name = "value3", type = STRING)
    private String value3;

    public Integer getValue1() {
        return value1;
    }

    public void setValue1(Integer value1) {
        this.value1 = value1;
    }

    public Integer getValue2() {
        return value2;
    }

    public void setValue2(Integer value2) {
        this.value2 = value2;
    }

    public String getValue3() {
        return value3;
    }

    public void setValue3(String value3) {
        this.value3 = value3;
    }

    @Override
    public String toString() {
        return "NestedEvent{" +
                "value1=" + value1 +
                ", value2=" + value2 +
                '}';
    }
}
