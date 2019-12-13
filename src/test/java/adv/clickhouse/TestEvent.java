package adv.clickhouse;


import adv.clickhouse.model.DbEvent;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static adv.clickhouse.ChType.*;


@ChTable("TestEvent")
public class TestEvent implements DbEvent {

    @ChColumn(name = "columnBool", type = UINT8)
    private Boolean columnBool;

    @ChColumn(name = "columnDate", type = DATE)
    private LocalDate columnDate;

    @ChColumn(name = "columnDateTime", type = DATE_TIME)
    private LocalDateTime columnDateTime;

    @ChColumn(name = "columnUInt64", type = UINT64)
    private Long columnUInt64;

    @ChColumn(name = "columnUInt32", type = UINT32)
    private Integer columnUInt32;

    @ChColumn(name = "columnUInt16", type = UINT16)
    private Integer columnUInt16;

    @ChColumn(name = "columnUInt8", type = UINT8)
    private Integer columnUInt8;

    @ChColumn(name = "columnListUInt16", type = ARRAY, arrayType = UINT16)
    private List<Integer> columnListUInt16 = new ArrayList<>();


    @ChColumn(name = "columnNested", type = NESTED)
    private List<NestedEvent> columnNested = new ArrayList<>();

    public Boolean getColumnBool() {
        return columnBool;
    }

    public void setColumnBool(Boolean columnBool) {
        this.columnBool = columnBool;
    }

    public LocalDate getColumnDate() {
        return columnDate;
    }

    public void setColumnDate(LocalDate columnDate) {
        this.columnDate = columnDate;
    }

    public LocalDateTime getColumnDateTime() {
        return columnDateTime;
    }

    public void setColumnDateTime(LocalDateTime columnDateTime) {
        this.columnDateTime = columnDateTime;
    }

    public Long getColumnUInt64() {
        return columnUInt64;
    }

    public void setColumnUInt64(Long columnUInt64) {
        this.columnUInt64 = columnUInt64;
    }

    public Integer getColumnUInt32() {
        return columnUInt32;
    }

    public void setColumnUInt32(Integer columnUInt32) {
        this.columnUInt32 = columnUInt32;
    }

    public Integer getColumnUInt16() {
        return columnUInt16;
    }

    public void setColumnUInt16(Integer columnUInt16) {
        this.columnUInt16 = columnUInt16;
    }

    public Integer getColumnUInt8() {
        return columnUInt8;
    }

    public void setColumnUInt8(Integer columnUInt8) {
        this.columnUInt8 = columnUInt8;
    }

    public List<Integer> getColumnListUInt16() {
        return columnListUInt16;
    }

    public void setColumnListUInt16(List<Integer> columnListUInt16) {
        this.columnListUInt16 = columnListUInt16;
    }

    public List<NestedEvent> getColumnNested() {
        return columnNested;
    }

    public void setColumnNested(List<NestedEvent> columnNested) {
        this.columnNested = columnNested;
    }
}
