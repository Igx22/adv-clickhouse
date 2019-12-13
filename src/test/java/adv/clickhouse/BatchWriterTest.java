package adv.clickhouse;

import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


/*
  CREATE TABLE TestEvent
(
    columnBool UInt8,
    columnDate Date,
    columnDateTime DateTime,
    columnUInt64 UInt64,
    columnUInt32 UInt32,
    columnUInt16 UInt16,
    columnUInt8 UInt8,
    columnListUInt16 Array(UInt16),
    columnNested Nested(
    value1 UInt16,
    value2 UInt16)
) ENGINE = Memory
 */
public class BatchWriterTest {

    @Test
    public void test2() throws Exception {
        ChAnnotationScanner chAnnotationScanner = new ChAnnotationScanner("db0", "adv.clickhouse");
        {
            TestEvent event = new TestEvent();
            event.setColumnBool(true);
            LocalDate dt = LocalDate.of(2017, 6, 21);
            event.setColumnDate(dt);
            List<NestedEvent> nestedEventList = new ArrayList<>();
            {
                NestedEvent nestedEvent = new NestedEvent();
                nestedEvent.setValue1(111);
                nestedEvent.setValue3("test1");
                nestedEventList.add(nestedEvent);
            }
            {
                nestedEventList.add(null);
            }
            {
                NestedEvent nestedEvent = new NestedEvent();
                nestedEvent.setValue1(333);
                nestedEvent.setValue3("test3");
                nestedEventList.add(nestedEvent);
            }
            event.setColumnNested(nestedEventList);
            StringBuilder sql = new StringBuilder();
            BatchWriter<TestEvent> batch = new BatchWriter<>(0, TestEvent.class, sql, chAnnotationScanner, 0);
            batch.push(event);
            batch.finish();
            System.out.println(sql);
            assertEquals("INSERT INTO db0.TestEvent (columnBool,columnDate,columnDateTime,columnUInt64,columnUInt32,columnUInt16,columnUInt8,columnListUInt16,columnNested.value1,columnNested.value2,columnNested.value3) VALUES(1,'2017-06-21','0000-00-00 00:00:00',0,0,0,0,[],[111,0,333],[0,0,0],['test1','','test3']);", sql.toString());
            assertEquals(batch.getSize(), sql.length());
        }
    }

    @Test
    public void test1() throws Exception {
        ChAnnotationScanner chAnnotationScanner = new ChAnnotationScanner("db0", "adv.clickhouse");
        {
            TestEvent event = new TestEvent();
            event.setColumnBool(true);
            LocalDate dt = LocalDate.of(2017, 6, 21);
            event.setColumnDate(dt);
            event.setColumnDateTime(LocalDateTime.of(dt, LocalTime.of(16, 38)));
            event.setColumnUInt8(8);
            event.setColumnUInt16(65000);
            event.setColumnUInt32(650000000);
            event.setColumnUInt64(650000000000L);

            List<Integer> ids = new ArrayList<>();
            ids.add(1);
            ids.add(2);
            event.setColumnListUInt16(ids);

            List<NestedEvent> nestedEventList = new ArrayList<>();
            {
                NestedEvent nestedEvent = new NestedEvent();
                nestedEvent.setValue1(111);
                nestedEvent.setValue2(222);
                nestedEventList.add(nestedEvent);
            }
            {
                NestedEvent nestedEvent = new NestedEvent();
                nestedEvent.setValue1(333);
                nestedEvent.setValue2(444);
                nestedEventList.add(nestedEvent);
            }
            event.setColumnNested(nestedEventList);
            StringBuilder sql = new StringBuilder();
            BatchWriter<TestEvent> batch = new BatchWriter<>(0, TestEvent.class, sql, chAnnotationScanner, 0);
            batch.push(event);
            batch.finish();
            System.out.println(sql);
            assertEquals("INSERT INTO db0.TestEvent (columnBool,columnDate,columnDateTime,columnUInt64,columnUInt32,columnUInt16,columnUInt8,columnListUInt16,columnNested.value1,columnNested.value2,columnNested.value3) VALUES(1,'2017-06-21','2017-06-21 16:38:00',650000000000,650000000,65000,8,[1,2],[111,333],[222,444],['','']);", sql.toString());
        }
        {
            TestEvent event = new TestEvent();
            List<Integer> ids = new ArrayList<>();
            ids.add(1);
            event.setColumnListUInt16(ids);

            List<NestedEvent> nestedEventList = new ArrayList<>();
            {
                NestedEvent nestedEvent = new NestedEvent();
                nestedEvent.setValue1(111);
                nestedEvent.setValue2(222);
                nestedEventList.add(nestedEvent);
            }
            event.setColumnNested(nestedEventList);
            StringBuilder sql = new StringBuilder();
            BatchWriter<TestEvent> batch = new BatchWriter<>(0, TestEvent.class, sql, chAnnotationScanner, 0);
            batch.push(event);
            batch.finish();
            System.out.println(sql);
            assertEquals("INSERT INTO db0.TestEvent (columnBool,columnDate,columnDateTime,columnUInt64,columnUInt32,columnUInt16,columnUInt8,columnListUInt16,columnNested.value1,columnNested.value2,columnNested.value3) VALUES(0,'0000-00-00','0000-00-00 00:00:00',0,0,0,0,[1],[111],[222],['']);", sql.toString());
        }
        {
            TestEvent event = new TestEvent();
            StringBuilder sql = new StringBuilder();
            BatchWriter<TestEvent> batch = new BatchWriter<>(0, TestEvent.class, sql, chAnnotationScanner, 0);
            batch.push(event);
            batch.finish();
            System.out.println(sql);
            assertEquals("INSERT INTO db0.TestEvent (columnBool,columnDate,columnDateTime,columnUInt64,columnUInt32,columnUInt16,columnUInt8,columnListUInt16,columnNested.value1,columnNested.value2,columnNested.value3) VALUES(0,'0000-00-00','0000-00-00 00:00:00',0,0,0,0,[],[],[],[]);", sql.toString());
        }
    }

    @Test
    public void test3() throws Exception {
        ChAnnotationScanner chAnnotationScanner = new ChAnnotationScanner("db0", "adv.clickhouse");
        {
            TestEvent event = new TestEvent();
            event.setColumnBool(true);
            LocalDate dt = LocalDate.of(2017, 6, 21);
            event.setColumnDate(dt);
            List<NestedEvent> nestedEventList = new ArrayList<>();
            {
                NestedEvent nestedEvent = new NestedEvent();
                nestedEvent.setValue1(111);
                nestedEvent.setValue3("test1");
                nestedEventList.add(nestedEvent);
            }
            {
                nestedEventList.add(null);
            }
            {
                NestedEvent nestedEvent = new NestedEvent();
                nestedEvent.setValue1(333);
                nestedEvent.setValue3("test3");
                nestedEventList.add(nestedEvent);
            }
            event.setColumnNested(nestedEventList);
            StringBuilder sql = new StringBuilder();
            BatchWriter<TestEvent> batch = new BatchWriter<>(0, TestEvent.class, sql, chAnnotationScanner, 0);
            batch.push(event);
            batch.push(event);
            batch.finish();
            System.out.println(sql);
            assertEquals("INSERT INTO db0.TestEvent (columnBool,columnDate,columnDateTime,columnUInt64,columnUInt32,columnUInt16,columnUInt8,columnListUInt16,columnNested.value1,columnNested.value2,columnNested.value3) VALUES(1,'2017-06-21','0000-00-00 00:00:00',0,0,0,0,[],[111,0,333],[0,0,0],['test1','','test3']),(1,'2017-06-21','0000-00-00 00:00:00',0,0,0,0,[],[111,0,333],[0,0,0],['test1','','test3']);", sql.toString());
            assertEquals(batch.getSize(), sql.length());
        }
    }
}