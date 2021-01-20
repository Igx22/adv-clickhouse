package adv.clickhouse;

import adv.clickhouse.model.DbEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static adv.util.Check.notNull;

public class BatchWriter<T extends DbEvent> {
    private static final Logger log = LoggerFactory.getLogger(BatchWriter.class);

    // если UTF-16 строка превысит эту длину, при аллокации мы получим OutOfMemoryError: Requested array size exceeds VM limit
    public static final long MAX_LENGTH = Integer.MAX_VALUE;

    private final int batchId;
    private final Class<T> dataType;
    private final StringBuilder insert;
    private final ChAnnotationScanner chAnnotationScanner;
    private final long creationTs;
    private boolean firstRow = true;
    private ChAnnotationScanner.ChClassInfo chClassInfo;

    public BatchWriter(int batchId, Class<T> dataType, StringBuilder stringBuilder, ChAnnotationScanner chAnnotationScanner, long creationTs) {
        this.batchId = batchId;
        this.dataType = dataType;
        this.insert = stringBuilder;
        this.chAnnotationScanner = chAnnotationScanner;
        this.creationTs = creationTs;
        writePreamble();
    }

    private void writePreamble() {
        insert.setLength(0);

        chClassInfo = chAnnotationScanner.getReflectionMap().get(dataType);
        notNull(chClassInfo, "unsupported class %s", dataType);
        insert.append("INSERT INTO ");
        insert.append(chAnnotationScanner.dbName);
        insert.append(".");
        insert.append(chClassInfo.getTableName());
        insert.append(" (");
        boolean first1 = true;
        for (ChAnnotationScanner.ChFieldInfo fieldInfo : chClassInfo.getFieldByJavaName().values()) {
            if (!first1) {
                insert.append(",");
            }
            first1 = false;
            if (fieldInfo.chType == ChType.NESTED) {
                Class listType = fieldInfo.getListType();
                ChAnnotationScanner.ChClassInfo nestedChClassInfo = chAnnotationScanner.getReflectionMap().get(listType);
                boolean first2 = true;
                for (ChAnnotationScanner.ChFieldInfo nestFieldInfo : nestedChClassInfo.getFieldByJavaName().values()) {
                    if (!first2) {
                        insert.append(",");
                    }
                    first2 = false;
                    insert.append(fieldInfo.getSqlName());
                    insert.append(".");
                    insert.append(nestFieldInfo.getSqlName());
                }
            } else {
                insert.append(fieldInfo.getSqlName());
            }
        }
        insert.append(")");
        insert.append(" VALUES");
    }

    public boolean push(T obj) {
        int rollbackPosition = insert.length();

        StringBuilder buf = new StringBuilder();

        if (!firstRow) {
            buf.append(",");
        }

        buf.append("(");
        boolean firstValue = true;
        for (ChAnnotationScanner.ChFieldInfo chFieldInfo : chClassInfo.getFieldByJavaName().values()) {
            if (!firstValue) {
                buf.append(",");
            }
            firstValue = false;
            try {
                switch (chFieldInfo.chType) {
                    case ARRAY: chFieldInfo.doGetAndFormatArray(obj, buf); break;
                    case NESTED: chFieldInfo.doGetAndFormatNested(obj, buf); break;
                    case TUPLE: chFieldInfo.doGetAndFormatTuple(obj, buf); break;
                    default:
                        if (chFieldInfo.isBatchId) {
                            buf.append(batchId);
                        } else {
                            buf.append(chFieldInfo.doGetAndFormat(obj));
                        }
                }
            } catch (Exception e) {
                log.error("error formatting field: {} {}", chFieldInfo.javaName, chFieldInfo, e);
                throw new IllegalStateException(e);
            }
        }
        buf.append(")");

        if(buf.length() > MAX_LENGTH - insert.length()) {
            return false;
        } else {
            insert.append(buf);
        }

        firstRow = false;
        return true;
    }

    public boolean hasData() {
        return !firstRow;
    }

    public int getSize() {
        return insert.length();
    }

    public void finish() {
        insert.append(";");
    }

    public String getStatement() {
        return insert.toString();
    }

    public String getTable() {
        return chClassInfo.getTableName();
    }

    public long getCreationTs() {
        return creationTs;
    }

    public int getBatchId() {
        return batchId;
    }
}
