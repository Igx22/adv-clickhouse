package adv.clickhouse;

import adv.clickhouse.model.DbEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static adv.util.Check.notNull;

public class BatchWriter<T extends DbEvent> {
    private static final Logger log = LoggerFactory.getLogger(BatchWriter.class);

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

    public void push(T obj) {
        int rollbackPosition = insert.length();
        if (!firstRow) {
            insert.append(",");
        }

        insert.append("(");
        boolean firstValue = true;
        for (ChAnnotationScanner.ChFieldInfo chFieldInfo : chClassInfo.getFieldByJavaName().values()) {
            if (!firstValue) {
                insert.append(",");
            }
            firstValue = false;
            try {
                if (chFieldInfo.chType == ChType.ARRAY) {
                    chFieldInfo.doGetAndFormatArray(obj, insert);
                } else if (chFieldInfo.chType == ChType.NESTED) {
                    chFieldInfo.doGetAndFormatNested(obj, insert);
                } else if (chFieldInfo.isBatchId) {
                    insert.append(batchId);
                } else {
                    insert.append(chFieldInfo.doGetAndFormat(obj));
                }
            } catch (Exception e) {
                insert.setLength(rollbackPosition);
                log.error("error formatting field: {} {}", chFieldInfo.javaName, chFieldInfo, e);
                throw new IllegalStateException(e);
            }
        }
        insert.append(")");

        firstRow = false;
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
