package adv.clickhouse;

import org.reflections.Reflections;
import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

import static adv.util.Check.isTrue;
import static adv.util.Check.notNull;

public class ChAnnotationScanner {
    private static final Logger log = LoggerFactory.getLogger(ChAnnotationScanner.class);
    private Map<Class, ChClassInfo> reflectionMap = new HashMap<>();
    private final String pkg;
    final String dbName;

    public ChAnnotationScanner(String dbName, @NotNull String pkg) {
        this.dbName = dbName;
        this.pkg = pkg;
        doScan();
    }

    /**
     * Ищем аннотации, сохраняя порядок полей,
     * в отличие от reflections.getFieldsAnnotatedWith(ChColumn.class);
     */
    static Set<Field> findFields(Class<?> classs, Class<? extends Annotation> ann) {
        Set<Field> set = new LinkedHashSet<>();
        Class<?> c = classs;
        while (c != null) {
            for (Field field : c.getDeclaredFields()) {
                if (field.isAnnotationPresent(ann)) {
                    set.add(field);
                }
            }
            c = c.getSuperclass();
        }
        return set;
    }

    private void doScan() {
        log.debug("searching for default CH classes in path adv.clickhouse.model");
        Reflections defaultClasses = new Reflections("adv.clickhouse.model",
                new TypeAnnotationsScanner(), new FieldAnnotationsScanner(), new SubTypesScanner());
        log.debug("searching for " + ChTable.class + " in path " + pkg);
        Reflections reflections = new Reflections(pkg,
                new TypeAnnotationsScanner(), new FieldAnnotationsScanner(), new SubTypesScanner());
        reflections.merge(defaultClasses);
        Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(ChTable.class);
        for (Class<?> clazz : annotatedClasses) {
            ChClassInfo classInfo = new ChClassInfo();
            classInfo.tableName = clazz.getAnnotation(ChTable.class).value();
            notNull(classInfo.tableName, "Table name is null in %s", clazz);
            reflectionMap.put(clazz, classInfo);
            final Set<Field> annotatedFields = findFields(clazz, ChColumn.class);
            final Set<Field> batchIdFields = findFields(clazz, BatchId.class);
            for (Field annotatedField : annotatedFields) {
                try {
                    PropertyDescriptor pd = new PropertyDescriptor(annotatedField.getName(), clazz);
                    final ChFieldInfo fieldInfo = new ChFieldInfo();
                    fieldInfo.field = annotatedField;
                    fieldInfo.javaName = annotatedField.getName();
                    fieldInfo.readMethod = pd.getReadMethod();
                    fieldInfo.writeMethod = pd.getWriteMethod();
                    fieldInfo.isBatchId = batchIdFields.contains(annotatedField);
                    ChColumn chColumnAnnotation = annotatedField.getAnnotation(ChColumn.class);
                    fieldInfo.chType = chColumnAnnotation.type();
                    fieldInfo.arrayType = chColumnAnnotation.arrayType();
                    fieldInfo.sqlName = chColumnAnnotation.name();
                    notNull(classInfo.tableName, "Column name is null in %s #%s", clazz, annotatedField);
                    notNull(fieldInfo.readMethod);
                    notNull(fieldInfo.writeMethod);
                    notNull(chColumnAnnotation);
                    classInfo.fieldByJavaName.put(fieldInfo.javaName, fieldInfo);
                    classInfo.fieldBySqlName.put(fieldInfo.sqlName, fieldInfo);
                    if (fieldInfo.chType == ChType.NESTED || fieldInfo.chType == ChType.ARRAY) {
                        isTrue(List.class.isAssignableFrom(fieldInfo.getPlainFieldClazz()),
                                "NESTED/ARRAY requires java.util.List: %s", annotatedField);
                    }

                } catch (Exception e) {
                    log.error("error processing field: {}", annotatedField);
                    throw new IllegalStateException(e);
                }
            }
            log.debug("processed class: {}", classInfo);
        }
        log.debug("found: {} classes", reflectionMap.size());
    }

    Map<Class, ChClassInfo> getReflectionMap() {
        return reflectionMap;
    }


    static class ChClassInfo {
        String tableName;
        Map<String, ChFieldInfo> fieldByJavaName = new LinkedHashMap<>();
        Map<String, ChFieldInfo> fieldBySqlName = new LinkedHashMap<>();


        String getTableName() {
            return tableName;
        }

        Map<String, ChFieldInfo> getFieldByJavaName() {
            return fieldByJavaName;
        }

        public Map<String, ChFieldInfo> getFieldBySqlName() {
            return fieldBySqlName;
        }

        @Override
        public String toString() {
            return "ChClassInfo{" +
                    "fieldByJavaName=" + fieldByJavaName +
                    '}';
        }
    }

    class ChFieldInfo {
        // имя поля в sql
        String sqlName;
        // имя поля в java dto
        String javaName;
        // ссылка на поле
        Field field;
        // тип в clickhouse
        ChType chType;
        // для массивов: тип элемента в clickhouse
        ChType arrayType;
        // метод для чтения поля
        Method readMethod;
        // метод для записи поля
        Method writeMethod;
        // признак того, что поле содержит batchId
        public boolean isBatchId;

        String getSqlName() {
            return sqlName;
        }

        Class getListType() {
            isTrue(chType == ChType.NESTED || chType == ChType.ARRAY);
            ParameterizedType stringListType = (ParameterizedType) field.getGenericType();
            Class<?> stringListClass = (Class<?>) stringListType.getActualTypeArguments()[0];
            return stringListClass;
        }

        Class getPlainFieldClazz() {
            return field.getType(); //(Class) ((ParameterizedType) field.getGenericType()).getRawType();
        }

        // это массив из наших объектов
        // [NestedEvent{value1=1, value2=2}, NestedEvent{value1=3, value2=4}]
        // который мы преобразуем в 2 массива clickhouse вида: [value1 array], [value2 array]
        // [1,3],[2,4]
        void doGetAndFormatNested(Object obj, StringBuilder buf) {
            isTrue(chType == ChType.NESTED);
            try {
                final List itemList = (List) readMethod.invoke(obj);
                if (log.isTraceEnabled()) {
                    if (log.isTraceEnabled()) {
                        log.trace("doGetAndFormatArray: field: {} value:{}", javaName, itemList);
                    }
                }
                final Class<?> itemType = getListType();
                ChClassInfo itemClassInfo = reflectionMap.get(itemType);
                // для каждого поля value1, value2 из nested структуры
                boolean first1 = true;
                for (ChFieldInfo nestedField : itemClassInfo.getFieldByJavaName().values()) {
                    if (!first1) {
                        buf.append(",");
                    }
                    first1 = false;
                    buf.append("[");
                    // для каждого элемента из списка объектов
                    boolean first2 = true;
                    if (itemList != null) {
                        for (Object item : itemList) {
                            if (!first2) {
                                buf.append(",");
                            }
                            first2 = false;
                            String itemStr;
                            if (item == null) {
                                itemStr = nestedField.chType.formatObject(null, null);
                            } else {
                                itemStr = nestedField.doGetAndFormat(item);
                            }
                            buf.append(itemStr);
                        }
                    }
                    buf.append("]");
                }
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }

        void doGetAndFormatArray(Object obj, StringBuilder buf) {
            isTrue(chType == ChType.ARRAY);
            try {
                final List itemList = (List) readMethod.invoke(obj);
                if (log.isTraceEnabled()) {
                    if (log.isTraceEnabled()) {
                        log.trace("doGetAndFormatArray: field: {} value:{}", javaName, itemList);
                    }
                }
                final Class<?> itemType = getListType();
                buf.append("[");
                boolean first = true;
                if (itemList != null) {
                    for (Object item : itemList) {
                        if (!first) {
                            buf.append(",");
                        }
                        first = false;
                        String itemStr = arrayType.formatObject(itemType, item);
                        buf.append(itemStr);
                    }
                }
                buf.append("]");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        // внимание: форматирует в нативный clickhouse sql
        String doGetAndFormat(Object obj) {
            try {
                final Object fieldValue = readMethod.invoke(obj);
                if (log.isTraceEnabled()) {
                    if (log.isTraceEnabled()) {
                        log.trace("doGetAndFormat: field: {} value:{}", javaName, fieldValue);
                    }
                }
                final Class<?> fieldType = field.getType();
                return chType.formatObject(fieldType, fieldValue);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }

        // внимание: парсит из jdbc объектов (то что выдает jdbc driver)
        void doSetFromJdbc(Object obj, String sqlName, Object jdbcValue) {
            if (log.isTraceEnabled()) {
                log.trace("doSetFromJdbc(): fieldName: {}, fieldValue: {} / {}", sqlName, jdbcValue, jdbcValue.getClass().getSimpleName());
            }
            final Class<?> fieldType = field.getType();
            try {
                Object fieldValue = chType.parseObject(fieldType, sqlName, jdbcValue);
                writeMethod.invoke(obj, fieldValue);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public String toString() {
            return "ChFieldInfo{" + javaName + '}';
        }
    }


    public static class DbEventMapper<T> implements org.springframework.jdbc.core.RowMapper<T> {
        private final Class<T> clazz;
        private final ChAnnotationScanner annotationScanner;

        public DbEventMapper(Class<T> clazz, ChAnnotationScanner annotationScanner) {
            this.clazz = clazz;
            this.annotationScanner = annotationScanner;
        }


        @Override
        public T mapRow(ResultSet rs, int rowNum) throws SQLException {
            try {
                ChClassInfo chClassInfo = annotationScanner.getReflectionMap().get(clazz);
                T obj = clazz.newInstance();
                ResultSetMetaData rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    String columnName = rsmd.getColumnName(i);
                    ChFieldInfo chFieldInfo = chClassInfo.getFieldBySqlName().get(columnName);
                    Object columnValue = rs.getObject(i);
                    if (chFieldInfo != null) {
                        chFieldInfo.doSetFromJdbc(obj, columnName, columnValue);
                    } else {
                        log.debug("mapRow(): Unsupported: sqlName: {}, fieldType: {}, sqlValue: {}", columnName, clazz, columnValue);
                    }
                }
                if (log.isDebugEnabled()) {
                    log.debug("mapRow(): row#{} parsed object: {}", rowNum, obj);
                }
                return obj;
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
