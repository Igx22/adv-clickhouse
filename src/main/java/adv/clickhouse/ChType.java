package adv.clickhouse;

import java.time.LocalDate;
import java.time.LocalDateTime;

public enum ChType implements ChConverter {

    NONE {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            throw new UnsupportedOperationException();
        }
    },

    // строка должна содержать только base16 символы, мы пишем ее как байты чз hex/unhex
    HEX_STRING {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatStringBase16((String) fieldValue);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseStringBase16((String) jdbcValue);
        }
    },

    STRING {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatString((String) fieldValue);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseString((String) jdbcValue);
        }
    },

    ARRAY {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            throw new IllegalStateException();
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return null;
            // todo поддержка парсинга массивов [1,2,3]
//            String arrayStr = (String) jdbcValue;
//            if (arrayStr == null || arrayStr.length() < 2) {
//                return null;
//            }
//            String arrayContents = arrayStr.substring(1, arrayStr.length());
//            String[] arrayElements = arrayContents.split(",");
//            throw new IllegalStateException();
        }
    },

    NESTED {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return null;
            // todo поддержка парсинга вложенных структур [1,2,3]
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            throw new IllegalStateException();
        }
    },

    DATE {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatDate((LocalDate) fieldValue);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ((java.sql.Date) jdbcValue).toLocalDate();
        }
    },

    DATE_TIME {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatDateTime((LocalDateTime) fieldValue);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            //todo пока не работает т.к. кривой драйвер теряет дату!!!
            //doSetFromJdbc(): fieldName: event_date_time, fieldValue: 19:16:04 / Time
            //LocalTime localTime = ((Time) sqlValue).toLocalTime();
            //writeMethod.invoke(obj, LocalDateTime.now().with);
            return null;
        }
    },

    /*BOOLEAN {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatDecimalBoolean((Boolean) fieldValue);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseDecimalBoolean((Integer) jdbcValue);
        }
    },*/

    UINT8 {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            if (Boolean.class.isAssignableFrom(fieldType)) {
                return ClickHouseUtil.formatDecimalBoolean((Boolean) fieldValue);
            }
            return ClickHouseUtil.formatNumber((Number) fieldValue, this);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            if (Boolean.class.isAssignableFrom(fieldType)) {
                return ClickHouseUtil.parseDecimalBoolean((Long) jdbcValue);
            }
            return ClickHouseUtil.parseNumber(fieldType, sqlName, jdbcValue);
        }


    },

    UINT16 {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatNumber((Number) fieldValue, this);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseNumber(fieldType, sqlName, jdbcValue);
        }
    },

    UINT32 {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatNumber((Number) fieldValue, this);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseNumber(fieldType, sqlName, jdbcValue);
        }
    },

    FLOAT32 {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatNumber((Number) fieldValue, this);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseNumber(fieldType, sqlName, jdbcValue);
        }
    },


    UINT64 {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatNumber((Number) fieldValue, this);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseNumber(fieldType, sqlName, jdbcValue);
        }
    },

    INT8 {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatNumber((Number) fieldValue, this);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseNumber(fieldType, sqlName, jdbcValue);
        }
    },

    INT16 {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatNumber((Number) fieldValue, this);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseNumber(fieldType, sqlName, jdbcValue);
        }
    },

    INT32 {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatNumber((Number) fieldValue, this);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseNumber(fieldType, sqlName, jdbcValue);
        }
    },

    INT64 {
        @Override
        public String formatObject(Class<?> fieldType, Object fieldValue) {
            return ClickHouseUtil.formatNumber((Number) fieldValue, this);
        }

        @Override
        public Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue) {
            return ClickHouseUtil.parseNumber(fieldType, sqlName, jdbcValue);
        }
    };


}
