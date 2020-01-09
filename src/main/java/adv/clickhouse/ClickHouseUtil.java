package adv.clickhouse;


import adv.util.BitUtil;
import adv.util.Check;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.stream.Collectors;

import static adv.util.Check.isTrue;


public class ClickHouseUtil {
    private static final DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter FORMATTER_YYYYMMDD_HHMMSS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String formatByte(byte value) {
        return Integer.toString(value);
    }

    public static String formatDate(LocalDate value) {
        if (value == null) {
            return "'0000-00-00'";
        }
        return "'" + FORMATTER_YYYYMMDD.format(value) + "'";
    }

    public static LocalDate parseDate(String value) {
        return LocalDate.parse(value, FORMATTER_YYYYMMDD);
    }

    public static String formatDateTime(LocalDateTime value) {
        if (value == null) {
            return "'0000-00-00 00:00:00'";
        }
        return "'" + FORMATTER_YYYYMMDD_HHMMSS.format(value) + "'";
    }

    public static LocalDateTime parseDateTime(String value) {
        return LocalDateTime.parse(value, FORMATTER_YYYYMMDD_HHMMSS);
    }

    public static String formatNumberArray(Collection<? extends Number> values, ChType convType) {
        if (values == null) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder(values.size() * 5);
        sb.append("[");
        sb.append(values.stream().map(aShort -> formatNumber(aShort, convType)).collect(Collectors.joining(",")));
        sb.append("]");
        return sb.toString();
    }


    public static String formatDecimalBoolean(Boolean value) {
        return Boolean.TRUE.equals(value) ? "1" : "0";
    }

    public static Boolean parseDecimalBoolean(String value) {
        return "1".equals(value);
    }

    public static Boolean parseDecimalBoolean(Long value) {
        return Long.valueOf(1).equals(value);
    }


    public static String formatString(String value) {
        if (value == null) {
            return "''";
        }
        return "'" + ru.yandex.clickhouse.ClickHouseUtil.escape(value) + "'";
    }

    public static String parseString(String value) {
        return value;
    }

    public static String formatStringBase16(String value) {
        if (value == null) {
            return "''";
        }
        if (!BitUtil.isBase16String((String) value, -1)) {
            throw new IllegalStateException(String.format("invalid content: value: %s", value));
        }
        return new StringBuilder().append("unhex('").append(ru.yandex.clickhouse.ClickHouseUtil.escape(value)).append("')").toString();
    }

    public static String parseStringBase16(String value) {
        String result = BitUtil.toBase16(value.getBytes());
        return result;
    }

    public static String formatDouble(Double value) {
        return Double.toString(value);
    }

    public static Double parseDouble(String value) {
        return Double.parseDouble(value);
    }

    public static Long parseLong(String value) {
        return Long.parseLong(value);
    }


    public static Integer parseInt(String value) {
        return Integer.parseInt(value);
    }

    public static Short parseShort(String value) {
        return Short.parseShort(value);
    }

    public static String formatNumber(Number aValue, ChType conv) {
        if (aValue == null) {
            return "0";
        }
        long value = aValue.longValue();
        if (conv == ChType.INT8) {
            isTrue(Byte.MIN_VALUE <= value && value <= BitUtil.INT8_MAX, "value out of range %s", value);
        }
        if (conv == ChType.INT16) {
            isTrue(Short.MIN_VALUE <= value && value <= BitUtil.INT16_MAX, "value out of range %s", value);
        }
        if (conv == ChType.INT32) {
            isTrue(Integer.MIN_VALUE <= value && value <= BitUtil.INT32_MAX, "value out of range %s", value);
        }
        if (conv == ChType.INT64) {
        }
        if (conv == ChType.UINT8) {
            isTrue(0 <= value && value <= BitUtil.UINT8_MAX, "value out of range %s", value);
        }
        if (conv == ChType.UINT16) {
            isTrue(0 <= value && value <= BitUtil.UINT16_MAX, "value out of range %s", value);
        }
        if (conv == ChType.UINT32) {
            isTrue(0 <= value && value <= BitUtil.UINT32_MAX, "value out of range %s", value);
        }
        if (conv == ChType.UINT64) {
            return Long.toUnsignedString(value);
        }
        if (conv == ChType.FLOAT32) {
            if (aValue instanceof Float) {
                return Float.toString((Float) aValue);
            } else if (aValue instanceof Double) {
                return Double.toString((Double) aValue);
            } else {
                throw new IllegalStateException("unsupported type" + aValue.getClass());
            }
        }
        return Long.toString(value);
    }

    @NotNull
    public static Object parseNumber(Class<?> fieldType, String sqlName, Object jdbcValue) {
        if (Long.class.isAssignableFrom(fieldType)) {
            return ((Number) jdbcValue).longValue();

        } else if (Integer.class.isAssignableFrom(fieldType)) {
            return ((Number) jdbcValue).intValue();
        } else if (Short.class.isAssignableFrom(fieldType)) {
            return ((Number) jdbcValue).shortValue();
        }
        throw new IllegalStateException(String.format("invalid fieldType: %s fieldName: %s jdbcValue: %s", fieldType, sqlName, jdbcValue));
    }

    public static String singleQuote(String val) {
        return "'" + val + "'";
    }

    public static String formatChEnumToString(ChEnumeration fieldValue) {
        Check.notNull(fieldValue, "Got ENUM with null value, please provide a reasonable default for enum");
        return singleQuote(fieldValue.getChName());
    }

    public static String formatChEnumToNumber(ChEnumeration fieldValue, ChType conv) {
        long value;
        if (fieldValue == null) {
            value = 0;
        } else {
            value = fieldValue.getChIndex();
        }
        if (conv == ChType.INT8) {
            isTrue(Byte.MIN_VALUE <= value && value <= BitUtil.INT8_MAX, "value out of range %s", value);
        }
        if (conv == ChType.INT16) {
            isTrue(Short.MIN_VALUE <= value && value <= BitUtil.INT16_MAX, "value out of range %s", value);
        }
        if (conv == ChType.INT32) {
            isTrue(Integer.MIN_VALUE <= value && value <= BitUtil.INT32_MAX, "value out of range %s", value);
        }
        if (conv == ChType.INT64) {
        }
        if (conv == ChType.UINT8) {
            isTrue(0 <= value && value <= BitUtil.UINT8_MAX, "value out of range %s", value);
        }
        if (conv == ChType.UINT16) {
            isTrue(0 <= value && value <= BitUtil.UINT16_MAX, "value out of range %s", value);
        }
        if (conv == ChType.UINT32) {
            isTrue(0 <= value && value <= BitUtil.UINT32_MAX, "value out of range %s", value);
        }
        if (conv == ChType.UINT64) {
            return Long.toUnsignedString(value);
        }
        return Long.toString(value);
    }
}
