package adv.clickhouse;

import java.lang.annotation.*;

/**
 * Вместо значения помеченного поля будет записан id батча, в котором происходила вставка
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface BatchId {
}
