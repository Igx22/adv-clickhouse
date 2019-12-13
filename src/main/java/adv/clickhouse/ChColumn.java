package adv.clickhouse;

import java.lang.annotation.*;

@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ChColumn {

    String name();

    ChType type();

    ChType arrayType() default ChType.NONE;

}

