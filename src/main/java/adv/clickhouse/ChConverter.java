package adv.clickhouse;

import org.jetbrains.annotations.Nullable;

public interface ChConverter {

    /**
     * форматируем в нативный clickhouse sql в поле VALUES одно поле из DTO
     *
     * @param fieldType java-тип поля в dto
     * @param fieldValue ссылка на значение поля
     * @return
     */
    String formatObject(@Nullable Class<?> fieldType, @Nullable Object fieldValue);

    /**
     * парсим из jdbc одно поле
     *
     * @param fieldType java-тип поля в dto
     * @param sqlName имя sql колонки
     * @param jdbcValue объект значение из jdbc драйвера
     */
    Object parseObject(Class<?> fieldType, String sqlName, Object jdbcValue);

}
