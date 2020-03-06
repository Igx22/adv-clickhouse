package adv.clickhouse;

// интерфейс для enum, пример 
public interface ChEnumeration {

   // объектный вид enum
   ChEnumeration findByChName(String chName);

    // текстовый вид enum (если будем класть в ENUM8 или STRING колонки)
   String getChName();

   // числовой вид enum (если будем класть в INTXX колонки)
   int getChIndex();

    ChEnumeration getByChIndex(int i);
}
