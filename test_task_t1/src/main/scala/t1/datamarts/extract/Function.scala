package t1.datamarts.extract

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType

class Function extends Logging with Connect{

  /**
   *
   * @param table название таблицы
   * @param schema схема
   * @param delimiter разделитель аттрибутов
   * @param source путь к файлу истонику
   * @param cols выборка аттрибутов
   * @param condition фильтрация
   * @return Функция для загрузки данных из источника csv
   */
  def extractTable(table: String,
                   schema: StructType,
                   delimiter: String = ";",
                   source: String,
                   cols: Seq[Column] = Seq(col("*")),
                   condition: Column = lit(true)
                  ): DataFrame = {
    log.warn(s"Загрузка таблицы $table")

    val df = spark
      .read
      .option("header", "true")
      .option("delimiter", s"$delimiter")
      .schema(schema)
      .csv(s"$source")
      .filter(condition)
      .select(cols: _*)
    log.warn(s"Таблица $table загружена")
    df
  }

}
