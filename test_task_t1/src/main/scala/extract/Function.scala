package extract

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.StructType
import org.apache.log4j.{Level, Logger}

class Function extends Logging with Connect{

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
