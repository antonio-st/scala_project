package t1.datamarts.load
import t1.datamarts.extract.Variables._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import t1.datamarts.extract.Connect


import t1.datamarts.transform.Transform

class Load extends Logging with Connect{

  // настройка логирования
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  val transf = new Transform
  val resultDF: DataFrame = transf.run()

  def load() = {

    log.warn(s"Запись результата orc в $resultTable")

    resultDF
      .write
      .partitionBy("TRANZACTION_DATE")
      .mode("overwrite")
      .option("compression", "snappy")
//      .format("orc")
//      .insertInto(resultTable)
      .orc(resultTable)
    log.warn("Готово")

    log.warn(s"Запись результата csv в $resultTableCsv")

    resultDF
      .coalesce(1)
      .write
      .option("header", "True")
      .option("delimiter", ";")
      .csv(resultTableCsv)

    log.warn("Готово")

    log.warn(s"Проверка результата")
    spark
      .read
      .orc(resultTable)
      .show()

    log.warn("Готово")



  }
}
