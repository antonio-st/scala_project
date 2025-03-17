package t1.datamarts.load
import t1.datamarts.extract.Variables._
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import t1.datamarts.extract.Connect
import t1.datamarts.transform.Transform
import t1.datamarts.processes.Processes

class Load extends Logging with Connect{

  // настройка логирования
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  val transf = new Processes
  val resultDF: DataFrame = transf.processes()

  def load() = {

    // Указываем, что нужно перезаписывать только рассчитанные партиции
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    log.warn(s"Запись результата orc в $resultTable")

    resultDF
      .write
      .partitionBy("TRANZACTION_DATE")
      .mode(SaveMode.Overwrite)
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
      .mode(SaveMode.Overwrite)
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
