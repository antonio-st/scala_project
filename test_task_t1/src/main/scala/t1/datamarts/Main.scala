package t1.datamarts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import t1.datamarts.extract.Connect
import t1.datamarts.load.Load


object Main extends Connect {

  // настройка логирования
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)


  def main(args: Array[String]): Unit = {

    val start = new Load
    start.load()

  }

}
