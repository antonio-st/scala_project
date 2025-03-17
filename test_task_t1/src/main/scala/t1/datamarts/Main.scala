package t1.datamarts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import t1.datamarts.extract.Connect
import t1.datamarts.load.Load
import t1.datamarts.Parameters


object Main extends Connect {

  // настройка логирования
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  // аргументы необходимо при запуске jar передавать с ним, здесь объявлены для демонстрации работы
  val args = Seq("--load-date", "2020-09")
  val conf = new Parameters(args)

  def main(args: Array[String]): Unit = {

    val start = new Load
    start.load()

  }

}
