package t1.datamarts

import org.rogach.scallop._

/**
 *
 * Парсинг и валидация входных параметров для загрузки
 */
class Parameters(arguments: Seq[String]) extends ScallopConf(arguments) {
  val loadDate: ScallopOption[String] = opt[String](required = true, name = "load-date")
  verify()

}
