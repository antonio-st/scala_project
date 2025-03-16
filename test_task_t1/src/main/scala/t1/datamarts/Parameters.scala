package t1.datamarts

import org.rogach.scallop._

class Parameters(arguments: Seq[String]) extends ScallopConf(arguments) {
  val loadDate: ScallopOption[String] = opt[String](required = true, name = "load-date")
  verify()

}
