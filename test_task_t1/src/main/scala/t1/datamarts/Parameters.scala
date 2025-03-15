package t1.datamarts

import org.rogach.scallop._

class Parameters(arguments: Seq[String]) extends ScallopConf(arguments) {
  val loadDate: ScallopOption[Int] = opt[Int](required = true, name = "load-date")
  verify()

}
