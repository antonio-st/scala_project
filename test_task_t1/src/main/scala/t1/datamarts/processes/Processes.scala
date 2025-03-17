package t1.datamarts.processes
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import t1.datamarts.transform.Transform

class Processes extends Logging{

  def processes() = {

    val transf = new Transform
    val resultTemp: DataFrame = transf.run()

    resultTemp
      .withColumn("TRANZACTION_DATE", date_format(to_date(col("PROCESSED_DTTM")), "yyyy-MM"))
  }


}
