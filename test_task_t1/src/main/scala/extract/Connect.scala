package extract

import org.apache.spark.sql.SparkSession

trait Connect {

    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("test_t1")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

}
