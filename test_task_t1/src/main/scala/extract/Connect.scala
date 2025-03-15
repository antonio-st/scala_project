package extract

import org.apache.spark.sql.SparkSession

trait Connect {
    /**
     * инициализация Spark сессии
     */
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("test_t1")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

}
