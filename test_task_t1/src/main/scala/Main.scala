import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import extract.Connect
import extract.Extract


object Main extends Logging with Connect {
  def main(args: Array[String]): Unit = {


    val dl = new Extract()

    dl.cdAccountDF.show()
    dl.cdLoanAgreementDF.show()
    dl.cdAgreementXCustomerDF.show()
    dl.cdIndividualCustomerDF.show()
    dl.cdGlobalIndividualCustomerDF.show()
    dl.fctLoanAccountBalanceDF.show()
    dl.techLoanRepaymentScheduleDF.show()
    dl.cdInternalOrgDetailDF.show()




//    val popGroupDF: DataFrame = Seq(
//      (1, "Pink Floyd", 25),
//      (2, "ZZ-Top", 10),
//      (3, "The Beatles", 20),
//      (4, "Pink Floyd", 3),
//      (5, "The Beatles", 2),
//      (6, "ZZ-Top", 5),
//      (7, "Pink Floyd", 3),
//    )
//      .toDF("id", "name", "albums")
//
//    val countryDF: DataFrame = Seq(
//      (1, "England"),
//      (2, "USA"),
//      (3, "England"),
//      (4, "England"),
//      (5, "England"),
//      (6, "USA"),
//      (7, "England")
//    ).toDF("id", "city")
//
//    popGroupDF.show()
//    countryDF.show()
//
//    val groupDF: DataFrame =
//      popGroupDF
//        .groupBy("name")
//        .agg(
//      sum("albums")
//        ).as("all_album")
//
//    groupDF.show()
//
//    val joinDF: DataFrame =
//      popGroupDF
////        .join(countryDF.as("c"), $"pg.id" === $"c.id")
//        .join(countryDF, Seq("id"))
//
//    joinDF.show()
//
//    log.warn("\n========= Запись в файл ==============\n")
//    joinDF
//      .write
//      .partitionBy("name")
//      .mode("overwrite")
//      .orc("group")
//    log.warn("============= Готово ===========")
//
//    log.warn(" ==== Чтение orc ==== ")
//
//    val readDF: DataFrame =
//      spark
//        .read
//        .orc("group")
//
//    readDF.show()
//
//val windowSpec = Window.partitionBy("name").orderBy("albums")
//
//    readDF
//      .select(
//        col("*"),
//        sum("albums").over(windowSpec).as("wf"),
//        rank().over(windowSpec).as("rank"),
//        dense_rank().over(windowSpec).as("dense_rank"),
//        row_number().over(windowSpec).as("row_number")
//      ).show()


  }


}