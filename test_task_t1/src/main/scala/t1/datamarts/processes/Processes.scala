package t1.datamarts.processes
import t1.datamarts.extract.Extract
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import t1.datamarts.Parameters

object Processes {

  // аргументы необходимо при запуске jar передавать с ним, здесь объявлены для демонстрации работы
  val args = Seq("--load-date", "2020")
  val conf = new Parameters(args)

  val dl = new Extract()


  // Реализация инкремента по году
    val fctLoanAccountBalanceProc: DataFrame = dl.fctLoanAccountBalanceExtr
    .withColumn("TRANZACTION_DATE", year(to_date(col("PROCESSED_DTTM"))))
//      .withColumn("CD", add_months(current_date(), -12 * 5))
//    .filter(col("TRANZACTION_DATE") >= add_months(current_date(), -12 * 5))
      .filter(col("TRANZACTION_DATE") === conf.loadDate.apply())

  val cdAccountProc: DataFrame = dl.cdAccountDF
  val cdLoanAgreementProc: DataFrame = dl.cdLoanAgreementDF
  val cdAgreementXCustomerProc: DataFrame = dl.cdAgreementXCustomerDF
  val cdIndividualCustomerProc: DataFrame = dl.cdIndividualCustomerDF
  val cdGlobalIndividualCustomerProc: DataFrame = dl.cdGlobalIndividualCustomerDF
  val techLoanRepaymentScheduleProc: DataFrame = dl.techLoanRepaymentScheduleDF
  val cdInternalOrgDetailProc: DataFrame = dl.cdInternalOrgDetailDF


}
