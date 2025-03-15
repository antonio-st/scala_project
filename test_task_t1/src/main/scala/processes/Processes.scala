package processes
import extract.Extract
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object Processes {

  val dl = new Extract()

  // Реализация инкремента по году
    val fctLoanAccountBalanceProc: DataFrame = dl.fctLoanAccountBalanceExtr
    .withColumn("TRANZACTION_YEAR", year(to_date(col("PROCESSED_DTTM"))))
    .filter(col("TRANZACTION_YEAR") >= 2020)

  val cdAccountProc: DataFrame = dl.cdAccountDF
  val cdLoanAgreementProc: DataFrame = dl.cdLoanAgreementDF
  val cdAgreementXCustomerProc: DataFrame = dl.cdAgreementXCustomerDF
  val cdIndividualCustomerProc: DataFrame = dl.cdIndividualCustomerDF
  val cdGlobalIndividualCustomerProc: DataFrame = dl.cdGlobalIndividualCustomerDF
  val techLoanRepaymentScheduleProc: DataFrame = dl.techLoanRepaymentScheduleDF
  val cdInternalOrgDetailProc: DataFrame = dl.cdInternalOrgDetailDF


}
