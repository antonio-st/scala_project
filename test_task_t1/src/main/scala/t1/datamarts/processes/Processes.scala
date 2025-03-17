package t1.datamarts.processes
import org.apache.spark.internal.Logging
import t1.datamarts.extract.Extract
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import t1.datamarts.Parameters
import t1.datamarts.transform.Transform

class Processes extends Logging{

  def processes() = {

    val transf = new Transform
    val resultTemp: DataFrame = transf.run()

    resultTemp
      .withColumn("TRANZACTION_DATE", date_format(to_date(col("PROCESSED_DTTM")), "yyyy-MM"))
  }
//  val dl = new Extract()

  // Реализация инкремента по году
//    val fctLoanAccountBalanceProc: DataFrame = dl.fctLoanAccountBalanceExtr
    //      .withColumn("CD", add_months(current_date(), -12 * 5))
//    .filter(col("TRANZACTION_DATE") >= add_months(current_date(), -12 * 5))

//  val cdAccountProc: DataFrame = dl.cdAccountDF
//  val cdLoanAgreementProc: DataFrame = dl.cdLoanAgreementDF
//  val cdAgreementXCustomerProc: DataFrame = dl.cdAgreementXCustomerDF
//  val cdIndividualCustomerProc: DataFrame = dl.cdIndividualCustomerDF
//  val cdGlobalIndividualCustomerProc: DataFrame = dl.cdGlobalIndividualCustomerDF
//  val techLoanRepaymentScheduleProc: DataFrame = dl.techLoanRepaymentScheduleDF
//  val cdInternalOrgDetailProc: DataFrame = dl.cdInternalOrgDetailDF


}
