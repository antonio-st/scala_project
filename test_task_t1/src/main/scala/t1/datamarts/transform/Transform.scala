package t1.datamarts.transform

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import t1.datamarts.extract.Extract
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import t1.datamarts.Parameters
import t1.datamarts.transform.Transform._

class Transform extends Logging {

  // настройка логирования
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  // аргументы необходимо при запуске jar передавать с ним, здесь объявлены для демонстрации работы
  val args = Seq("--load-date", "2020")
  val conf = new Parameters(args)
  val dl = new Extract()

  def run() = {

    log.warn("Расчет SR_11 и SR_13")
    val sr11Sr13DF: DataFrame = getSr11Sr13(dl.fctLoanAccountBalanceExtr, dl.cdAccountDF, dl.cdLoanAgreementDF)
    log.warn("Готово")

    log.warn("Расчет SR_15 и SR_17")
    val sr15Sr17DF: DataFrame = getSr15Sr17(sr11Sr13DF, dl.cdAgreementXCustomerDF, dl.cdInternalOrgDetailDF)
    log.warn("Готово")
    sr15Sr17DF.show()



  }

}

object Transform {

  /**
   * @return Расчет SR_11 и SR_13
   */
  private def getSr11Sr13(fctLoanAccountBalanceExtr: DataFrame,
           cdAccountDF: DataFrame,
           cdLoanAgreementDF: DataFrame
          ): DataFrame = {

    fctLoanAccountBalanceExtr.as("flab")
      .join(cdAccountDF.as("ca"), col("flab.account_rk") === col("ca.account_rk")
      && col("flab.account_num") === col("ca.account_num"))
      .join(cdLoanAgreementDF.as("la"), Seq("agreement_rk"))
  }

  /**
   * @return Расчет SR_15 и SR_17
   */
  private def getSr15Sr17(sr11Sr13DF: DataFrame,
                      cdAgreementXCustomerDF: DataFrame,
                      cdInternalOrgDetailDF: DataFrame
                     ): DataFrame = {
    sr11Sr13DF.as("sr13")
      .join(cdAgreementXCustomerDF.as("axc"), col("sr13.agreement_rk") === col("axc.agreement_rk")
      && col("sr13.customer_rk") === col("axc.customer_rk"))
      .join(cdInternalOrgDetailDF.as("iod"), Seq("internal_org_id"))

  }

}
