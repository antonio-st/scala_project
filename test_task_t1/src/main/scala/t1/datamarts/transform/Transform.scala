package t1.datamarts.transform

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import t1.datamarts.extract.Extract
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import t1.datamarts.Parameters
import t1.datamarts.transform.Transform._
import t1.datamarts.transform
import t1.datamarts.transform.Variables._

class Transform extends Logging {

  // настройка логирования
  Logger.getRootLogger.setLevel(Level.WARN)
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)

  // аргументы необходимо при запуске jar передавать с ним, здесь объявлены для демонстрации работы

  val dl = new Extract()

  def run() = {

    log.warn("Расчет SR_11 и SR_13")
    val sr11Sr13DF: DataFrame = getSr11Sr13(dl.fctLoanAccountBalanceExtr, dl.cdAccountDF, dl.cdLoanAgreementDF)
    log.warn("Готово")

    val cols = sr11Sr13DF.columns
    log.warn(s"${cols.foreach(x => println(x))}")

    log.warn("Расчет SR_15 и SR_17")
    val sr15Sr17DF: DataFrame = getSr15Sr17(sr11Sr13DF, dl.cdAgreementXCustomerDF, dl.cdInternalOrgDetailDF)
    log.warn("Готово")

    log.warn("Расчет SR_18, SR_19, SR_20")
    val sr18Sr20DF: DataFrame = getSr18Sr20(sr15Sr17DF, dl.cdInternalOrgDetailDF, dl.techLoanRepaymentScheduleDF)
    log.warn("Готово")

    log.warn("Расчет SR_22, SR_23, SR_24")
    val sr22Sr24DF: DataFrame = getSr22Sr24(sr18Sr20DF, dl.cdIndividualCustomerDF, dl.cdGlobalIndividualCustomerDF)
    log.warn("Готово")

    log.warn("Итоговый результат таблицы STG_CREDIT")
    val result: DataFrame = getResult(sr22Sr24DF)
    log.warn("Готово")

    result

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
      .join(cdLoanAgreementDF.as("la"), col("flab.agreement_rk") === col("la.agreement_rk"))
      .select(sr11Sr13Cols: _*)
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
      .join(cdInternalOrgDetailDF.as("iod"), col("sr13.internal_org_id") === col("iod.internal_org_id"))
      .select(sr15Sr17Cols: _*)
  }


  /**
   * @return Расчет SR_18 ,SR_19, SR_20
   */
  private def getSr18Sr20(sr15Sr17DF: DataFrame,
                          cdInternalOrgDetailDF: DataFrame,
                          techLoanRepaymentScheduleDF: DataFrame
                         ): DataFrame = {
  sr15Sr17DF.as("sr17")
      .join(cdInternalOrgDetailDF.as("iod"), col("init_internal_org_id") === col("sr17.internal_org_id"), "left")
      .join(techLoanRepaymentScheduleDF
      .groupBy("agreement_rk")
      .agg(
        max("NEXT_PAYMENT_FROM_DT").as("NEXT_PAYMENT_FROM_DT"),
        max("NEXT_MONTHLY_PAYMENT_AMT").as("NEXT_MONTHLY_PAYMENT_AMT")
      )
      .select(
        col("AGREEMENT_RK"),
        col("NEXT_PAYMENT_FROM_DT"),
        col("NEXT_MONTHLY_PAYMENT_AMT")
      ).as("sr19"), col("sr17.agreement_rk") === col("sr19.agreement_rk"), "left"
      )
      .select(sr18Sr20Cols: _*)
  }

  /**
   * @return Расчет SR_22, SR_23, SR_24
   */
  private def getSr22Sr24(sr18Sr20DF: DataFrame,
                          cdIndividualCustomerDF: DataFrame,
                          cdGlobalIndividualCustomerDF: DataFrame
                         ): DataFrame = {

    sr18Sr20DF.as("sr20")
      .join(cdIndividualCustomerDF.as("ic"), Seq("customer_rk"), "left")
      .join(cdGlobalIndividualCustomerDF.as("gic"), col("ic.customer_global_rk") === col("customer_mdm_rk"))

  }

  /**
   * @return Итоговый результат таблицы STG_CREDIT
   */
  private def getResult(sr22Sr24DF: DataFrame): DataFrame = {

    sr22Sr24DF
      .select(
        col("CUSTOMER_MDM_ID").as("CUSTOMER_MDM_ID"), // глобальный ид клиента
        col("CONTRACT_NUM").as("CONTRACT_NUM"), // номер контракта
        col("LOAN_AMT").as("LOAN_AMT"), // сумма кредита
        col("OPEN_DT").as("OPEN_DT"), // дата открытия договора
        col("sr20.BRANCH_ID").as("TERBANK_ID_FILIAL_VTB"), // ид филиала
        col("BRANCH_NM").as("TERBANK_NM_FILIAL_VTB"), // наименование филиала
        col("REGIONAL_OPER_OFFICE_ID").as("BRANCH_ID_ROO_VTB"), // ид офиса
        col("REGIONAL_OPER_OFFICE_NM").as("BRANCH_NM_ROO_VTB"), // наименование офиса
        col("INTERNAL_ORG_ID").as("SUBBRANCH_NM_PODRAZD_SCHETA_VTB"), // ид подразделения счета
        col("INTERNAL_ORG_NM").as("SUBBRANCH_ID_PODRAZD_SCHETA_VTB"), // наименование подразделения счета
        col("INTERNAL_ORG_ID").as("BRANCHBANK_ID_PODRAZD_VYDACHI_VTB"), // ид отделения инициатора
        col("INTERNAL_ORG_NM").as("BRANCHBANK_NM_PODRAZD_VYDACHI_VTB"), // наименование отделения инициатора
        col("ADDRESS").as("BRANCHBANK_ADR_PODRAZD_VYDACHI_VTB"), // адрес отделения инициатора (!! ошибка в ТЗ, неверное поле ADRESS)
        when(col("CURRENCY_ISO_CD") === "RUR", 810)
        .when(col("CURRENCY_ISO_CD") === "USD", 840)
        .when(col("CURRENCY_ISO_CD") === "EUR", 978)
        .when(col("CURRENCY_ISO_CD") === "CHF", 756)
        .when(col("CURRENCY_ISO_CD") === "JPY", 392)
          .otherwise(lit(null)).as("CURRENCY") ,// валюта
        substring(col("NEXT_PAYMENT_FROM_DT"), 9, 2).as("PAYMENT_DT"), // дата платежа значением атрибута
        col("NEXT_MONTHLY_PAYMENT_AMT").as("PAYMENT_AMT"), // платеж по ОД
        col("ACCOUNT_NUM").as("ACCOUNT_NUM"), // номер счета
        col("BALANCE_AMT").as("BALANCE_AMT"), // остаток на счете
        col("EFFECTIVE_FROM_DTTM").as("EFFECTIVE_FROM_DTTM"), // дата начала действия записи
        col("PROCESSED_DTTM"), // месяц/год инкремента
//        col("TRANZACTION_DATE") // месяц/год инкремента
      )

  }

}
