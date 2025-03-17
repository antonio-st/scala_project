package t1.datamarts.extract

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import Variables._
import t1.datamarts.Main._


class Extract extends Function with Logging{

  // настройка логирования
    Logger.getRootLogger.setLevel(Level.WARN)
    val logger: Logger = Logger.getLogger(getClass.getName)
    logger.setLevel(Level.INFO)

  log.warn(s"Получена дата ${conf.loadDate.apply()}")

  log.warn("Загрузка источников")

  val cdAccountDF: DataFrame =
    extractTable("CD_ACCOUNT", cdAccountSchema, ";", cdAccountTable, cdAccountCol,
      col("CUSTOMER_TYPE_CD") === "Ч" && col("ACCOUNT_TYPE_CD") === "А" && col("IS_ACTIVE_FLG") === 1)

  val cdLoanAgreementDF: DataFrame =
    extractTable("CD_LOAN_AGREEMENT", cdLoanAgreementSchema, ";", cdLoanAgreementTable, cdLoanAgreementCol,
      col("CUSTOMER_TYPE_CD") === "Ч" && col("CONTRACT_TYPE_CD") != "ЗАЯВКА" && col("IS_ACTIVE_FLG") === 1)

    // ошибка в ТЗ или неточность - CUSTOMER_ROLE_CD=’ЗАЕМЩИК’ в источнике формат "Заемщик"
  val cdAgreementXCustomerDF: DataFrame =
    extractTable("CD_AGREEMENT_X_CUSTOMER", cdAgreementXCustomerSchema, ";", cdAgreementXCustomerTable, cdAgreementXCustomerCol,
      col("CUSTOMER_TYPE_CD") === "Ч" && lower(col("CUSTOMER_ROLE_CD")) === "заемщик" && col("IS_ACTIVE_FLG") === 1)

  val cdIndividualCustomerDF: DataFrame =
    extractTable("CD_INDIVIDUAL_CUSTOMER", cdIndividualCustomerSchema, ";", cdIndividualCustomerTable, cdIndividualCustomerCol,
      col("IS_ACTIVE_FLG") === 1)

  val cdGlobalIndividualCustomerDF: DataFrame =
    extractTable("CD_GLOBAL_INDIVIDUAL_CUSTOMER", CdGlobalIndividualCustomerSchema, ";", cdGlobalIndividualCustomerTable,
      CdGlobalIndividualCustomerCol, col("IS_ACTIVE_FLG") === 1)

  val fctLoanAccountBalanceExtr: DataFrame =
    extractTable("FCT_LOAN_ACCOUNT_BALANCE", fctLoanAccountBalanceSchema, ";", fctLoanAccountBalanceTable,
      fctLoanAccountBalanceCol, col("DELETED_FLG") =!= 1 && col("BALANCE_AMT") > 0)
        .filter(date_format(to_date(col("PROCESSED_DTTM")), "yyyy-MM") === conf.loadDate.apply())


  val techLoanRepaymentScheduleDF: DataFrame =
    extractTable("TECH_LOAN_REPAYMENT_SCHEDULE", techLoanRepaymentScheduleSchema, ";",
      techLoanRepaymentScheduleTable)

  val cdInternalOrgDetailDF: DataFrame =
    extractTable("CD_INTERNAL_ORG_DETAIL", cdInternalOrgDetailSchema, ";", cdInternalOrgDetailTable,
      cdInternalOrgDetailCol, col("IS_ACTIVE_FLG") === 1)

  log.warn("Загрузка источников завершена")






}
