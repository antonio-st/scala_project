package extract

import org.apache.spark.sql._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types._
import schemas._
import Variables._



class Extract extends Function with Logging{

    Logger.getRootLogger.setLevel(Level.WARN)
    val logger: Logger = Logger.getLogger(getClass.getName)
    logger.setLevel(Level.INFO)

  log.warn("Загрузка источников")

  val cdAccountSchema = schemas.CdAccount.structType

  val cdAccountDF: DataFrame =
    extractTable("CD_ACCOUNT", cdAccountSchema, ";", "sources/cd_account.csv",
      cdAccountCol,
      col("CUSTOMER_TYPE_CD") === "Ч" && col("ACCOUNT_TYPE_CD") === "А" && col("IS_ACTIVE_FLG") === 1)

  val cdLoanAgreementSchema = schemas.CdLoanAgreement.structType

  val cdLoanAgreementDF: DataFrame =
    extractTable("CD_LOAN_AGREEMENT", cdLoanAgreementSchema, ";", "sources/cd_loan_agreement.csv",
      cdLoanAgreementCol,
      col("CUSTOMER_TYPE_CD") === "Ч" && col("CONTRACT_TYPE_CD") != "ЗАЯВКА" && col("IS_ACTIVE_FLG") === 1)

  val cdAgreementXCustomerSchema = schemas.CdAgreementXCustomer.structType

    // ошибка в ТЗ  - CUSTOMER_ROLE_CD=’ЗАЕМЩИК’ есть только Заемщик
  val cdAgreementXCustomerDF: DataFrame =
    extractTable("CD_AGREEMENT_X_CUSTOMER", cdAgreementXCustomerSchema, ";", "sources/cd_agreement_x_customer.csv",
      cdAgreementXCustomerCol,
      col("CUSTOMER_TYPE_CD") === "Ч" && lower(col("CUSTOMER_ROLE_CD")) === "заемщик" && col("IS_ACTIVE_FLG") === 1)

  val cdIndividualCustomerSchema = schemas.CdIndividualCustomer.structType

  val cdIndividualCustomerDF: DataFrame =
    extractTable("CD_INDIVIDUAL_CUSTOMER", cdIndividualCustomerSchema, ";", "sources/cd_individual_customer.csv",
      cdIndividualCustomerCol,
      col("IS_ACTIVE_FLG") === 1)

  val CdGlobalIndividualCustomerSchema = schemas.CdGlobalIndividualCustomer.structType

  val cdGlobalIndividualCustomerDF: DataFrame =
    extractTable("CD_GLOBAL_INDIVIDUAL_CUSTOMER", CdGlobalIndividualCustomerSchema, ";", "sources/cd_global_individual_customer.csv",
      CdGlobalIndividualCustomerCol,
      col("IS_ACTIVE_FLG") === 1)

  val fctLoanAccountBalanceSchema = schemas.FctLoanAccountBalance.structType

  val fctLoanAccountBalanceDF: DataFrame =
    extractTable("FCT_LOAN_ACCOUNT_BALANCE", fctLoanAccountBalanceSchema, ";", "sources/fct_loan_account_balance.csv",
      fctLoanAccountBalanceCol,
      col("DELETED_FLG") =!= 1 && col("BALANCE_AMT") > 0)

  val techLoanRepaymentScheduleSchema = schemas.TechLoanRepaymentSchedule.structType

  val techLoanRepaymentScheduleDF: DataFrame =
    extractTable("TECH_LOAN_REPAYMENT_SCHEDULE", techLoanRepaymentScheduleSchema, ";",
      "sources/tech_loan_repayment_schedule.csv"
)

  val cdInternalOrgDetailSchema = schemas.CdInternalOrgDetail.structType

  val cdInternalOrgDetailDF: DataFrame =
    extractTable("CD_INTERNAL_ORG_DETAIL", cdInternalOrgDetailSchema, ";", "sources/cd_internal_org_detail.csv",
      cdInternalOrgDetailCol,
      col("IS_ACTIVE_FLG") === 1)

  log.warn("Загрузка источников завершена")






}
