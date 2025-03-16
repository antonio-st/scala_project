package t1.datamarts.extract

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import schemas._

object Variables {

  val resultTable: String = "table/stg/credit"
  val resultTableCsv: String = "table/stg/csv"

  // схемы данных
  val cdAccountSchema = schemas.CdAccount.structType
  val cdLoanAgreementSchema = schemas.CdLoanAgreement.structType
  val cdAgreementXCustomerSchema = schemas.CdAgreementXCustomer.structType
  val cdIndividualCustomerSchema = schemas.CdIndividualCustomer.structType
  val CdGlobalIndividualCustomerSchema = schemas.CdGlobalIndividualCustomer.structType
  val fctLoanAccountBalanceSchema = schemas.FctLoanAccountBalance.structType
  val techLoanRepaymentScheduleSchema = schemas.TechLoanRepaymentSchedule.structType
  val cdInternalOrgDetailSchema = schemas.CdInternalOrgDetail.structType

    // путь к источникам данных
  val cdAccountTable: String = "sources/cd_account.csv"
  val cdLoanAgreementTable: String = "sources/cd_loan_agreement.csv"
  val cdAgreementXCustomerTable: String = "sources/cd_agreement_x_customer.csv"
  val cdIndividualCustomerTable: String = "sources/cd_individual_customer.csv"
  val cdGlobalIndividualCustomerTable: String = "sources/cd_global_individual_customer.csv"
  val fctLoanAccountBalanceTable: String = "sources/fct_loan_account_balance.csv"
  val cdInternalOrgDetailTable: String = "sources/cd_internal_org_detail.csv"
  val techLoanRepaymentScheduleTable: String = "sources/tech_loan_repayment_schedule.csv"

    // аттрибуты df
  val cdAccountCol: Seq[Column] = Seq(
    "ACCOUNT_RK",
    "ACCOUNT_NUM"
  ).map(col)

  val cdLoanAgreementCol: Seq[Column] = Seq(
    "AGREEMENT_RK",
    "CONTRACT_NUM",
    "OPEN_DT",
    "LOAN_AMT",
    "CREDIT_LIMIT_AMT",
    "CURRENCY_ISO_CD",
    "INTERNAL_ORG_ID",
    "INIT_INTERNAL_ORG_ID",
    "PRODUCT_OPERATIONAL_RK",
    "CUSTOMER_RK"
  ).map(col)

  val cdAgreementXCustomerCol: Seq[Column] = Seq(
    "AGREEMENT_RK",
    "CUSTOMER_RK"
  ).map(col)

  val cdIndividualCustomerCol: Seq[Column] = Seq(
    "CUSTOMER_RK",
    "CUSTOMER_GLOBAL_RK"
  ).map(col)

  val CdGlobalIndividualCustomerCol: Seq[Column] = Seq(
    "CUSTOMER_MDM_RK",
    "CUSTOMER_MDM_ID"
  ).map(col)

  val fctLoanAccountBalanceCol: Seq[Column] = Seq(
    "AGREEMENT_RK",
    "ACCOUNT_RK",
    "ACCOUNT_NUM",
    "BALANCE_AMT",
    "EFFECTIVE_FROM_DTTM",
    "EFFECTIVE_TO_DTTM",
    "IS_ACTIVE_FLG",
    "PROCESSED_DTTM"
  ).map(col)

  val cdInternalOrgDetailCol: Seq[Column] = Seq(
    "BRANCH_ID",
    "BRANCH_NM",
    "REGIONAL_OPER_OFFICE_ID",
    "REGIONAL_OPER_OFFICE_NM",
    "INTERNAL_ORG_ID",
    "INTERNAL_ORG_NM",
    "ADDRESS"
  ).map(col)

}
