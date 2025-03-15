package extract

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Variables {

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
