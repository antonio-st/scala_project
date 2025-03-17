package t1.datamarts.transform

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Variables {

  val sr11Sr13Cols = Seq(
    "flab.AGREEMENT_RK",
    "flab.ACCOUNT_RK",
    "flab.ACCOUNT_NUM",
    "flab.BALANCE_AMT",
    "flab.EFFECTIVE_FROM_DTTM",
    "flab.EFFECTIVE_TO_DTTM",
    "flab.IS_ACTIVE_FLG",
    "flab.PROCESSED_DTTM",
    "la.CONTRACT_NUM",
    "la.OPEN_DT",
    "la.LOAN_AMT",
    "la.CREDIT_LIMIT_AMT",
    "la.CURRENCY_ISO_CD",
    "la.INTERNAL_ORG_ID",
    "la.INIT_INTERNAL_ORG_ID",
    "la.PRODUCT_OPERATIONAL_RK",
    "la.CUSTOMER_RK"
  ).map(col)

  val sr15Sr17Cols: Seq[Column] = Seq(
    "sr13.AGREEMENT_RK",
    "sr13.ACCOUNT_RK",
    "sr13.ACCOUNT_NUM",
    "sr13.BALANCE_AMT",
    "sr13.EFFECTIVE_FROM_DTTM",
    "sr13.EFFECTIVE_TO_DTTM",
    "sr13.IS_ACTIVE_FLG",
    "sr13.PROCESSED_DTTM",
    "sr13.CONTRACT_NUM",
    "sr13.OPEN_DT",
    "sr13.LOAN_AMT",
    "sr13.CREDIT_LIMIT_AMT",
    "sr13.CURRENCY_ISO_CD",
    "sr13.INTERNAL_ORG_ID",
    "sr13.INIT_INTERNAL_ORG_ID",
    "sr13.PRODUCT_OPERATIONAL_RK",
    "sr13.CUSTOMER_RK",
    "iod.BRANCH_ID",
    "iod.BRANCH_NM",
    "iod.REGIONAL_OPER_OFFICE_ID",
    "iod.REGIONAL_OPER_OFFICE_NM",
    "iod.INTERNAL_ORG_NM",
    "iod.ADDRESS"
  ).map(col)

  val sr18Sr20Cols: Seq[Column] = Seq(
    "sr17.AGREEMENT_RK",
    "sr17.ACCOUNT_RK",
    "sr17.ACCOUNT_NUM",
    "sr17.BALANCE_AMT",
    "sr17.EFFECTIVE_FROM_DTTM",
    "sr17.EFFECTIVE_TO_DTTM",
    "sr17.IS_ACTIVE_FLG",
    "sr17.PROCESSED_DTTM",
    "sr17.CONTRACT_NUM",
    "sr17.OPEN_DT",
    "sr17.LOAN_AMT",
    "sr17.CREDIT_LIMIT_AMT",
    "sr17.CURRENCY_ISO_CD",
    "sr17.INTERNAL_ORG_ID",
    "sr17.INIT_INTERNAL_ORG_ID",
    "sr17.PRODUCT_OPERATIONAL_RK",
    "sr17.CUSTOMER_RK",
    "sr17.BRANCH_ID",
    "sr17.BRANCH_NM",
    "sr17.REGIONAL_OPER_OFFICE_ID",
    "sr17.REGIONAL_OPER_OFFICE_NM",
    "sr17.INTERNAL_ORG_NM",
    "sr17.ADDRESS",
    "sr19.NEXT_PAYMENT_FROM_DT",
    "sr19.NEXT_MONTHLY_PAYMENT_AMT"
  ).map(col)

}
