package schemas

import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

object FctLoanAccountBalance extends Enumeration {

  private val DELIMITER = "\\t"

  val AGREEMENT_RK, ACCOUNT_RK, ACCOUNT_NUM, BALANCE_AMT,
  EFFECTIVE_FROM_DTTM, EFFECTIVE_TO_DTTM, DELETED_FLG,
  IS_ACTIVE_FLG, PROCESSED_DTTM = Value

  val structType = StructType(
    Seq(
      StructField(AGREEMENT_RK.toString, DecimalType(18, 0)),
      StructField(ACCOUNT_RK.toString, DecimalType(18, 0)),
      StructField(ACCOUNT_NUM.toString, StringType),
      StructField(BALANCE_AMT.toString, DecimalType(23, 5)),
      StructField(EFFECTIVE_FROM_DTTM.toString, StringType),
      StructField(EFFECTIVE_TO_DTTM.toString, StringType),
      StructField(DELETED_FLG.toString, StringType),
      StructField(IS_ACTIVE_FLG.toString, StringType),
      StructField(PROCESSED_DTTM.toString, StringType)
    )
  )
}
