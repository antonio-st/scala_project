package schemas

import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

object CdAccount extends Enumeration {

  private val DELIMITER = "\\t"

  val ACCOUNT_RK, ACCOUNT_NUM, ACCOUNT_TYPE_CD,
  CUSTOMER_TYPE_CD, IS_ACTIVE_FLG = Value

  val structType = StructType(
    Seq(
      StructField(ACCOUNT_RK.toString, DecimalType(18, 0)),
      StructField(ACCOUNT_NUM.toString, StringType),
      StructField(ACCOUNT_TYPE_CD.toString, StringType),
      StructField(CUSTOMER_TYPE_CD.toString, StringType),
      StructField(IS_ACTIVE_FLG.toString, StringType)
    )
  )
}
