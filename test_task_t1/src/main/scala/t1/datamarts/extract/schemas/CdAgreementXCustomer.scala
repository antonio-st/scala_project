package schemas

import org.apache.spark.sql.types._

object CdAgreementXCustomer extends Enumeration {

  private val DELIMITER = "\\t"

  val AGREEMENT_RK, CUSTOMER_RK, CUSTOMER_TYPE_CD,
  CUSTOMER_ROLE_CD, IS_ACTIVE_FLG = Value

  val structType = StructType(
    Seq(
      StructField(AGREEMENT_RK.toString, DecimalType(18, 0)),
      StructField(CUSTOMER_RK.toString, IntegerType),
      StructField(CUSTOMER_TYPE_CD.toString, StringType),
      StructField(CUSTOMER_ROLE_CD.toString, StringType),
      StructField(IS_ACTIVE_FLG.toString, StringType)
    )
  )
}
