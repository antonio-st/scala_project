package schemas

import org.apache.spark.sql.types._

object CdLoanAgreement extends Enumeration {

  private val DELIMITER = "\\t"

  val AGREEMENT_RK, CONTRACT_NUM, OPEN_DT, LOAN_AMT,
  CREDIT_LIMIT_AMT, CURRENCY_ISO_CD, CONTRACT_TYPE_CD,
  INTERNAL_ORG_ID, INIT_INTERNAL_ORG_ID, PRODUCT_OPERATIONAL_RK,
  CUSTOMER_RK, CUSTOMER_TYPE_CD,
  IS_ACTIVE_FLG = Value

  val structType = StructType(
    Seq(
      StructField(AGREEMENT_RK.toString, DecimalType(18, 0)),
      StructField(CONTRACT_NUM.toString, StringType),
      StructField(OPEN_DT.toString, StringType),
      StructField(LOAN_AMT.toString, DecimalType(23, 5)),
      StructField(CREDIT_LIMIT_AMT.toString, DecimalType(23, 5)),
      StructField(CURRENCY_ISO_CD.toString, StringType),
      StructField(CONTRACT_TYPE_CD.toString, StringType),
      StructField(INTERNAL_ORG_ID.toString, StringType),
      StructField(INIT_INTERNAL_ORG_ID.toString, StringType),
      StructField(PRODUCT_OPERATIONAL_RK.toString, IntegerType),
      StructField(CUSTOMER_RK.toString, IntegerType),
      StructField(CUSTOMER_TYPE_CD.toString, StringType),
      StructField(IS_ACTIVE_FLG.toString, StringType)
    )
  )
}
