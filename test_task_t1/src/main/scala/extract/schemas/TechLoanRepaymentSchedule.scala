package schemas

import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

object TechLoanRepaymentSchedule extends Enumeration {

  private val DELIMITER = "\\t"

  val AGREEMENT_RK, NEXT_PAYMENT_FROM_DT, NEXT_MONTHLY_PAYMENT_AMT = Value

  val structType = StructType(
    Seq(
      StructField(AGREEMENT_RK.toString, DecimalType(18, 0)),
      StructField(NEXT_PAYMENT_FROM_DT.toString, StringType),
      StructField(NEXT_MONTHLY_PAYMENT_AMT.toString, DecimalType(23, 5))
    )
  )
}
