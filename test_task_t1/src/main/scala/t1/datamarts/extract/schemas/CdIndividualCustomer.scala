package schemas

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CdIndividualCustomer extends Enumeration {

  private val DELIMITER = "\\t"

  val CUSTOMER_RK, CUSTOMER_GLOBAL_RK, IS_ACTIVE_FLG = Value

  val structType = StructType(
    Seq(
      StructField(CUSTOMER_RK.toString, IntegerType),
      StructField(CUSTOMER_GLOBAL_RK.toString, IntegerType),
      StructField(IS_ACTIVE_FLG.toString, StringType)
    )
  )


}
