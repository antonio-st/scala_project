package schemas

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CdGlobalIndividualCustomer extends Enumeration {

  private val DELIMITER = "\\t"

  val CUSTOMER_MDM_RK, CUSTOMER_MDM_ID, IS_ACTIVE_FLG = Value

  val structType = StructType(
    Seq(
      StructField(CUSTOMER_MDM_RK.toString, IntegerType),
      StructField(CUSTOMER_MDM_ID.toString, StringType),
      StructField(IS_ACTIVE_FLG.toString, StringType)
    )
  )
}
