package schemas

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CdInternalOrgDetail extends Enumeration {

  private val DELIMITER = "\\t"

  val BRANCH_ID, BRANCH_NM, REGIONAL_OPER_OFFICE_ID,
  REGIONAL_OPER_OFFICE_NM, INTERNAL_ORG_ID, INTERNAL_ORG_NM,
  ADDRESS, IS_ACTIVE_FLG = Value

  val structType = StructType(
    Seq(
      StructField(BRANCH_ID.toString, StringType),
      StructField(BRANCH_NM.toString, StringType),
      StructField(REGIONAL_OPER_OFFICE_ID.toString, StringType),
      StructField(REGIONAL_OPER_OFFICE_NM.toString, StringType),
      StructField(INTERNAL_ORG_ID.toString, StringType),
      StructField(INTERNAL_ORG_NM.toString, StringType),
      StructField(ADDRESS.toString, StringType),
      StructField(IS_ACTIVE_FLG.toString, StringType)
    )
  )
}
