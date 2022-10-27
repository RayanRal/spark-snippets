package com.rayanral

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class Rounder(precision: Int) {

  def roundDoubleColumns(df: DataFrame): DataFrame = {
    val columns: List[StructField] = df.schema.fields.toList
    var updatedDf = df
    columns.foreach { column =>
      column.dataType match {
        case DoubleType =>
          updatedDf = updatedDf.withColumn(column.name, round(col(column.name), precision))
        case MapType(StringType, DoubleType, _) =>
          updatedDf = updatedDf.withColumn(
            column.name,
            RoundingUDFs.roundMapValuesUDF(col(column.name), lit(precision))
          )
        case ArrayType(DoubleType, _) =>
          updatedDf = updatedDf.withColumn(
            column.name,
            RoundingUDFs.roundArrayValuesUDF(col(column.name), lit(precision))
          )
        case _ =>
      }
    }
    updatedDf
  }
}

object RoundingUDFs {

  val roundMapValuesUDF: UserDefinedFunction = udf((map: Map[String, Double], precision: Int) => {
    map.mapValues(
      v => BigDecimal(v).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
    )
  })

  val roundArrayValuesUDF: UserDefinedFunction = udf((array: Seq[Double], precision: Int) => {
    array.map(
      v => BigDecimal(v).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
    )
  })

}
