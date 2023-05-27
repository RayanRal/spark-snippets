package com.rayanral

import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
 * Rounder allows you to quickly round all values of type Double in whole DataFrame or in selected columns
 * Useful for cases when you're joining data comping from several different sources
 * and want to get a uniform result
 *
 */
class Rounder(precision: Int) {

  def roundDoubleColumns(df: DataFrame, cols: List[String]): DataFrame = {
    val columns: List[StructField] = df.schema.fields.filter(f => cols.contains(f.name)).toList
    roundDoubleColumnStructs(df, columns)
  }

  def roundDoubleColumns(df: DataFrame): DataFrame = {
    val columns: List[StructField] = df.schema.fields.toList
    roundDoubleColumnStructs(df, columns)
  }

  def roundDoubleColumnStructs(df: DataFrame, columns: List[StructField]): DataFrame = {
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
