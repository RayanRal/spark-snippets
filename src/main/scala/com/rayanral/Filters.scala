package com.rayanral

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}

object Filters {


  def filterDfByEmptyCols(df: DataFrame, columnNames: List[String]): DataFrame = {
    // remove columns if they are not present in the dataframe
    val existingCols = columnNames.filter(df.columns.contains)
    val colFilter = filterByColumns(existingCols)
    df.filter(!colFilter)
  }

  private def filterByColumns(columnNames: List[String]): Column = {
    if(columnNames.isEmpty) {
      lit(false)
    } else {
      columnNames
        .map(colName => col(colName).isNull.or(col(colName) === 0))
        .reduce(_.and(_))
    }
  }




}
