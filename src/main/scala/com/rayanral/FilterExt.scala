package com.rayanral

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit}

/**
 * Utility methods allowing to easily filter invalid data from DataFrame
 */
object FilterExt {

  implicit class DfFilters(df: DataFrame) {

    /**
     * removing rows where any of listed columns are empty (0 or null)
     */
    def filterIfAnyColsEmpty(columnNames: List[String]): DataFrame = {
      val existingCols = columnNames.filter(df.columns.contains)
      val colFilter = filterByColumns(existingCols, (c1: Column, c2: Column) => c1.or(c2))
      df.filter(!colFilter)
    }

    /**
     * removing rows where all listed columns are empty (0 or null)
     */
    def filterIfAllColsEmpty(columnNames: List[String]): DataFrame = {
      val existingCols = columnNames.filter(df.columns.contains)
      val colFilter = filterByColumns(existingCols, (c1: Column, c2: Column) => c1.and(c2))
      df.filter(!colFilter)
    }

    private def filterByColumns(columnNames: List[String], joinOperation: (Column, Column) => Column): Column = {
      if (columnNames.isEmpty) {
        lit(false)
      } else {
        columnNames
          .map(colName => col(colName).isNull.or(col(colName) === 0))
          .reduce(joinOperation)
      }
    }
  }






}
