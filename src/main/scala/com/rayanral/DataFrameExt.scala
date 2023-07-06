package com.rayanral

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame}

object DataFrameExt {

  implicit class ColumnMethods(df: DataFrame) {

    // in Spark 3.1 you can use unionByName(df2, allowMissingColumns=True)
    // but in Spark 2+ you can use this implementation instead
    def safeUnion(otherDf: DataFrame): DataFrame = {
      val dfCols = df.columns.toSet
      val otherCols = otherDf.columns.toSet
      val allCols = dfCols ++ otherCols

      def expr(currentCols: Set[String], allCols: Set[String]): List[Column] = {
        allCols.toList.map {
          case x if currentCols.contains(x) => col(x)
          case x => lit(null).as(x)
        }
      }

      df
        .select(expr(dfCols, allCols): _*)
        .union(otherDf.select(expr(otherCols, allCols): _*))
    }


  }

}
