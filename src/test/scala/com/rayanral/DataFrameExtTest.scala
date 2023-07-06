package com.rayanral

import com.rayanral.DataFrameExt._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._

import java.util

class DataFrameExtTest extends AnyFlatSpec with SparkTester {

  "safeUnion" should "join dataframes with different columns" in {
    val df1Schema = StructType(
      Seq(
        StructField("col1", StringType),
        StructField("col2", DoubleType),
        StructField("col3", LongType)
      )
    )
    val df2Schema = StructType(
      Seq(
        StructField("col4", StringType),
        StructField("col5", DoubleType),
        StructField("col6", LongType)
      )
    )

    val testDf1 = sparkSession.createDataFrame(
      util.Arrays.asList(
        Row("data1", 0.051, 1)
      ),
      df1Schema
    )
    val testDf2 = sparkSession.createDataFrame(
      util.Arrays.asList(
        Row("data5", 0.042, 27)
      ),
      df2Schema
    )
    val unionDf = testDf1.safeUnion(testDf2)
    unionDf.columns.length shouldBe 6
    unionDf.columns should contain allOf ("col1", "col5")
  }

  "safeUnion" should "join dataframes with same columns" in {
    val dfSchema = StructType(
      Seq(
        StructField("col1", StringType),
        StructField("col2", DoubleType),
        StructField("col3", LongType)
      )
    )

    val testDf1 = sparkSession.createDataFrame(
      util.Arrays.asList(
        Row("data1", 0.051, 1)
      ),
      dfSchema
    )
    val testDf2 = sparkSession.createDataFrame(
      util.Arrays.asList(
        Row("data5", 0.042, 27)
      ),
      dfSchema
    )
    val unionDf = testDf1.safeUnion(testDf2)
    unionDf.schema.fields should contain theSameElementsAs dfSchema.fields
  }

  "safeUnion" should "join dataframes with overlapping columns" in {
    val col1Field = StructField("col1", StringType)
    val col3Field = StructField("col3", LongType)
    val df1Schema = StructType(
      Seq(
        col1Field,
        StructField("df1col", DoubleType),
        col3Field
      )
    )
    val df2Schema = StructType(
      Seq(
        col3Field,
        col1Field,
        StructField("df2col", DoubleType)
      )
    )

    val testDf1 = sparkSession.createDataFrame(
      util.Arrays.asList(
        Row("data1", 0.051, 1)
      ),
      df1Schema
    )
    val testDf2 = sparkSession.createDataFrame(
      util.Arrays.asList(
        Row(27, "data5", 0.042)
      ),
      df2Schema
    )
    val unionDf = testDf1.safeUnion(testDf2)
    unionDf.columns.length shouldBe 4
    unionDf.columns should contain allOf ("col1", "col3", "df1col", "df2col")
  }

}
