package com.rayanral

import org.scalatest._
import flatspec._
import org.scalatest.matchers.should.Matchers._


case class FilterTestData(name: String, val1: Double, val2: Double, val3: Int, val4: String)

class FiltersTest extends AnyFlatSpec with SparkTester {

  import sparkSession.implicits._

  val testData: Seq[FilterTestData] = Seq(
    FilterTestData("data1", 0.051, 1.0, 5, "s"),
    FilterTestData("data2", 0.0, 2.1, 0, "f"),
    FilterTestData("data3", 0.75, 4.2, 7, "q"),
  )

  "filter" should "remove row where columns are empty" in {
    val testDf = sparkSession.createDataFrame[FilterTestData](testData)
    val result = Filters.filterDfByEmptyCols(testDf, columnNames = List("val1", "val3"))
      .as[FilterTestData]
      .collect()
      .toList
    result.size should be(2)
    result.map(_.name) should contain("data1").and(contain("data3"))
  }

  "filter" should "not fail if col list is empty" in {
    val testDf = sparkSession.createDataFrame[FilterTestData](testData)
    val result = Filters
      .filterDfByEmptyCols(testDf, columnNames = List.empty)
      .as[FilterTestData]
      .collect()
      .toList
    result.size should be(3)
  }

  "filter" should "not fail if incorrect column names were passed" in {
    val testDf = sparkSession.createDataFrame[FilterTestData](testData)
    val result = Filters
      .filterDfByEmptyCols(testDf, columnNames = List("non_existent_col"))
      .as[FilterTestData]
      .collect()
      .toList
    result.size should be(3)
  }

}
