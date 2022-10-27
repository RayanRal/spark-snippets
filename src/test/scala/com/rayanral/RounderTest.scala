package com.rayanral

import org.scalatest._
import flatspec._
import org.scalatest.matchers.should.Matchers._

case class TestData(name: String, value: Double, otherValue: Double)

class RounderTest extends AnyFlatSpec with SparkTester {

  import sparkSession.implicits._

  "rounder" should "process double values" in {
    val rounder = new Rounder(2)
    val testData = Seq(
      TestData("data1", 0.051, 1.0),
      TestData("data2", 0.055, 1.0),
      TestData("data3", 0.75, 2.3),
    )
    val testDf = sparkSession.createDataFrame[TestData](testData)
    val result = rounder.roundDoubleColumns(testDf)
    val localResult = result.as[TestData].collect().toList
    localResult.find(_.name == "data1").get.value should be(0.05)
    localResult.find(_.name == "data1").get.otherValue should be(1.0)

    localResult.find(_.name == "data2").get.value should be(0.06)
    localResult.find(_.name == "data2").get.otherValue should be(1.0)

    localResult.find(_.name == "data3").get.value should be(0.75)
    localResult.find(_.name == "data3").get.otherValue should be(2.3)
  }

}
