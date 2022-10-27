ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "spark-snippets",
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "3.3.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "3.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.14" % "test"