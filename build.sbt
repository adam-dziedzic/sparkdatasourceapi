name := "SparkDataSourceAPI"

scalaVersion := "2.11.8"

version := "0.1"

scalacOptions ++= Seq("-deprecation")

// grading libraries
libraryDependencies += "junit" % "junit" % "4.10" % "test"

// for funsets
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "2.0.0"