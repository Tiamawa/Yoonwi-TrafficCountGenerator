name := "TraficCountGenerator"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++=Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.1.0",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.0",
  "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.1.0",
  "org.mongodb.scala" % "mongo-scala-driver_2.11" %"2.1.0",
  "com.typesafe" % "config" % "1.3.1",
  "commons-configuration" % "commons-configuration" % "1.10",

)