name := "piline-hist"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.1"
val configVersion = "1.3.2"
val logbackVersion = "1.2.3"
val scalatestVersion = "3.0.5"
val sttpVersion = "1.6.3"
val json4sVersion = "3.6.0"
val sparkTestingBaseVersion = s"${sparkVersion}_0.12.0"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % sparkVersion ,
  "org.apache.spark" %% "spark-sql" % sparkVersion ,
  "org.apache.spark" %% "spark-hive" % sparkVersion)