ThisBuild / name := "dm-credit-akbogdanov"
ThisBuild / version := "1.1.0"
ThisBuild / scalaVersion := "2.13.8"
val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "compile",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "compile",
  "com.typesafe" % "config" % "1.4.2",
  "org.rogach" %% "scallop" % "4.1.0"
)
assembly / assemblyMergeStrategy := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
