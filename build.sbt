name := "mse-sql-example"

version := "0.1"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.0.0-preview2",
  // "com.github.fqaiser94" %% "mse" % "0.2.4",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test,
  "org.scalacheck" %% "scalacheck" % "1.14.1" % Test
)