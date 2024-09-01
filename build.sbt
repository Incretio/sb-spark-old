ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.4.3"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)

libraryDependencies ++= Seq(
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.4.1",
  "joda-time" % "joda-time" % "2.10.10",
  "org.elasticsearch" %% "elasticsearch-spark-30" % "7.12.0",
  "commons-httpclient" % "commons-httpclient" % "3.1",
  "org.postgresql" % "postgresql" % "42.3.3"
)

lazy val root = (project in file("."))
  .settings(
    name := "data_mart"
  )