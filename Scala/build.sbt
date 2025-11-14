name := "compact-parquet-job"

version := "0.1"

scalaVersion := "2.12.15"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.2" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.4.2" % "provided",
  "org.postgresql" % "postgresql" % "42.6.0"
)

// Явно задать имя итогового assembly-jar 
ThisBuild / assembly / assemblyJarName := "compact-parquet-job-assembly-0.1.jar"