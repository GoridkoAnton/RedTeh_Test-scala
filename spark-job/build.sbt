import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.{MergeStrategy, PathList}

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "spark-compact-job",
    version := "0.1.0",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.4.2" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.4.2" % "provided",
      "org.postgresql"    % "postgresql" % "42.7.4"
    ),

    // не запускаем тесты при сборке fat-jar (если их нет — тоже ок)
    Test / parallelExecution := false,
    assembly / test := {},

    // merge-стратегии для jar
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
        MergeStrategy.concat
      case PathList("META-INF", xs @ _*) =>
        MergeStrategy.discard
      case "reference.conf" =>
        MergeStrategy.concat
      case _ =>
        MergeStrategy.first
    }
  )
