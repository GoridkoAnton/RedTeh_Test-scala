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
    assembly / assemblyMergeStrategy := {
      case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
enablePlugins(com.eed3si9n.scalaform.ScalaFormPlugin)
