ThisBuild / scalaVersion     := "2.12.15"
ThisBuild / organization     := "com.example"
ThisBuild / version          := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "spark-compact-job",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.4.2" % "provided",
      // если понадобится JDBC к PG:
      "org.postgresql"   %  "postgresql" % "42.7.4"
    ),
    Compile / mainClass := Some("com.example.SparkCompactJob"),
    assembly / assemblyMergeStrategy := {
      case "META-INF/services/org.apache.hadoop.fs.FileSystem" => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )

addCommandAlias("ci", ";clean;assembly")
