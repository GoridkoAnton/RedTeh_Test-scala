ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15" // Заданная версия Scala

lazy val root = (project in file("."))
  .settings(
    name := "spark-compact-job",
    libraryDependencies ++= Seq(
      // Spark в режиме "local"
      "org.apache.spark" %% "spark-core" % "3.3.0" % "provided", // Используем 'provided' т.к. будем запускать в образе Spark
      "org.apache.spark" %% "spark-sql" % "3.3.0" % "provided",

      // JDBC драйвер для PostgreSQL (Пункт 4.c)
      "org.postgresql" % "postgresql" % "42.5.0"
    )
  )