 version := "0.1.0-SNAPSHOT"

 scalaVersion := "2.12.14"

lazy val root = (project in file("."))
  .settings(
    name := "Tesco_311220121"
  )
 //libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided"
 //libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2" % "provided"
 libraryDependencies ++= Seq(
   "org.apache.spark" %% "spark-core" % "3.1.2",
   "org.apache.spark" %% "spark-sql" % "3.1.2",
   "org.apache.spark" %% "spark-streaming" % "3.1.2",
 )