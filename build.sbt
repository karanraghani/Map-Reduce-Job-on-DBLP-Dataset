name := "HomeWork2"

version := "1.0"

scalaVersion := "2.12.8"


// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.2.1"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.2.1"
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.28"
// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
// https://mvnrepository.com/artifact/org.scala-lang.modules/scala-xml
libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.2.0"

enablePlugins(AssemblyPlugin)

//mainClass := Some("JobRunner")

// https://mvnrepository.com/artifact/org.scalatest/scalatest-funsuite
//libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.0-M1"


// https://mvnrepository.com/artifact/org.scalatest/scalatest
//libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test

//libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
//libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test
// https://mvnrepository.com/artifact/org.scalactic/scalactic
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.8"



