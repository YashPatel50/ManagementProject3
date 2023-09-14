name := "ManagementProject3"

version := "0.1"

scalaVersion := "2.12.3"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.3.1"

// Enable implicit conversions to Dataset
addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)

scalacOptions += "-Xplugin-require:macroparadise"