
name := "Tumor"

version := "1.0"

scalaVersion := "2.10.5"

resolvers += "cloudera-repo-releases" at "https://repository.cloudera.com/artifactory/repo/"
resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"


libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.4.0" % "provided"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.5" % "provided"
libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.4.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.4.0" % "provided"
libraryDependencies += "org.apache.commons" % "commons-math3" % "3.4"
