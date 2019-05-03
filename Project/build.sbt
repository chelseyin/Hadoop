name := "BookScanner"
 
scalaVersion := "2.10.5"
 
// Resolvers
resolvers += "Apache HBase"  at "https://repository.apache.org/content/repositories/releases"
resolvers += "Thrift"        at "http://people.apache.org/~rawson/repo/"
resolvers += "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += "Cloudera Repo" at "https://repository.cloudera.com/content/repositories/releases"
 
// Dependencies
 
// ------------- Spark -----------
libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "1.6.0" % "provided"
)
 
// ----------- Hadoop -------------------
libraryDependencies ++= Seq (
  "org.apache.hadoop" % "hadoop-core" % "1.2.0" % "provided"
)
 
// Nerdammer Spark-HBase Connector
 
libraryDependencies ++= Seq (
    "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3"
)
 
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
 
assemblyMergeStrategy in assembly := {
  case x if x.matches("META-INF/services/.*") => MergeStrategy.filterDistinctLines
  case x if x.matches(".*(xml|dtd|xsd|xml|html|properties|class|thrift)$") => MergeStrategy.first
  case "reference.conf" => MergeStrategy.concat
  case old =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(old)
}
 
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter {_.data.getName != "spark-hbase-connector_2.10-1.0.3.jar"}
}