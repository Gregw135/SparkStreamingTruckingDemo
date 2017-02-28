
name := "SparkStreaming"

version := "2.0.0"

scalaVersion := "2.10.4"

libraryDependencies ++= {
  val sparkVer = "1.6.1"
  Seq(
    "org.apache.spark"     %% "spark-core"              % sparkVer withSources(),
    "org.apache.spark"     %% "spark-mllib"             % sparkVer withSources(),
    "org.apache.spark"     %% "spark-sql"               % sparkVer withSources(),
    "org.apache.spark"     %% "spark-streaming"         % sparkVer withSources(),
    "org.apache.spark"     %% "spark-streaming-kafka" % sparkVer withSources(),
    "org.apache.kafka"     %% "kafka" % "0.8.0"  withSources(),
    "com.typesafe" % "config" % "1.3.1"
  )
}

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*)      => MergeStrategy.first
  case PathList("javax", "xml", xs @ _*)      => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs @ _*)      => MergeStrategy.first
  case PathList("com", "google", xs @ _*)      => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

//
//
//