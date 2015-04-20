name := "spark_codebase"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  "typesafe-repository" at "http://repo.typesafe.com/typesafe/releases"
)

val sparkVersion = "1.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
    exclude("org.apache.zookeeper", "zookeeper"),
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" %sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion
    exclude("org.apache.zookeeper", "zookeeper"),
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,
  "org.apache.kafka" %% "kafka" % "0.8.2.1"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
    exclude("org.slf4j", "slf4j-simple")
    exclude("log4j", "log4j")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("com.101tec", "zkclient"),
  "org.apache.curator" % "curator-test" % "2.4.0",
  "com.101tec" % "zkclient" % "0.4"
    exclude("org.apache.zookeeper", "zookeeper"),
  "joda-time" % "joda-time" % "2.7",
  "com.maxmind.geoip2" % "geoip2" % "2.1.0",
  // Test dependencies
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.xerial.snappy" % "snappy-java" % "1.1.1.7"
)

// Cannot run tests in parallel because of:
// `akka.actor.InvalidActorNameException: actor name [LocalBackendActor] is not unique!`
parallelExecution in Test := false

// Forking is required as testing requires multiple threads
fork in Test := true

// Skip running tests during assembly
test in assembly := {}