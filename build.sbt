import sbt.Keys._

name := "spark_codebase"

version := "1.0"

scalaVersion := "2.10.5"

resolvers ++= Seq(
  "Typesafe repository snapshots"   at "http://repo.typesafe.com/typesafe/snapshots/",
  "Typesafe repository releases"    at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype repo"                   at "https://oss.sonatype.org/content/groups/scala-tools/",
  "Sonatype releases"               at "https://oss.sonatype.org/content/repositories/releases",
  "Sonatype snapshots"              at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype staging"                at "http://oss.sonatype.org/content/repositories/staging",
  "Java.net Maven2 Repository"      at "http://download.java.net/maven/2/",
  "Twitter Repository"              at "http://maven.twttr.com",
  "Websudos releases"               at "http://maven.websudos.co.uk/ext-release-local"
)

val sparkVersion = "1.2.1"
val PhantomVersion = "1.6.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
    exclude("org.apache.zookeeper", "zookeeper"),
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" %sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion
    exclude("org.apache.zookeeper", "zookeeper"),
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion,
  "org.slf4j" % "slf4j-api" % "1.7.12",
  "org.apache.kafka" %% "kafka" % "0.8.2.1"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
    exclude("log4j", "log4j")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("com.101tec", "zkclient")
    excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.curator" % "curator-test" % "2.4.0"
    excludeAll ExclusionRule(organization = "io.netty")
    excludeAll ExclusionRule(organization = "org.jboss.netty")
    exclude("com.google.guava", "guava"),
  "com.101tec" % "zkclient" % "0.4"
    exclude("org.apache.zookeeper", "zookeeper"),
  "joda-time" % "joda-time" % "2.7",
  "com.maxmind.geoip2" % "geoip2" % "2.1.0",
  "com.websudos" %% "phantom-dsl" % PhantomVersion
    exclude("com.google.guava", "guava"),
  "com.websudos" %% "phantom-zookeeper" % PhantomVersion
    exclude("com.google.guava", "guava")
    excludeAll ExclusionRule(organization = "io.netty")
    excludeAll ExclusionRule(organization = "org.jboss.netty")
    excludeAll ExclusionRule(organization = "org.slf4j"),
  "com.typesafe" % "config" % "1.2.1",
  "com.google.guava" % "guava" % "16.0.1",
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

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)                => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html"        => MergeStrategy.first
  case "application.conf"                                   => MergeStrategy.concat
  case "com/twitter/common/args/apt/cmdline.arg.info.txt.1" => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}