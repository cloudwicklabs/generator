import sbt._
import Keys._
import sbtassembly.{MergeStrategy, PathList, AssemblyKeys}
import AssemblyKeys._

object Build extends Build {
  // Can't upgrade to 2.11.x yet as kafka still depends on 2.10.x
  val ScalaVersion = "2.10.4"

  lazy val commonSettings = Seq(
    version := "0.5",
    scalaVersion := ScalaVersion,
    organization := "com.cloudwick",
    scalacOptions ++= Seq("-unchecked", "-deprecation")
  )

  lazy val assemblySettings = Seq(
    test in assembly := {},
    assemblyMergeStrategy in assembly := {
      case PathList("javax", "servlet", xs @ _*)                => MergeStrategy.first
      case PathList(ps @ _*) if ps.last endsWith ".html"        => MergeStrategy.first
      case "application.conf"                                   => MergeStrategy.concat
      case ".gitkeep"                                           => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    }
  )

  lazy val root = Project("generator", file("."), settings = commonSettings ++ assemblySettings) settings(
    libraryDependencies ++= Dependencies.compile,
    libraryDependencies ++= Dependencies.testDependencies,
    resolvers ++= Dependencies.resolvers
  )

  object Dependencies {
    val compile = Seq(
      "ch.qos.logback" % "logback-classic" % "1.1.2",
      "com.github.scopt" %% "scopt" % "3.3.0",
      "org.apache.avro" % "avro" % "1.7.6",
      "commons-lang" % "commons-lang" % "2.6",
      "org.apache.commons" % "commons-math" % "2.2",
      "org.apache.commons" % "commons-io" % "1.3.2",
      "org.apache.kafka" %% "kafka" % "0.8.0"
        exclude("javax.jms", "jms")
        exclude("com.sun.jdmk", "jmxtools")
        exclude("com.sun.jmx", "jmxri")
        excludeAll ExclusionRule(organization = "org.slf4j"),
      "org.apache.zookeeper" % "zookeeper" % "3.4.6"
        exclude("javax.jms", "jms")
        exclude("com.sun.jdmk", "jmxtools")
        exclude("com.sun.jmx", "jmxri")
        excludeAll ExclusionRule(organization = "org.slf4j"),
      "io.github.cloudify" %% "scalazon" % "0.11",
      "com.amazonaws" % "amazon-kinesis-client" % "1.0.0",
      "com.gensler" %% "scalavro" % "0.6.2"
    )

    val testDependencies = Seq(
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % "test"
    )

    val resolvers = Seq(
      "amateras-repo" at "http://amateras.sourceforge.jp/mvn/",
      // For scalazon
      "BintrayJCenter" at "http://jcenter.bintray.com"
    )
  }
}
