import com.typesafe.sbt.SbtMultiJvm
import com.typesafe.sbt.SbtMultiJvm.MultiJvmKeys.MultiJvm

val akkaVersion = "2.5.31"

val project = Project(
  id = "hailstorm",
  base = file(".")
)
  .settings(SbtMultiJvm.multiJvmSettings: _*)
  .settings(
    name := """hailstorm""",
    version := "1.0.0",
    scalaVersion := "2.12.11",
    scalacOptions in Compile ++= Seq("-encoding", "UTF-8", "-target:jvm-1.8", "-deprecation", "-feature", "-unchecked", "-Xlog-reflective-calls", "-Xlint"),
    javacOptions in Compile ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint:unchecked", "-Xlint:deprecation"),
    resolvers += Resolver.jcenterRepo,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-agent" % akkaVersion,
      "com.typesafe.akka" %% "akka-remote" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
      "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "uk.org.lidalia" % "sysout-over-slf4j" % "1.0.2",
      "org.xerial" % "sqlite-jdbc" % "3.30.1",
      "org.scalatest" %% "scalatest" % "3.2.0" % "test",
      "io.kamon" % "sigar-loader" % "1.6.6-rev002",
      "com.github.pathikrit" %% "better-files-akka" % "3.5.0",
      "io.jvm.uuid" %% "scala-uuid" % "0.2.4",
      "org.rogach" %% "scallop" % "3.1.5",
      "net.smacke" % "jaydio" % "0.1",
      "com.github.serceman" % "jnr-fuse" % "0.5.4"),
    javaOptions in run ++= Seq(
      "-Xms8g", "-Xmx32g", "-XX:+UseG1GC", "-Daeron.term.buffer.length=2097152"),
    Keys.fork in run := true,
    mainClass in(Compile, run) := Some("ch.epfl.labos.hailstorm.HailstormFS")
  )
