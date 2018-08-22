javacOptions ++= Seq(
    "-source",
    "1.8",
    "-target",
    "1.8",
    "-Xlint"
)

scalacOptions ++= Seq(
    "-Xmax-classfile-name", "242",
    "-feature",
    "-Ywarn-unused",
    "-Ywarn-unused-import"
)

val beamVersion = "2.6.0"

lazy val root = (project in file(".")).
  settings(
    name := "landfill",
    version := "0.1",
    scalaVersion := "2.12.6",
    retrieveManaged := true,
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5",
    libraryDependencies += "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
    libraryDependencies += "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
    libraryDependencies += "org.apache.beam" % "beam-sdks-java-extensions-json-jackson" % beamVersion,
    libraryDependencies += "org.apache.beam" % "beam-sdks-java-io-google-cloud-platform" % beamVersion
  )

assemblyMergeStrategy in assembly := {
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case PathList("META-INF", "native", xs@_*) => MergeStrategy.first // io.netty
  case PathList("META-INF", "services", xs@_*) => MergeStrategy.filterDistinctLines // IOChannelFactory
  case PathList("META-INF", xs@_*) => MergeStrategy.discard // google's repacks
  case _ => MergeStrategy.first
}

test in assembly := {}

testOptions in Test := Seq(
  // -oD add duration reporting; see http://www.scalatest.org/user_guide/using_scalatest_with_sbt
  Tests.Argument("-oD")
)

val scalaStyleConfigUrl = Some(url("https://raw.githubusercontent.com/mozilla/moztelemetry/master/scalastyle-config.xml"))
(scalastyleConfigUrl in Compile) := scalaStyleConfigUrl
(scalastyleConfigUrl in Test) := scalaStyleConfigUrl
