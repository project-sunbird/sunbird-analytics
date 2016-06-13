// Comment to get more information during initialization
logLevel := Level.Warn
scalaVersion := "2.10.6"
// The Typesafe repository

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Typesafe repository plugin" at "https://repo.scala-sbt.org/scalasbt/sbt-plugin-releases/",
  "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
)

// Use the Play sbt plugin for Play projects
addSbtPlugin("com.typesafe.play" % "sbt-plugin" % "2.4.4")
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")
