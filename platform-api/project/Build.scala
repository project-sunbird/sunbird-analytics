import sbt._
import sbt.Keys._

object MyBuild extends Build {

    val core = Project("analytics-api-core", file("analytics-api-core"))
        .settings(
            version := Pom.version(baseDirectory.value),
            libraryDependencies ++= Pom.dependencies(baseDirectory.value))

    val root = play.Project("analytics-api", path = file("analytics-api"))
        .dependsOn(core)
        .settings(
            version := Pom.version(baseDirectory.value),
            libraryDependencies ++= Pom.dependencies(baseDirectory.value).filterNot(d => d.name == core.id))

    override def rootProject = Some(root)
}