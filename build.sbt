import sbtrelease.ReleaseStateTransformations._
import sbtrelease.Version.Bump

name := "aecor-postgres-journal"

organization := "io.aecor"

val Scala212 = "2.12.15"
val Scala213 = "2.13.6"

ThisBuild / scalaVersion := Scala213
ThisBuild / crossScalaVersions := Seq(Scala212, Scala213)

ThisBuild / githubWorkflowPublishTargetBranches := Seq()
ThisBuild / githubWorkflowEnv += ("JABBA_INDEX" -> "https://github.com/typelevel/jdk-index/raw/main/index.json")
ThisBuild / githubWorkflowJavaVersions := Seq("adoptium@8", "adoptium@17")
ThisBuild / githubWorkflowBuild := Seq(
  WorkflowStep
    .Sbt(
      List("scalafmtCheckAll", "scalafmtSbtCheck"),
      name = Some("Check formatting")
    ),
  WorkflowStep.Sbt(List("Test/compile"), name = Some("Compile")),
  WorkflowStep.Sbt(List("test"), name = Some("Run tests"))
)

lazy val kindProjectorVersion = "0.13.2"
lazy val aecorVersion = "0.20.2-CE3" // published to the local nexus for now
lazy val doobieVersion = "1.0.0-RC1"
lazy val catsEffectVersion = "3.3.1"
lazy val scalaCheckVersion = "1.15.1"
lazy val scalaTestVersion = "3.2.6"
lazy val catsVersion = "2.7.0"
lazy val circeVersion = "0.13.0"
lazy val logbackVersion = "1.2.3"
lazy val catsTaglessVersion = "0.14.0"
lazy val testContainersVersion = "0.39.5"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  ("evo-releases" at "http://nexus.market.local/repository/maven-releases/")
    .withAllowInsecureProtocol(true)
)

libraryDependencies ++= Seq(
  "io.aecor" %% "core" % aecorVersion,
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-postgres" % doobieVersion,
  "org.tpolecat" %% "doobie-hikari" % doobieVersion,
  "org.typelevel" %% "cats-effect" % catsEffectVersion,
  "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.tpolecat" %% "doobie-scalatest" % doobieVersion % Test,
  "org.typelevel" %% "cats-testkit" % catsVersion % Test,
  "org.typelevel" %% "cats-tagless-macros" % catsTaglessVersion % Test,
  "io.circe" %% "circe-core" % circeVersion % Test,
  "io.circe" %% "circe-generic" % circeVersion % Test,
  "io.circe" %% "circe-parser" % circeVersion % Test,
  "ch.qos.logback" % "logback-classic" % logbackVersion % Test,
  "com.dimafeng" %% "testcontainers-scala" % testContainersVersion % Test,
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testContainersVersion
)

addCommandAlias("fmt", "; Compile / scalafmt; Test / scalafmt; scalafmtSbt")

scalacOptions ++= Seq(
  "-J-Xss16m",
  "-Xsource:2.13"
)
addCompilerPlugin(("org.typelevel" %% "kind-projector" % kindProjectorVersion).cross(CrossVersion.full))

Test / parallelExecution := false

scalacOptions --= Seq("-Ywarn-unused:imports", "-Xfatal-warnings", "-Xlint:nullary-override")

publishMavenStyle := true

releaseCrossBuild := true

releaseCommitMessage := s"Set version to ${if (releaseUseGlobalVersion.value) (ThisBuild / version).value
else version.value}"
releaseVersionBump := sbtrelease.Version.Bump.Minor
publishTo := {
  val nexus = "http://nexus.market.local/repository/maven-"
  if (isSnapshot.value)
    Some("snapshots".at(nexus + "snapshots/").withAllowInsecureProtocol(true))
  else
    Some("releases".at(nexus + "releases/").withAllowInsecureProtocol(true))
}
credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

releaseCrossBuild := true
releaseVersionBump := Bump.Minor
releaseCommitMessage := s"Set version to ${if (releaseUseGlobalVersion.value) (ThisBuild / version).value
else version.value}"
releaseIgnoreUntrackedFiles := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value
homepage := Some(url("https://github.com/evotor/aecor-postgres-journal"))
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))
publishMavenStyle := true
Test / publishArtifact := false
pomIncludeRepository := { _ =>
  false
}

autoAPIMappings := true
scmInfo := Some(
  ScmInfo(url("https://github.com/evotor/aecor-postgres-journal"),
          "scm:git:git@github.com:evotor/aecor-postgres-journal.git"
  )
)
pomExtra :=
  <developers>
    <developer>
      <id>notxcain</id>
      <name>Denis Mikhaylov</name>
      <url>https://github.com/notxcain</url>
    </developer>
  </developers>

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges
)
