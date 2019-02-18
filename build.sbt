import sbtrelease.ReleaseStateTransformations._
import sbtrelease.Version.Bump

name := "aecor-postgres-journal"

organization := "io.aecor"

scalaVersion := "2.12.8"

lazy val kindProjectorVersion = "0.9.7"
lazy val aecorVersion = "0.19.0-SNAPSHOT"
lazy val doobieVersion = "0.6.0"
lazy val scalaCheckVersion = "1.13.4"
lazy val scalaTestVersion = "3.0.1"
lazy val scalaCheckShapelessVersion = "1.1.4"
lazy val catsVersion = "1.4.0"
lazy val circeVersion = "0.10.1"
lazy val scalametaParadiseVersion = "3.0.0-M11"
lazy val logbackVersion = "1.2.3"

resolvers ++= Seq(
  Resolver.sonatypeRepo("public")
)

libraryDependencies ++= Seq(
  "io.aecor" %% "core" % aecorVersion,
  "org.tpolecat" %% "doobie-core" % doobieVersion,
  "org.tpolecat" %% "doobie-postgres" % doobieVersion,
  "org.tpolecat" %% "doobie-hikari" % doobieVersion,
  "org.typelevel" %% "cats-effect" % "1.1.0",
  "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.tpolecat" %% "doobie-scalatest" % doobieVersion % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % scalaCheckShapelessVersion % Test,
  "org.typelevel" %% "cats-testkit" % catsVersion % Test,
  "io.circe" %% "circe-core" % circeVersion % Test,
  "io.circe" %% "circe-generic" % circeVersion % Test,
  "io.circe" %% "circe-parser" % circeVersion % Test,
  "io.circe" %% "circe-java8" % circeVersion % Test,
  "ch.qos.logback" % "logback-classic" % logbackVersion % Test
)

addCompilerPlugin(
  "org.scalameta" % "paradise" % scalametaParadiseVersion cross CrossVersion.full
)

scalacOptions ++= Seq(
  "-J-Xss16m",
  "-deprecation",
  "-encoding",
  "utf-8",
  "-explaintypes",
  "-feature",
  "-language:existentials",
  "-language:experimental.macros",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xcheckinit",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Xlint:adapted-args",
  "-Xlint:by-name-right-associative",
  "-Xlint:constant",
  "-Xlint:delayedinit-select",
  "-Xlint:doc-detached",
  "-Xlint:inaccessible",
  "-Xlint:infer-any",
  "-Xlint:missing-interpolator",
  "-Xlint:nullary-override",
  "-Xlint:nullary-unit",
  "-Xlint:option-implicit",
  "-Xlint:package-object-classes",
  "-Xlint:poly-implicit-overload",
  "-Xlint:private-shadow",
  "-Xlint:stars-align",
  "-Xlint:type-parameter-shadow",
  "-Xlint:unsound-match",
  "-Yno-adapted-args",
  "-Ypartial-unification",
  "-Ywarn-dead-code",
  "-Ywarn-extra-implicit",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused:implicits",
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:params",
  "-Ywarn-unused:patvars",
  "-Ywarn-unused:privates",
  "-Ywarn-value-discard",
  "-Xsource:2.13",
  "-Xplugin-require:macroparadise"
)
addCompilerPlugin("org.spire-math" %% "kind-projector" % kindProjectorVersion)

parallelExecution in Test := false
scalacOptions in (Compile, console) --= Seq("-Ywarn-unused:imports",
                                            "-Xfatal-warnings")

publishMavenStyle := true

releaseCrossBuild := true

releaseCommitMessage := s"Set version to ${if (releaseUseGlobalVersion.value) (version in ThisBuild).value
else version.value}"
releaseVersionBump := sbtrelease.Version.Bump.Minor
publishTo := {
  val nexus = "http://nexus.market.local/repository/maven-"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "snapshots/")
  else
    Some("releases" at nexus + "releases/")
}

releaseCrossBuild := true
releaseVersionBump := Bump.Minor
releaseCommitMessage := s"Set version to ${if (releaseUseGlobalVersion.value) (version in ThisBuild).value
else version.value}"
releaseIgnoreUntrackedFiles := true
releasePublishArtifactsAction := PgpKeys.publishSigned.value
homepage := Some(url("https://github.com/evotor/aecor-postgres-journal"))
licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT"))
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ =>
  false
}
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
}
autoAPIMappings := true
scmInfo := Some(
  ScmInfo(url("https://github.com/evotor/aecor-postgres-journal"),
          "scm:git:git@github.com:evotor/aecor-postgres-journal.git")
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
  ReleaseStep(action = "sonatypeReleaseAll" :: _),
  pushChanges
)
