name := "aecor-postgres-journal"

version := "0.1"

scalaVersion := "2.12.4"


lazy val kindProjectorVersion = "0.9.4"
lazy val aecorVersion = "0.17.0-SNAPSHOT"
lazy val doobieVersion = "0.5.2"
lazy val scalaCheckVersion = "1.13.4"
lazy val scalaTestVersion = "3.0.1"
lazy val scalaCheckShapelessVersion = "1.1.4"
lazy val catsVersion = "1.1.0"


libraryDependencies ++= Seq(
  "io.aecor" %% "test-kit" % aecorVersion,
  "org.tpolecat" %% "doobie-core"      % doobieVersion,
  "org.tpolecat" %% "doobie-hikari"    % doobieVersion,
  "org.tpolecat" %% "doobie-postgres"  % doobieVersion,
  "org.scalacheck" %% "scalacheck" % scalaCheckVersion % Test,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % scalaCheckShapelessVersion % Test,
  "org.typelevel" %% "cats-testkit" % catsVersion % Test
)

scalacOptions ++= commonScalacOptions
addCompilerPlugin("org.spire-math" %% "kind-projector" % kindProjectorVersion)
parallelExecution in Test := false
scalacOptions in (Compile, doc) := (scalacOptions in (Compile, doc)).value.filter(_ != "-Xfatal-warnings")
lazy val commonScalacOptions = Seq(
  "-J-Xss16m",
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:experimental.macros",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused-import",
  "-Ypartial-unification",
  "-Xsource:2.13"
)