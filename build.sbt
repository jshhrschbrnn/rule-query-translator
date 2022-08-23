val scala3Version = "3.1.2"

lazy val root = project
  .in(file("."))
  .settings(
    name := "rule-query-translator",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-effect" % "3.3.12",
    "org.typelevel" %% "cats-parse" % "0.3.7",
    "org.semanticweb.rulewerk" % "rulewerk-vlog" % "0.8.0",
    "org.semanticweb.rulewerk" % "rulewerk-owlapi" % "0.8.0",
    "org.eclipse.rdf4j" % "rdf4j-rio-turtle" % "4.0.1",
    "org.eclipse.rdf4j" % "rdf4j-queryparser-sparql" % "4.0.1",
    "org.eclipse.rdf4j" % "rdf4j-util" % "3.7.7",
    "org.eclipse.rdf4j" % "rdf4j-sparqlbuilder" % "4.0.1",
    "org.eclipse.rdf4j" % "rdf4j-sail-memory" % "4.0.2",
    "org.eclipse.rdf4j" % "rdf4j-repository-sail" % "4.0.2",
    "com.monovore" %% "decline" % "2.3.0",
    "com.monovore" %% "decline-effect" % "2.3.0",
    "dev.optics" %% "monocle-core"  % "3.1.0",
    "dev.optics" %% "monocle-macro" % "3.1.0",
    "org.scala-graph" % "graph-core_2.13" % "1.13.5",
    "org.slf4j" % "slf4j-nop" % "2.0.0-alpha7",
    "org.scalacheck" %% "scalacheck" % "1.16.0",
    ),
  )

ThisBuild / assemblyMergeStrategy := {
  case PathList(ps @ _*) if ps.exists(_ contains "common") => MergeStrategy.last
  case PathList(ps @ _*) if ps.exists(_ contains "slf4j-api") => MergeStrategy.last
  case PathList(ps @ _*) if ps.exists(_ contains "module-info.class") => MergeStrategy.last
  case PathList(ps @ _*) if ps.last == "UUIDable.class" => MergeStrategy.last
  case x   =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}
