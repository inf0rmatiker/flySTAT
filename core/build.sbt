name := "FlySTAT"

version := "1.0"

scalaVersion := "2.11.8"

mainClass in (Compile, run) := Some("Main")

libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-core" % "2.3.0"
)
