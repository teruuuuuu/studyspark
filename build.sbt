
name := "StudySpark"

version := "1.0"

scalaVersion := "2.11.8"
val sparkVersion = "2.1.0"

// Intelijより直接実行する場合はspark関係はcompileにする、sbt assemblyでjarを出力する場合はprovidedにしておく
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-graphx_2.11" % sparkVersion % "provided",
  "org.apache.spark" % "spark-hive_2.11" % sparkVersion % "provided",
  "joda-time" % "joda-time" % "2.9.7"
)

// assembly settings
assemblyJarName in assembly := "studyspark.jar"
assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
