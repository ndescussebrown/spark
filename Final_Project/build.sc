import mill._, scalalib._

import $ivy.`net.sourceforge.plantuml:plantuml:8059`
import net.sourceforge.plantuml.SourceFileReader;
import java.io.File

import mill.modules.Assembly

object Deps {
  val SPARK_VERSION = "3.1.3"
}

object batch extends ScalaModule { outer =>

  import Deps._ 

  def scalaVersion = "2.12.10"
  def scalacOptions =
    Seq("-encoding", "utf-8", "-explaintypes", "-feature", "-deprecation")

  def ivySparkDeps = Agg(
    ivy"org.apache.spark::spark-sql:${SPARK_VERSION}"
      .exclude("org.slf4j" -> "slf4j-log4j12"),
    ivy"org.slf4j:slf4j-api:1.7.16",
    ivy"org.slf4j:slf4j-log4j12:1.7.16"
  )

  def compileIvyDeps = ivySparkDeps

  def assemblyRules =
    Assembly.defaultRules ++
      Seq(
        "scala/.*",
        "org.slf4j.*",
        "org.apache.log4j.*"
      ).map(Assembly.Rule.ExcludePattern.apply)

  object standalone extends ScalaModule {
    def scalaVersion = outer.scalaVersion
    def moduleDeps = Seq(outer)
    def ivyDeps = outer.ivySparkDeps
    override def mainClass = T { Some("SparkCli") }

    def forkArgs = Seq("-Dspark.master=local[*]")
  }
}
