import mill._, scalalib._

import $ivy.`net.sourceforge.plantuml:plantuml:8059`
import net.sourceforge.plantuml.SourceFileReader;
import java.io.File

import mill.modules.Assembly

object Deps {
  val SPARK_VERSION = "3.1.2"
  val ELASTICSEARCH_VERSION = "7.15.0"
  val ZIO_V1 = "1.0.9"
  val ZIO_V2 = "2.0.0-RC2"
}

object batch extends ScalaModule { outer =>

  import Deps._ 

  def scalaVersion = "2.12.15"
  def scalacOptions =
    Seq("-encoding", "utf-8", "-explaintypes", "-feature", "-deprecation")

  def ivySparkDeps = Agg(
    ivy"org.apache.spark::spark-avro:${SPARK_VERSION}",
    ivy"org.apache.spark::spark-sql:${SPARK_VERSION}"
      .exclude("org.slf4j" -> "slf4j-log4j12"),
    ivy"org.apache.spark::spark-streaming:${SPARK_VERSION}"
      .exclude("org.slf4j" -> "slf4j-log4j12"),
    ivy"org.slf4j:slf4j-api:1.7.16",
    ivy"org.slf4j:slf4j-log4j12:1.7.16"
  )

  def ivyDeps = Agg(
    ivy"com.lihaoyi::upickle:0.9.7",
    ivy"org.elasticsearch::elasticsearch-spark-30:${ELASTICSEARCH_VERSION}",
    ivy"dev.zio::zio:${ZIO_V2}",
    ivy"io.getquill::quill-spark:3.9.0",
    ivy"org.apache.spark::spark-streaming-kafka-0-10:3.1.2",
    ivy"org.apache.spark::spark-sql-kafka-0-10:3.1.2"
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

object runner extends ScalaModule {

  import Deps._ 

  def scalaVersion = "2.13.6"

  def ivyDeps = Agg(
    ivy"dev.zio::zio:${ZIO_V2}",
    ivy"com.lihaoyi::os-lib:0.7.8"
  )

  def genPuml() = T.command {
    println("Generating documentation")
    for {
        path <- os.list(os.pwd/"doc").filter(_.ext == "puml").map(_.relativeTo(os.pwd))
        file = path.toString
        res = os.proc("java", "-jar", "tools/plantuml.1.2021.12.jar", file).call()
    } yield path.toString
    
  }

}

object api extends ScalaModule {

  import Deps._ 

  def scalaVersion = "2.13.6"

  def ivyDeps = Agg(
    ivy"dev.zio::zio::${ZIO_V2}",
    ivy"com.sksamuel.elastic4s::elastic4s-client-esjava:${ELASTICSEARCH_VERSION}",
    ivy"com.google.guava:guava:31.0.1-jre",
    ivy"ch.qos.logback:logback-classic:1.2.6",
    ivy"com.github.ghostdogpr::caliban:2.0.0-RC2",
    ivy"com.github.ghostdogpr::caliban-zio-http:2.0.0-RC2",
    //ivy"io.d11::zhttp:2.0.0-RC5"
//    ivy"com.github.ghostdogpr::caliban-http4s:1.2.4",
//    ivy"org.http4s::http4s-server:0.23.6", 
//    ivy"org.http4s::http4s-blaze-server:0.23.6", 
  )

  override def mainClass = T { Some("ApiServer") }


}

object ziotest extends ScalaModule {

  import Deps._ 

  def scalaVersion = "2.13.6"

  def ivyDeps = Agg(
    ivy"dev.zio::zio::${ZIO_V2}",
    ivy"ch.qos.logback:logback-classic:1.2.6"
  )

}
