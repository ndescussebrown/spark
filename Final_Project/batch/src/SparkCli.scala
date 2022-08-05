import org.apache.spark.sql.SparkSession

object SparkCli {

    def main(command: Array[String]) = command match {
        case Array("batch", in, out) => SparkBatch.run((spark: SparkSession) => SparkBatch.convert(spark, in, out))
		case Array("report", in, out) => SparkBatch.run((spark: SparkSession) => SparkBatch.report(spark, in, out))
        case _ => println(s"command '$command' not recognized (batch|report)")
    }
}
