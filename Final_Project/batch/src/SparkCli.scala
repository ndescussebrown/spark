import org.apache.spark.sql.SparkSession

object SparkCli {

    def main(command: Array[String]) = command match {
        case Array("batch", in1,in2, out) => SparkBatch.run((spark: SparkSession) => SparkBatch.convert(spark, in1,in2, out))
		case Array("report", in1,in2, out) => SparkBatch.run((spark: SparkSession) => SparkBatch.report(spark,in1,in2, out))
        case _ => println(s"command '$command' not recognized (batch|report)")
    }
}
