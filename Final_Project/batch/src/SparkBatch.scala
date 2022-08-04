import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger;

import org.elasticsearch.spark.sql._

import io.getquill.{Query, QuillSparkContext}

case class SimpleLog(value: String)

case class AccessLog(ip: String, ident: String, user: String, datetime: String, request: String, status: String, size: String, referer: String, userAgent: String, unk: String)
object AccessLog {
    val R = """^(?<ip>[0-9.]+) (?<identd>[^ ]) (?<user>[^ ]) \[(?<datetime>[^\]]+)\] \"(?<request>[^\"]*)\" (?<status>[^ ]*) (?<size>[^ ]*) \"(?<referer>[^\"]*)\" \"(?<useragent>[^\"]*)\" \"(?<unk>[^\"]*)\"""".r

    def fromString(s: String) = s match { 
        case R(ip: String, ident: String, user: String, datetime: String, request: String, status: String, size: String, referer: String, userAgent: String, unk: String) => Some(AccessLog(ip, ident, user, datetime, request, status, size, referer, userAgent, unk))
        case _ => None 
    }

    def fromStringNullable(s: String): AccessLog = s match { 
        case R(ip: String, ident: String, user: String, datetime: String, request: String, status: String, size: String, referer: String, userAgent: String, unk: String) => AccessLog(ip, ident, user, datetime, request, status, size, referer, userAgent, unk)
        case _ => null 
    }
 
}

object SparkBatch {
    val log = Logger.getLogger(SparkBatch.getClass().getName())

    def run(f: SparkSession => Unit) = {
        val builder = SparkSession.builder.appName("Spark Batch")
        builder
        .config("es.index.auto.create", "true")
        .config("es.nodes.wan.only", "true")
        .config("es.net.http.auth.user", "elastic")
        .config("es.net.http.auth.pass", "somethingsecret")
        .config("es.batch.size.bytes", 1024*1024*4)
        //.config("spark.eventLog.enabled", true)
        //.config("spark.eventLog.dir", "./spark-logs")
        val spark = builder.getOrCreate()
        f(spark)
        spark.close
    }

    def cleanQuill(spark: SparkSession) = {
        implicit val sqlContext = spark.sqlContext
        import sqlContext.implicits._
        import QuillSparkContext._

        spark.udf.register("asAccessLog", (s: String) => AccessLog.fromStringNullable(s))
        val asAccessLogUdf = quote {
            (s: String) => infix"asAccessLog($s)".as[AccessLog]
        }

        val accessLogDS: Dataset[SimpleLog] = spark.read.text("access.log.gz").as[SimpleLog]
        val accessLogs = quote {
           liftQuery(accessLogDS)
        }

        val result = quote {
            (q: Query[SimpleLog]) => q.map(x => asAccessLogUdf(x.value))
        }

        val ds: Dataset[AccessLog] = QuillSparkContext.run(result(accessLogs))
        ds.explain(true)
    }    

    def clean(spark: SparkSession, in: String, out: String) = {
       val REQ_EX = "([^ ]+)[ ]+([^ ]+)[ ]+([^ ]+)".r
       
       import spark.implicits._

       def readSource = spark.read.text(in).as[String]

       def cleanData(ds: Dataset[String]) = {
            val logs = ds.flatMap(AccessLog.fromString _)
            logs.printSchema()
            val dsWithTime = logs.withColumn("datetime", to_timestamp(logs("datetime"), "dd/MMM/yyyy:HH:mm:ss X"))
            val dsExtended = dsWithTime
                .withColumn("method", regexp_extract(dsWithTime("request"), REQ_EX.toString, 1))
                .withColumn("uri", regexp_extract(dsWithTime("request"), REQ_EX.toString, 2))
                .withColumn("http", regexp_extract(dsWithTime("request"), REQ_EX.toString, 3)).drop("request")
            dsExtended               
       }

       val dsExtended = cleanData(readSource)
       log.info(s"${dsExtended.schema.toDDL}")
       dsExtended.write.option("compression", "gzip").mode("Overwrite").parquet(out)
    }

    def index(spark: SparkSession, in: String) = {
        val df = spark.read.parquet(in).repartition(8)
        df.saveToEs("web/logs")
    }

    def report(spark: SparkSession, in: String, out: String) = {
        import spark.implicits._
        val df = spark.read.parquet(in).withColumn("date", col("datetime").cast("date"))
        df.createOrReplaceTempView("logs")
        val dates = spark.sql("""
        with dates as (
            select date, count(*) as cnt 
            from logs 
            group by date 
            having count(*) > 20000
            order by cnt desc)

        select date from dates             
        """).createTempView("dates")

       /* 
       val countByIp = spark.sql("""
        with ips as (
            select date, ip, count(*) as cnt
            from logs
            group by date, ip
        )

        select date, collect_list(struct(ip, cnt as count)) as ip_list from ips group by date
        """).createTempView("ips")
        */

        def createCountByFieldView(field: String) = spark.sql(s"""
        with ${field}s as (
            select date, $field, count(*) as cnt
            from logs
            group by date, $field
        )

        select date, collect_list(struct($field, cnt as count)) as ${field}_list from ${field}s group by date
        """).createTempView(s"${field}s")

        createCountByFieldView("ip")
        createCountByFieldView("uri")

        val report = spark.sql("""
        select dates.date, ips.ip_list, uris.uri_list
        from dates 
        inner join ips on ips.date = dates.date 
        inner join uris on uris.date = dates.date
        """)
        
        report.coalesce(1).write.mode("Overwrite").parquet(out)
        
    }
}