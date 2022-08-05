import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger;

// Create Spark session 
object SparkBatch {
    val log = Logger.getLogger(SparkBatch.getClass().getName())

    def run(f: SparkSession => Unit) = {
        val builder = SparkSession.builder.appName("Spark Batch")
        val spark = builder.getOrCreate()
        f(spark)
        spark.close
    }
    
    case class BrazilCovidCases(date:String,region:String,state:String,cases:String,deaths:String)

// Convert file brazil_covis19_cities.csv to new_brazil_covid19.csv with required format
    def convert(spark: SparkSession, in: String, out: String) = {
        val REQ_EX = "([0-9]{2}[-][0-9]{2}[-][0-9]{2})[,]([a-zA-Z]+[-]?[a-zA-Z]+)[,]([A-Z]{2})[,]([0-9]+[.]?[0-9]*)[,]([0-9]+)".r
        
        import spark.implicits._

        def readSource = spark.read.format("csv").option("header","true").load(in)
        def readSourceAsString = readSource.map(row => row.mkString(","))


        val brazilcovid=BrazilCovidCases("date","region","state","cases","deaths")
            
        // We use a BrazilCovid constructor as per below
        BrazilCovidCases.apply _  

        def toBrazilCovidCases(params: List[String]) = BrazilCovidCases(params(0), params(1), params(2), params(3), params(4))

        def cleanData(ds: Dataset[String]) = {
                           
                // We apply do the pattern matching
                val dsParsed = ds.flatMap(x => REQ_EX.unapplySeq(x))

                // We then convert our parsed string to a BrazilCovidCases instance 
                val ds_out = dsParsed.map(toBrazilCovidCases _)  

                ds_out   
        }

        val dsClean = cleanData(readSourceAsString)
        //Then we create a view so we can use Spark SQL to query the data
        dsClean.createOrReplaceTempView("BrazilCovid")
        
        // We then save the file to csv format
        spark.sql("select count(*) as count from BrazilCovid").coalesce(1).write.mode("Overwrite").option("header",true).csv(out)
    }

	
// Produce report-diff.json to compare new_brazil_covid19.csv with brazil_covid19.csv
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
        
        report.coalesce(1).write.mode("Overwrite").json(out)
        
    }
}