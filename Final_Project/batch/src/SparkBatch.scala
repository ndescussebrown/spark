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
    
    // We define the case classes we wll be needing for file conversion
    //case class BrazilCovidCases(date:String,region:String,state:String,cases:String,deaths:String)
    //case class BrazilCovidCases_cities(date:String,state:String,cases:String,deaths:String)

// Convert files to dataset of strings format to prepare for Spakr sql
    def convert(spark: SparkSession, in1: String,in2: String, out: String) = {
        val REQ_EX_brazilcovid = "([0-9]{2}[-][0-9]{2}[-][0-9]{2})[,]([a-zA-Z]+[-]?[a-zA-Z]+)[,]([A-Z]{2})[,]([0-9]+[.]?[0-9]*)[,]([0-9]+)".r
        val REQ_EX_brazilcovid_cities = "([0-9]{4}[-][0-9]{2}[-][0-9]{2})[,]([A-Z]{2})[,]([0-9]+[.]?[0-9]*)[,]([0-9]+[.]?[0-9]*)".r

        import spark.implicits._



        // Convert brazil_covid19.csv file
        def read_brazilcovid = spark.read.format("csv").option("header","true").load(in1)
        //def read_brazilcovidAsString = read_brazilcovid.map(row => row.mkString(","))

        // Convert brazil_covid19_cities.csv file
        def read_brazilcovid_cities = spark.read.format("csv").option("header","true").load(in2)
        
        // We drop the columns we won't need from the brazil_covid19 file:
        //val read_brazilcovid_cities_reduced=read_brazilcovid_cities.drop("name","code")
        //def read_brazilcovid_citiesAsString = read_brazilcovid_cities_reduced.map(row => row.mkString(","))
            
        // We use a BrazilCovid constructor as per below for both covid file and covid_cities files
        //BrazilCovidCases.apply _  
        //BrazilCovidCases_cities.apply _  

        //def toBrazilCovidCases(params: List[String]) = BrazilCovidCases(params(0), params(1), params(2), params(3), params(4))
        //def toBrazilCovidCases_cities(params: List[String]) = BrazilCovidCases_cities(params(0), params(1), params(2), params(3))
        
        // We define our function to clean up the data 
        //def cleanData(ds: Dataset[String], my_req: scala.util.matching.Regex) = {
                           
                // We apply do the pattern matching
                //val dsParsed = ds.flatMap(x => my_req.unapplySeq(x))

                //dsParsed   
        //}

        // We turn our datasets of string to case class instances
        //val dsClean_brazilcovid = cleanData(read_brazilcovidAsString, REQ_EX_brazilcovid).map(toBrazilCovidCases _)
        //val dsClean_brazilcovid_cities = cleanData(read_brazilcovid_citiesAsString, REQ_EX_brazilcovid_cities).map(toBrazilCovidCases_cities _)
        
        //Then we create a view so we can use Spark SQL to query the data
        //dsClean_brazilcovid.createOrReplaceTempView("BrazilCovid")

        //We convert date string to date format for the brazil_covid19_cities file:
        //val dsWithDate_cities = dsClean_brazilcovid_cities.withColumn("date", to_date(dsClean_brazilcovid_cities("date"), "yyyy-MM-dd"))
        
        // We can now create the view for brazil_covid19_cities also:
        //dsWithDate_cities.createOrReplaceTempView("BrazilCovid_cities")
        
        // We then link the two files to obtain the region name and sum up cases and death for each day and each state, and save the file as csv
        //spark.sql("select date,regions.region,BrazilCovid_cities.state,sum(cases) as cases, sum(deaths) as deaths from BrazilCovid_cities left join (select distinct region,state from BrazilCovid) as regions on BrazilCovid_cities.state=regions.state group by date,BrazilCovid_cities.state,regions.region order by date,BrazilCovid_cities.state asc").coalesce(1).write.mode("Overwrite").option("header",true).csv(out)

        // MUCH QUICKER THAN THE ABOVE: 
        read_brazilcovid_cities.createOrReplaceTempView("BrazilCovid_cities")
        read_brazilcovid.createOrReplaceTempView("BrazilCovid")
        spark.sql("select to_date(date,'yyyy-MM-dd') as date,regions.region,BrazilCovid_cities.state,sum(cases) as cases, sum(deaths) as deaths from BrazilCovid_cities left join (select distinct region,state from BrazilCovid) as regions on BrazilCovid_cities.state=regions.state group by date,BrazilCovid_cities.state,regions.region order by to_date(date,'yyyy-MM-dd'),BrazilCovid_cities.state asc").coalesce(1).write.mode("Overwrite").option("header",true).csv(out)


    }

	
    // Produce report-diff.json to compare new_brazil_covid19.csv with brazil_covid19.csv
    def report(spark: SparkSession, in1: String,in2: String, out: String) = {
        import spark.implicits._
        val read_new_brazilcovid = spark.read.format("csv").option("header","true").load(in1)
        val read_brazilcovid = spark.read.format("csv").option("header","true").load(in2)
        read_new_brazilcovid.createOrReplaceTempView("new_brazilcovid")
        read_brazilcovid.createOrReplaceTempView("brazilcovid")

        //val output = spark.sql("""
        //select to_date(new.date, "yyyy-MM-dd") as new_date, new.state as new_state, new.cases-old.cases as diff_cases, new.deaths - old.deaths as diff_deaths from new
        //    LEFT JOIN old
        //    on to_date(old.date, "dd-MM-yy")=to_date(new.date, "yyyy-MM-dd") and old.state=new.state
        //    order by new_date, new_state asc"""
        //)

        //val mynew =  spark.sql("""with mynew as (select to_date(brazilcovid.date, 'dd-MM-yy') as old_date, brazilcovid.state as old_state from new_brazilcovid FULL OUTER JOIN brazilcovid on to_date(brazilcovid.date, 'dd-MM-yy')=to_date(new_brazilcovid.date, 'yyyy-MM-dd') and brazilcovid.state=new_brazilcovid.state WHERE to_date(new_brazilcovid.date, 'yyyy-MM-dd') IS NULL ) select count(*) as count_oldnotnew from mynew""")
        //val myold = spark.sql("""with myold as (select to_date(new_brazilcovid.date, 'yyyy-MM-dd') as new_date, new_brazilcovid.state as new_state from new_brazilcovid FULL OUTER JOIN brazilcovid on to_date(brazilcovid.date, 'dd-MM-yy')=to_date(new_brazilcovid.date, 'yyyy-MM-dd') and brazilcovid.state=new_brazilcovid.state WHERE to_date(brazilcovid.date, 'dd-MM-yy') IS NULL) select count(*) as count_newnotold from myold""")
        //val same = spark.sql("""with same as (select to_date(new_brazilcovid.date, 'yyyy-MM-dd') as new_date, new_brazilcovid.state as new_state, new_brazilcovid.cases-brazilcovid.cases as diff_cases, new_brazilcovid.deaths - brazilcovid.deaths as diff_deaths, to_date(brazilcovid.date, 'dd-MM-yy') as old_date, brazilcovid.state as old_state from new_brazilcovid JOIN brazilcovid on to_date(brazilcovid.date, 'dd-MM-yy')=to_date(new_brazilcovid.date, 'yyyy-MM-dd') and brazilcovid.state=new_brazilcovid.state WHERE new_brazilcovid.cases-brazilcovid.cases=0 AND new_brazilcovid.deaths-brazilcovid.deaths=0 ORDER BY new_date, new_state asc) select count(*) as count_same from same""")
        //val notsame = spark.sql("""with notsame as (select to_date(new_brazilcovid.date, 'yyyy-MM-dd') as new_date, new_brazilcovid.state as new_state, new_brazilcovid.cases-brazilcovid.cases as diff_cases, new_brazilcovid.deaths - brazilcovid.deaths as diff_deaths, to_date(brazilcovid.date, 'dd-MM-yy') as old_date, brazilcovid.state as old_state from new_brazilcovid JOIN brazilcovid on to_date(brazilcovid.date, 'dd-MM-yy')=to_date(new_brazilcovid.date, 'yyyy-MM-dd') and brazilcovid.state=new_brazilcovid.state WHERE new_brazilcovid.cases-brazilcovid.cases!=0 AND new_brazilcovid.deaths-brazilcovid.deaths!=0 ORDER BY new_date, new_state asc) select count(*) as count_notsame from notsame""")
        
        val output= spark.sql("""SELECT COUNT(to_date(new_brazilcovid.date, 'yyyy-MM-dd')) AS destination_rows,COUNT(to_date(brazilcovid.date, 'dd-MM-yy')) AS source_rows, SUM(CASE WHEN to_date(new_brazilcovid.date, 'yyyy-MM-dd') IS NULL THEN 1 ELSE 0 END) AS count_oldnotnew, SUM(CASE WHEN to_date(brazilcovid.date, 'dd-MM-yy') IS NULL THEN 1 ELSE 0 END) AS count_newnotold, SUM(CASE WHEN new_brazilcovid.cases-brazilcovid.cases=0 AND new_brazilcovid.deaths-brazilcovid.deaths=0 THEN 1 ELSE 0 END) AS count_same, SUM(CASE WHEN new_brazilcovid.cases-brazilcovid.cases!=0 OR new_brazilcovid.deaths-brazilcovid.deaths!=0 THEN 1 ELSE 0 END) AS count_notsame from new_brazilcovid FULL OUTER JOIN brazilcovid on to_date(brazilcovid.date, 'dd-MM-yy')=to_date(new_brazilcovid.date, 'yyyy-MM-dd') and brazilcovid.state=new_brazilcovid.state""")

        output.coalesce(1).write.mode("Overwrite").json(out)
        
    }
}