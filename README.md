# This repo is dedicated to the work performed as part of the final Data Pipelines 2 project for the DSTI MSc in Data Engineering

The project used some files included in the Kaggle dataset:  https://www.kaggle.com/datasets/unanimad/corona-virus-brazil

The Goal of the project is twofold:
* To compute a new_brazil_covid19.csv file using data in brazil_covid19_cities.csv so that the structure and meaning of the data is the same as brazil_covid19.csv.
* Compare the new_brazil_covid19.csv file and provided brazil_covid19.csv file

### 1. Strategy used to compute a new_brazil_covid19.csv file

The strategy used to build the project was incremental.

First, the spark-hands-on repository wiki from teacher Jean-Luc Canela (https://github.com/jlcanela/spark-hands-on/wiki) was used to run standalone commands in spark shell to try and address the requirements of the Final project. This served as a test that commands fulfilled their purpose and could then subsequently be used ina full-fledged spark programme.

The code was refined by using both the fulltext-search-sample (https://github.com/jlcanela/fulltext-search-sample) and basicreport (https://github.com/jlcanela/basicreport/) go-bys repositories from Jean-Luc Canela as I could see that both would be useful in building the project:
* 
* The basicreport files were used to better understand how to clean up the fulltext-search file of unnecessary methods.
* The fulltext-search-sample was used as as go-by to address the complexity of the challenge but reduce the number of files produced (only two files) whilst helping me understand which files were not required

Looking at the above strategy, I made a mistake that was to follow the spark-hands-on approach too closely because at first I couldn't identify the fundamental difference between the files to process. The spark-hands-on input file was basically a list of logs in string unstructured format, for which we had used a formal class, together with a regex expression, to structure the data. However in our case the data provided in csv file was already structured so that spark sql could actually be used directly on the dataframe generated when ingesting the input data. I intentionally left the first code I used (as commented lines) to show the approach taken, but then simplified it extensively to what it is now.

### 2. Strategy used to compare the new_brazil_covid19.csv file to the original brazil_covid19.csv file

I learnt from the mistake I made for the file conversion (see Section 1) and worked directly with the dataframe of the input file. As per the brief of the assignment, my goal was to produce a json file containing the following information:
* number of source rows
* number of destination rows
* number of rows present in source but not in destination (= key present in source but not in destination)
* number of rows present in destination but not in source (= key present in destination but not in source)
* number of rows identical in source and destination (= having same cases & deaths values for the given key)
* number of rows with different values in source and destination ( = having at least a different cases & deaths value for a given key)

I used a simple sql query to extract the above, which I saved into a dataframe ready to be exported to json format.

### 3. Outcome and explanation

The new_brazil_covid19.csv obtained using the processing from the barzil_covid19_cities files as explained in Section 6 was found to have some discrepancies compared to the original brazil_covid19.csv. 

The results from the report-diff.json file are reported in the table below:

| destination_rows | source_rows | count_oldnotnew | count_newnotold | count_same | count_notsame |
|------------------|-------------|-----------------|-----------------|------------|---------------|
|      11421       |     12258   |       837       |         0       |     7384   |      4037     |


Interpretation:

* count_oldnotnew = 837 means that for 837 (date,state) keys, there are no records of covid cases and deaths in the input brazil_covid19_cities.csv file for these states at these dates.
* count_newnotold = 0 means that there are no records in the new_brazil_covid19.csv file that are not also present in the original brazil_covid19.csv file.
* count_same = 7384 means there are 7384 identical records between the new_brazil_covid19.csv file and the original brazil_covid19.csv file.
* count_notsame = 4037 means there are 4037 records shared by both files but with different values, either in number of reported cases, reported deaths, or both.
* The difference between the destination_rows and source_rows, taken together with the count_oldnotnew and count_newnotold values means there are 837 records present in the original brazil_covid19.csv file that are not present in the new_brazil_covid19.csv file.

Explanation:
* Difference between the destination_rows and source_rows: This is due to the fact that the input file brazil_covid19_cities covers reporting period from 27-03-2020 to 23-05-2021, whereas the original brazil_covid19.csv file covers reporting period 25-02-2020 to 21-05-2021. So 31 days of reporting are missing (as 2020 was a leap year, hence February had 29 days) from the brazil_covid19_cities file, for each of the 27 states, i.e. exactly 837 records.
* count_newnotold = 0 is also explained by the above (reporting period for brazil_covid19_cities.csv file started 31 days after the reporting period for brazil_covid19.csv file but ended on the same day).
* With regard to count_notsame = 4037 as the difference between number of cases and deaths between both files is either negative or positive, there could be different explanation. When the number of cases or deaths is greater in the brazilcovid19.csv file than in the new_brazilcovid19.csv file (about than 38% of the number of records for the overlapping period), it could be due to the fact that the cases or deaths were not registered with the cities but in the countryside for example. When the number of cases or deaths is greater in the new_brazilcovid19.csv file than in the brazilcovid19.csv file (less than 1% of the number of records for the overlapping period and representing an error on actual number of cases or deaths of approximately 1% also), it could simply be due to reporting errors, or delays in reporting to the next day etc.

### 4. Architecture Diagram

XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX

### 5. How to clone the github project

From command line: 
```
git clone https://github.com/ndescussebrown/spark.git
```

### 6. How to run the batch using the mill command locally 

To run the batch locally, we need to use the standalone module of the build.sc file and run the following:

```
./mill -i batch.standalone.run batch data/brazil_covid19.csv data/brazil_covid19_cities.csv new_brazil_covid19.csv
```

There are two input files in our case:
* brazil_covid19.csv - this file is used to extract the region name corresponding to each state
* brazil_covid19_cities - this is the file we want to convert to the brazil_covid19.csv format

The output file is new_brazil_covid19.csv.

### 7. How to package automatically the jar file 

The command to package automatically the jar file is as follows:

```
./mill -i batch.assembly
```

But the above only packages the java code, not the spark dependencies. If we also want to package the spark dependencies, then we need to run the following command, which takes more time:

```
./mill -i batch.standalone.assembly
```

### 8. How to run the batch using the spark-submit command locally 

At first I had issues running spark-submit as I was getting this message "/bin/spark-class: No such file or directory". I worked out that it was because I had not set my environment variable SPARK_HOME in wsl (only in Windows). I corrected this by using the following:

```
export SPARK_HOME=/mnt/c/spark-3.1.3
```

I could then retry the command with the right path to spark-submit:

```
$SPARK_HOME/bin/spark-submit --class SparkCli $PWD/out/batch/assembly.dest/out.jar batch data/brazil_covid19.csv data/brazil_covid19_cities.csv new_brazil_covid19.csv
```

### 9. How to run the batch using the spark-submit command with AWS 

For the batch:
```
spark-submit --deploy-mode cluster --class SparkCli s3://ndbspark/jars/batch-v1.jar batch s3://ndbspark/data/input/brazil_covid19.csv s3://ndbspark/data/input/brazil_covid19_cities.csv s3://ndbspark/data/output/new_brazil_covid19_cities.csv 
```

For the report:
```
spark-submit --deploy-mode cluster --class SparkCli s3://ndbspark/jars/batch-v1.jar report s3://ndbspark/data/output/new_brazil_covid19.csv s3://ndbspark/data/input/brazil_covid19.csv s3://ndbspark/data/output/report-diff.json 
```

ADD SCREESHOTS!!!!!




### 10. How to generate the diff report locally 

To run the report locally, we need to use the standalone module of the build.sc file and run the following:

```
./mill -i batch.standalone.run report new_brazil_covid19.csv data/brazil_covid19.csv report-diff.json
```

There are two input files in our case:
* new_brazil_covid19.csv - this is the file generated as described in Section 6 above.
* brazil_covid19.csv - this is the file we want to compare it to

The output file, report-diff.json, contains 6 fields, which are as follows:
* "destination_rows":this is the number of rows present in the new_brazil_covid19.csv file
* "source_rows": this is the number of rows present in the original brazil_covid19.csv file
* "count_oldnotnew": this is the number of rows present in the original brazil_covid19.csv file but not in the new_brazil_covid19.csv file
* "count_newnotold":this is the number of rows present in the original brazil_covid19.csv file but not in the new_brazil_covid19.csv file
* "count_same": this is the number of rows that are identical in the original brazil_covid19.csv file and in the new_brazil_covid19.csv file
* "count_notsame": this is the number of rows that differ in the original brazil_covid19.csv file and in the new_brazil_covid19.csv file


### 11. How to copy the data to AWS s3 so that the AWS spark-submit command executes without error (aws s3 …)

Because I had been using Spark version 3.1.3 to run the jar manually (due to a number of installation issues on Windows) and I couldn't find this version in AWS, I had to repackage the jat file using Spark version 3.2.0 (updating the version in build.sc file and running the following command again:

```
./mill -i batch.assembly
```








### 12. How to fetch the new_brazil_covid19.csv file (aws s3 …)










### 13. How to fetch the report_diff.json file  (aws s3 …)