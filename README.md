# This repo is dedicated to the work performed as part of the final Data Pipelines 2 project for the DSTI MSc in Data Engineering

The project used some files included in the Kaggle dataset:  https://www.kaggle.com/datasets/unanimad/corona-virus-brazil

The Goal of the project is twofold:
* To compute a new_brazil_covid19.csv file using data in brazil_covid19_cities.csv so that the structure and meaning of the data is the same as brazil_covid19.csv.
* Compare the new_brazil_covid19.csv file and provided brazil_covid19.csv file

### Strategy used to compute a new_brazil_covid19.csv file

The strategy used to build the project was incremental.

First, the spark-hands-on repository wiki from teacher Jean-Luc Canela (https://github.com/jlcanela/spark-hands-on/wiki) was used to run standalone commands in spark shell to try and address the requirements of the Final project. This served as a test that commands fulfilled their purpose and could then subsequently be used ina full-fledged spark programme.

The strategy taken was to use both fulltext-search-sample (https://github.com/jlcanela/fulltext-search-sample) and basicreport (https://github.com/jlcanela/basicreport/) go-bys repositories from Jean-Luc Canela as I could see that both would be useful in building the project:
* The fulltext-search-sample was used as as go-by to address the complexity of the challenge but reduce the number of files produced (only two files) whilst helping me understand which files were not required
* The basicreport files were used to better understand how to clean up the fulltext-search file of unnecessary methods.

### How to clone the github project

From command line: 
```
git clone https://github.com/ndescussebrown/spark.git
```

### How to run the batch using the mill command locally 

To run the batch locally, we need to use the standalone module of the build.sc file and run the following:

```
./mill -i batch.standalone.run batch data/brazil_covid19.csv data/brazil_covid19_cities.csv new_brazil_covid19.csv
```

There are two input files in our case:
* brazil_covid19.csv - this file is used to extract the region name corresponding to each state
* brazil_covid19_cities - this is the file we want to convert to the brazil_covid19.csv format

### How to package automatically the jar file 

### How to run the batch using the spark-submit command locally 

### How to run the batch using the spark-submit command with AWS 

### How to generate the diff report locally 

### How to copy the data to AWS s3 so that the AWS spark-submit command executes without error (aws s3 …)

### How to fetch the new_brazil_covid19.csv file (aws s3 …)

### How to fetch the report_diff.json file  (aws s3 …)