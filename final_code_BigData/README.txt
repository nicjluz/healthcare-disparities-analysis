Authors: Ave Leone & Nicole Luzuriaga
Processing Big Data
Spring 2024

***File outline***
/data_files
Data used in process
AH_Provisional_Cancer_Death_Counts_by_Month_and_Year__2020-2021.csv
From US Department of Health and Human Services via https://catalog.data.gov/dataset/ah-provisional-cancer-death-counts-by-month-and-year-2020-2021-ab4a5
HIV_AIDS_Diagnoses_by_Neighborhood__Sex__and_Race_Ethnicity_20240425.csv
From: NYC open data
https://data.cityofnewyork.us/Health/HIV-AIDS-Diagnoses-by-Neighborhood-Sex-and-Race-Et/ykvb-493p/about_data 

/data_ingest
Text file with commands to run files properly (also featured below)

/etl_code 
Code to restructure data
AL for Ave Leone, NL for Nicole Luzuriaga
finalCodeCleanAL.scala
Takes AH_Provisional_Cancer file and removes unnecessary columns and makes data presentable
finalCodeCleanNL.scala
This file contains a method (‘loadAndCleanData’) that loads a CSV file into a DataFrame using SparkSession and cleans the data by dropping unnecessary columns and filtering out rows where SEX is ‘Males’.

/ana_code
Code to run and print stats
AL for Ave Leone, NL for Nicole Luzuriaga
finalCodeAnalyzeAL.scala
Takes dataframe from finalCodeCleanAL.scala and performs several sql queries via spark.sql to get needed information on race/ethnicity and age in relation to cancer deaths
finalCodeAnalyzeNL.scala
Takes dataframe and performs various analysis tasks on it, such as calculating mean, median, mode, and standard deviation of numerical data, and identifying the year, race/ethnicity, and neighborhood with the highest total diagnoses. 

/profiling_code
Code to run programs
AL for Ave Leone, NL for Nicole Luzuriaga
finalCodeAL.scala:
Calls finalCodeCleanAL and finalCodeAnalyzeAL functions to perform actions

finalCodeNL.scala:
This file contains the main entry point of the application of main method where the SparkSession is created, data is then loaded and cleaned using the methods from ‘finalCodeCleanNL’ and the cleaned data is then analyzed using methods from ‘finalCodeAnalyzeNL’. 

/screenshots
Various picture of running programs

To start out program:
hdfs dfs -put AH_Provisional_Cancer_Death_Counts_by_Month_and_Year__2020-2021.csv

Make sure all files are in same local dir then run:
spark-shell --deploy-mode client
:load finalCodeCleanAL.scala
:load finalCodeAnalyzeAL.scala
:load finalCodeAL.scala
Main.main(Array())

To re run, start locally then run:
rm finalCode*



