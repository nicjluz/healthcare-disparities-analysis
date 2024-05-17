import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Column
import org.apache.spark.sql.{DataFrame, SparkSession}

object finalCodeCleanAL {
  // Function to load and clean the DataFrame
  def loadAndCleanData(spark: SparkSession): DataFrame = {

    //Include initial code analysis for your data!! 
    val inputDF = spark.read.option("header", "true").csv("AH_Provisional_Cancer_Death_Counts_by_Month_and_Year__2020-2021.csv")

    //DATA CLEANING//

    //fliter 1 by sex (7th col)
    val filteredInputDF = inputDF.filter(col("Sex") === "Female (F)")
    // remove '(F)'from 'sex' col
    val filteredInputDF2 = filteredInputDF.withColumn("Sex", regexp_replace(col("Sex"), " \\(F\\)", ""))
        //drop not needed columns
    val columnsToDrop = Seq("Country","Start Date", "End Date")
    val filteredInputDF3 = filteredInputDF2.drop(columnsToDrop: _*)

    
    //make a total death column
    //makes group of all columns and gets sum
    val cancerDeathColumns = filteredInputDF3.columns.slice(6, 25) //were 9 and 28 in the original df 
    val filteredInputDF4 = cancerDeathColumns.foldLeft(filteredInputDF3) { (acc, colName) =>
      acc.withColumn(colName, col(colName).cast("int"))
    }
    val filteredInputDF5 = filteredInputDF4.withColumn("Total", cancerDeathColumns.map(col).reduce(_ + _))

    // make new the age group column 
    filteredInputDF5.createOrReplaceTempView("cancer_data")
    val result1DF = spark.sql("""
        SELECT *,
               CASE 
                 WHEN `Age Group` = "0-14 years" OR `Age Group` = "15-24 years" THEN "0-24 years"
                 WHEN `Age Group` = "25-34 years" OR `Age Group` = "35-44 years" OR `Age Group` = "45-54 years" THEN "25-54 years"
                 ELSE "55+ years"
               END AS `Age Broad Group`
        FROM cancer_data
    """)
    result1DF.createOrReplaceTempView("cancer_data")
    val resultDF = spark.sql("""
        SELECT `Data as of`, Year, Month, Sex, `Age Group`, `Race and Hispanic Origin`,
            `Malignant neoplasms (C00-C97)` as C00_C97,
            `Malignant neoplasms of lip, oral cavity and pharynx (C00-C14)` as C00_C14,
            `Malignant neoplasm of esophagus (C15)` as C15,
            `Malignant neoplasm of stomach (C16)` as C16,
            `Malignant neoplasms of colon, rectum and anus (C18-C21)` as C18_C21,
            `Malignant neoplasms of liver and intrahepatic bile ducts (C22)` as C22, 
            `Malignant neoplasm of pancreas (C25)` as C25,
            `Malignant neoplasm of larynx (C32)` as C32, 
            `Malignant neoplasms of trachea, bronchus and lung (C33-C34)` as C33_C34 ,
            `Malignant melanoma of skin (C43)` as C43,
            `Malignant neoplasm of breast (C50)` as C50,
            `Malignant neoplasm of cervix uteri (C53)` as C53, 
            `Malignant neoplasms of corpus uteri and uterus, part unspecified (C54-C55)` as C54_C55, 
            `Malignant neoplasm of ovary (C56)` as C56,
            `Malignant neoplasm of prostate (C61)` as C61,
            `Malignant neoplasms of kidney and renal pelvis (C64-C65)` as C64_C65,
            `Malignant neoplasm of bladder (C67)` as C67,
            `Malignant neoplasms of meninges, brain and other parts of central nervous system (C70-C72)` as C70_C72,
            `Malignant neoplasms of lymphoid, hematopoietic and related tissue (C81-C96)` as C81_C96,
            `All other and unspecified malignant neoplasms (C17,C23-C24,C26-C31,C37-C41,C44-C49,C51-C52,C57-C60,C62-C63,C66,C68-C69,C73-C80,C97)` as Other,
            Total, `Age Broad Group`
        FROM cancer_data
    """)
    resultDF.show()
    resultDF
    
   
  }
}
