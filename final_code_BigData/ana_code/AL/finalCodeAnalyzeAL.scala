import org.apache.spark.sql.{DataFrame, SparkSession}

object finalCodeAnalyzeAL {
  // function to analyze the DataFrame
  def analyzeDataFrame(spark: SparkSession, df: DataFrame): Unit = {

    df.createOrReplaceTempView("cancer_data")
    
    // MEAN MEADIN MODE + STDV

    // Calculating Mean, Median, and Mode
    val totalMean = spark.sql("SELECT avg (Total) as mean FROM cancer_data")
    val totalMedian = spark.sql("""
        SELECT percentile_approx(Total, 0.5) as median_rating
        FROM cancer_data
        """)
    val totalMode = spark.sql("""
        SELECT Total as mode_total, COUNT(*) as mode_count
        FROM cancer_data
        GROUP BY Total
        ORDER BY mode_count DESC
        LIMIT 1
    """)
    val totalStdDev = spark.sql("""
        SELECT stddev(Total) as std_dev_total
        FROM cancer_data
    """)

    //MORE ADVANCED ANALYSIS

    //which year had more death total counts?
    val yearHighestDeathCount = spark.sql("""
        SELECT Year, SUM(Total) as count_sum
        FROM cancer_data
        GROUP BY Year 
        ORDER BY count_sum
        LIMIT 1
    """)

    //which cancer type had the highest mortality rate?

    val typeHighestDeathCount = spark.sql("""
        SELECT `Race and Hispanic Origin`,
        CASE 
            GREATEST(SUM(C00_C97), SUM(C00_C14), SUM(C15), SUM(C16), SUM(C18_C21),
                SUM(C22), SUM(C25), SUM(C32), SUM(C33_C34), SUM(C43), SUM(C50), 
                SUM(C53), SUM(C54_C55), SUM(C56), SUM(C61), SUM(C64_C65), SUM(C67),
                SUM(C70_C72), SUM(C81_C96), SUM(Other))
            WHEN SUM(C00_C97) THEN 'C00_C97'
            WHEN SUM(C00_C14) THEN 'C00_C14'
            WHEN SUM(C15) THEN 'C15'
            WHEN SUM(C16) THEN 'C16'
            WHEN SUM(C18_C21) THEN 'C18_C21'
            WHEN SUM(C22) THEN 'C22' 
            WHEN SUM(C25) THEN 'C25'
            WHEN SUM(C32) THEN 'C32' 
            WHEN SUM(C33_C34) THEN 'C33_C34' 
            WHEN SUM(C43) THEN 'C43'
            WHEN SUM(C50) THEN 'C50'
            WHEN SUM(C53) THEN 'C53' 
            WHEN SUM(C54_C55) THEN 'C54_C55' 
            WHEN SUM(C56) THEN 'C56'
            WHEN SUM(C61) THEN 'C61'
            WHEN SUM(C64_C65) THEN 'C64_C65'
            WHEN SUM(C67) THEN 'C67'
            WHEN SUM(C70_C72) THEN 'C70_C72'
            WHEN SUM(C81_C96) THEN 'C81_C96'
            WHEN SUM(Other) THEN 'Other'
        END AS typeHighest
    FROM cancer_data
    GROUP BY `Race and Hispanic Origin`
    ORDER BY typeHighest DESC
    """)

    //what about which ethnic group had highest death count overall?
    val ethnicHighestDeathCount = spark.sql("""
        SELECT `Race and Hispanic Origin` as ethnicity , SUM(Total) as sum_total
        FROM cancer_data
        GROUP BY ethnicity
        ORDER BY sum_total DESC
    """)

    //what about age overall? across all ethnic groups
    val ageHigestDeathCount = spark.sql("""
        SELECT `Age Broad Group` as age , SUM(Total) as sum_total
        FROM cancer_data
        GROUP BY age
        ORDER BY sum_total DESC
    """)

    //use smaller given groups 
    val ageSpecificDeathCount = spark.sql("""
        SELECT `Age Group` as age , SUM(Total) as sum_total
        FROM cancer_data
        GROUP BY age
        ORDER BY sum_total DESC
    """)

    //which ethnic group?
    val ethnicLowestDeathCount = spark.sql("""
        SELECT `Race and Hispanic Origin` as ethnicity , SUM(Total) as sum_total
        FROM cancer_data
        GROUP BY ethnicity
        ORDER BY sum_total ASC
        LIMIT 1
    """)


    //DISPLAY
    println(s"BASICS FOR US WOMENS CANCER DATA")
    println(s"For this dataset (DEATH COUNT)-- ")
    totalMean.show() 
    totalMedian.show()
    totalMode.show()
    totalStdDev.show()

    println(s"Year with most deaths (2020 vs 2021): ")
    yearHighestDeathCount.show()

    println(s"Cancer type with most deaths arcoss all years, by race:")
    typeHighestDeathCount.show()

    println(s"Race/Ethnic group by higest death count: ")
    ethnicHighestDeathCount.show()
    println(s"Race/Ethnic group with overall lowest death count: ")
    ethnicLowestDeathCount.show()
    println(s"Age group with highest death count: ")
    ageHigestDeathCount.show()
    println(s"Age sepcific group with highest: ")
    ageSpecificDeathCount.show()

  }
}