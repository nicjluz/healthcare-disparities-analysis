import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object finalCodeAnalyzeNL {
  // Function to analyze the DataFrame
  def analyzeDataFrame(spark: SparkSession, df: DataFrame): Unit = {

    df.createOrReplaceTempView("hiv_data")
    
    // Calculate mean, median, mode, and standard deviation of numerical data
    val totalMean = spark.sql("SELECT avg(`TOTAL NUMBER OF HIV DIAGNOSES`) as mean FROM hiv_data")
    val totalMedian = spark.sql("SELECT percentile_approx(`TOTAL NUMBER OF HIV DIAGNOSES`, 0.5) as median FROM hiv_data")
    val totalMode = spark.sql("""
        SELECT `TOTAL NUMBER OF HIV DIAGNOSES` as mode_total, COUNT(*) as mode_count
        FROM hiv_data
        GROUP BY `TOTAL NUMBER OF HIV DIAGNOSES`
        ORDER BY mode_count DESC
        LIMIT 1
    """)
    val totalStdDev = spark.sql("SELECT stddev(`TOTAL NUMBER OF HIV DIAGNOSES`) as std_dev FROM hiv_data")

    // Get year with the highest total diagnoses
    val yearHighestDiagnoses = spark.sql("""
        SELECT YEAR, sum(`TOTAL NUMBER OF HIV DIAGNOSES`) as total_diagnoses
        FROM hiv_data
        GROUP BY YEAR
        ORDER BY total_diagnoses DESC
        LIMIT 1
    """)

    // Get race/ethnicity with the highest total diagnoses
    val raceHighestDiagnoses = spark.sql("""
        SELECT `RACE/ETHNICITY`, sum(`TOTAL NUMBER OF HIV DIAGNOSES`) as total_diagnoses
        FROM hiv_data
        WHERE `RACE/ETHNICITY` NOT IN ('All', 'Unknown')
        GROUP BY `RACE/ETHNICITY`
        ORDER BY total_diagnoses DESC
        LIMIT 1
    """)

    // Get neighborhood with the highest total diagnoses
    val neighborhoodHighestDiagnoses = spark.sql("""
        SELECT `Neighborhood (U.H.F)`, sum(`TOTAL NUMBER OF HIV DIAGNOSES`) as total_diagnoses
        FROM hiv_data
        WHERE `Neighborhood (U.H.F)` != 'All'
        GROUP BY `Neighborhood (U.H.F)`
        ORDER BY total_diagnoses DESC
        LIMIT 1
    """)

    // Get range of total diagnoses compared to others
    val totalDiagnosesRange = spark.sql("""
        SELECT
            `RACE/ETHNICITY`,
            sum(`TOTAL NUMBER OF HIV DIAGNOSES`) as total_diagnoses,
            max(`TOTAL NUMBER OF HIV DIAGNOSES`) as max_diagnoses,
            min(`TOTAL NUMBER OF HIV DIAGNOSES`) as min_diagnoses,
            (max(`TOTAL NUMBER OF HIV DIAGNOSES`) - sum(`TOTAL NUMBER OF HIV DIAGNOSES`)) as diff_max,
            (sum(`TOTAL NUMBER OF HIV DIAGNOSES`) - min(`TOTAL NUMBER OF HIV DIAGNOSES`)) as diff_min
        FROM hiv_data
        WHERE `RACE/ETHNICITY` NOT IN ('All', 'Unknown')
        GROUP BY `RACE/ETHNICITY`
        ORDER BY total_diagnoses DESC
    """)

    // Display the analysis results
    println("HIV DIAGNOSES ANALYSIS")
    totalMean.show()
    totalMedian.show()
    totalMode.show()
    totalStdDev.show()
    println("Year with the highest total diagnoses:")
    yearHighestDiagnoses.show()
    println("Race/Ethnicity with the highest total diagnoses:")
    raceHighestDiagnoses.show()
    println("Neighborhood with the highest total diagnoses:")
    neighborhoodHighestDiagnoses.show()
    println("Range of total diagnoses compared to others:")
    totalDiagnosesRange.show()
  }
}
