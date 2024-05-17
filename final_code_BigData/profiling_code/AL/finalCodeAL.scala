import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("DataFrameAnalysisApp")
      .master("local[*]")
      .config("spark.sql.debug.maxToStringFields", 100)
      .getOrCreate()

    import finalCodeCleanAL._
    import finalCodeAnalyzeAL._
    
    // Load data
    val cleanedData = finalCodeCleanAL.loadAndCleanData(spark)
    
    // analyze data
    finalCodeAnalyzeAL.analyzeDataFrame(spark, cleanedData)
    
    // Stop SparkSession
    spark.stop()
  }
}
