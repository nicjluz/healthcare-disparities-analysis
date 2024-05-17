import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object finalCodeNL {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("HIVDataFrameAnalysisApp")
      .master("local[*]")
      .getOrCreate()
    
    // Load and clean data
    val filePath = "HIV_AIDS_Diagnoses_by_Neighborhood__Sex__and_Race_Ethnicity_20240425.csv"
    val cleanedData = finalCodeCleanNL.loadAndCleanData(spark, filePath)
    
    // Analyze data
    finalCodeAnalyzeNL.analyzeDataFrame(spark, cleanedData)
    
    // Stop SparkSession
   spark.stop()
   }
}
