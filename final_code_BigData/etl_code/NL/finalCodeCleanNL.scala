import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}

object finalCodeCleanNL {
  def loadAndCleanData(spark: SparkSession, filePath: String): DataFrame = {
    // Load the data
    val df = spark.read.format("csv").option("header", "true").load(filePath)

    // Drop unnecessary columns and filter out rows where SEX is 'Males'
    val cleanedData = df.drop("Borough", "HIV DIAGNOSES PER 100,000 POPULATION", 
      "PROPORTION OF CONCURRENT HIV/AIDS DIAGNOSES AMONG ALL HIV DIAGNOSES", 
      "AIDS DIAGNOSES PER 100,000 POPULATION")
      .filter(!($"SEX" === "Males"))

    cleanedData
  }
}
