import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions._

// Initialize Spark session
val spark = SparkSession.builder.appName("Motor Vehicle Collision Analysis").getOrCreate()

val configuration = new Configuration()
val fileSystem = FileSystem.get(configuration)

val directoryPath = "/user/bj2351_nyu_edu/final/cleaned/motor-vehicle-collision"


// Function to get file path from directory
def getFilePath(path: Path): String =  {
  val file = fileSystem
      .listStatus(path)
      .filter(_.getPath.getName.endsWith(".csv"))
  file.head.getPath.toString
}
val filePath = getFilePath(new Path(directoryPath))

// Reading the CSV file
val data = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)
val dataClean = data 

// Define the columns for analysis
val numericalColumns = Seq(
  "NUMBER OF PERSONS INJURED", "NUMBER OF PERSONS KILLED",
  "NUMBER OF PEDESTRIANS INJURED", "NUMBER OF PEDESTRIANS KILLED",
  "NUMBER OF CYCLIST INJURED", "NUMBER OF CYCLIST KILLED",
  "NUMBER OF MOTORIST INJURED", "NUMBER OF MOTORIST KILLED"
)

// Calculate mean and standard deviation
numericalColumns.foreach { column =>
  dataClean.select(
    mean(col(column)).alias(s"${column}_mean"),
    stddev(col(column)).alias(s"${column}_stddev")
  ).show()
}

