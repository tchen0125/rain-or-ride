import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

val configuration = new Configuration()
val fileSystem = FileSystem.get(configuration)

val directoryPath = "/user/bj2351_nyu_edu/final/cleaned/daily-ridership"

def getFilePath(path: Path): String =  {
  val file = fileSystem
      .listStatus(path)
      .filter(_.getPath.getName.endsWith(".csv"))
  file.head.getPath.toString
}
val filePath = getFilePath(new Path(directoryPath))

val ridershipDF = spark.read.option("header", true).csv(filePath)

val columnsToAnalyze = Seq(
  "Subways: Total Estimated Ridership",
  "Buses: Total Estimated Ridership",
  "LIRR: Total Estimated Ridership",
  "Metro-North: Total Estimated Ridership",
  "Access-A-Ride: Total Scheduled Trips",
  "Bridges and Tunnels: Total Traffic",
  "Staten Island Railway: Total Estimated Ridership"
)

columnsToAnalyze.foreach { column =>
  ridershipDF.select(
    mean(col(column)).alias(s"${column}_mean"),
    stddev(col(column)).alias(s"${column}_stddev")
  ).show()
}
