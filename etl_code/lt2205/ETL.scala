import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("RidershipData").getOrCreate()
import spark.implicits._

val inputFile = "/user/bj2351_nyu_edu/final/data/MTA-daily-ridership.csv"
val dailyRidershipDF = spark.read.option("header", true).csv(inputFile)

val columnsToDrop = Seq(
  "Subways: % of Comparable Pre-Pandemic Day",
  "Buses: % of Comparable Pre-Pandemic Day",
  "LIRR: % of Comparable Pre-Pandemic Day",
  "Metro-North: % of Comparable Pre-Pandemic Day",
  "Access-A-Ride: % of Comparable Pre-Pandemic Day",
  "Bridges and Tunnels: % of Comparable Pre-Pandemic Day",
  "Staten Island Railway: % of Comparable Pre-Pandemic Day"
)

val cleanedDF = dailyRidershipDF.drop(columnsToDrop: _*)

val startDate = "01/01/2022"
val endDate = "12/31/2022"

val filteredDF = cleanedDF.filter($"Date".between(startDate, endDate))

filteredDF.show()

filteredDF.write.option("header", "true").csv("/user/lt2205_nyu_edu/daily-ridership-clean.csv")

