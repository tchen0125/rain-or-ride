import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

val configuration = new Configuration()
val fileSystem = FileSystem.get(configuration)

def getFilePath(path: Path): String =  {
    val file = fileSystem
        .listStatus(path)
        .filter(_.getPath.getName.endsWith(".csv"))

    file.head.getPath.toString
}

// Data Preparation

val cleanedPath = "/user/bj2351_nyu_edu/final/cleaned/"
val weatherAggPath = getFilePath(new Path(cleanedPath + "weather/agg"))
val weatherCondPath = getFilePath(new Path(cleanedPath + "weather/cond"))
val collisionPath = getFilePath(new Path(cleanedPath + "collisionData"))
val ridershipPath = getFilePath(new Path(cleanedPath + "daily-ridership"))

val weatherAgg  = spark.read.option("header", true).csv(weatherAggPath)
  .withColumnRenamed("date_only", "date")
  .withColumnRenamed("MinTemp", "min t")
  .withColumnRenamed("MaxTemp", "max t")
  .withColumn("AvgTemp", round($"AvgTemp", 1))
  .withColumnRenamed("AvgTemp", "avg t")

val weatherCond = spark.read.option("header", true).csv(weatherCondPath)
  .drop("DATE")
  .withColumnRenamed("date_only", "date")

val collision   =
  spark.read.option("header", true).csv(collisionPath)
  .withColumnRenamed("CRASH DATE", "date")
  .withColumnRenamed("CRASH Number", "crash")
  .withColumnRenamed("NUMBER OF PERSONS INJURED_sum", "ppl i")
  .withColumnRenamed("NUMBER OF PERSONS KILLED_sum",  "ppl k")
  .withColumnRenamed("NUMBER OF PEDESTRIANS INJURED_sum", "ped i")
  .withColumnRenamed("NUMBER OF PEDESTRIANS KILLED_sum", "ped k")
  .withColumnRenamed("NUMBER OF CYCLIST INJURED_sum", "cyc i")
  .withColumnRenamed("NUMBER OF CYCLIST KILLED_sum", "cyc k")
  .withColumnRenamed("NUMBER OF MOTORIST INJURED_sum", "mot i")
  .withColumnRenamed("NUMBER OF MOTORIST KILLED_sum", "mot k")

val ridership   = spark.read.option("header", true).csv(ridershipPath)
  .withColumnRenamed("Date", "date")
  .withColumnRenamed("Subways: Total Estimated Ridership", "sub")
  .withColumnRenamed("Buses: Total Estimated Ridership", "bus")
  .withColumnRenamed("LIRR: Total Estimated Ridership", "lirr")
  .withColumnRenamed("Metro-North: Total Estimated Ridership", "metro-north")
  .withColumnRenamed("Access-A-Ride: Total Scheduled Trips", "acc-a-ride")
  .withColumnRenamed("Bridges and Tunnels: Total Traffic", "brdg-tun")
  .withColumnRenamed("Staten Island Railway: Total Estimated Ridership", "sttn-rw")


val full = weatherAgg.join(weatherCond, "date").join(collision, "date").join(ridership, "date")

// Data Analysis

full.show()

// Does a single weather condition affect the number of crashses?

val conditions = List("Rain", "Snow", "Fog", "Haze", "Mist")

println("Weather Conditions vs. Crashes")
conditions.foreach(cd => {
  full
    .groupBy(col(cd) === "true")
    .agg(
      sum("crash").alias("tt crash"),
      sum("ppl i").alias("ppl injured"),
      sum("ppl k").alias("ppl killed"),
      count("crash").alias("days"))
    .withColumn("ppl injured per day", ($"ppl injured" / $"days"))
    .withColumn("ppl injured per day", ($"ppl killed" / $"days"))
    .withColumn("crashes per day", round($"tt crash" / $"days"))
    .show()
})

println("Weather Conditions vs. Subway Ridership")
conditions.foreach(cd => {
  full
    .groupBy(col(cd) === "true")
    .agg(
      sum("sub").alias("sub"),
      count("sub").alias("days")
    )
    .withColumn("per day", round(col("sub") / col("days")))
    .show()
})

println("Weather Conditions vs. Bus Ridership")
conditions.foreach(cd => {
  full
    .groupBy(col(cd) === "true")
    .agg(
      sum("bus").alias("bus"),
      count("sub").alias("days")
    )
    .withColumn("per day", round(col("bus") / col("days")))
    .show()
})



