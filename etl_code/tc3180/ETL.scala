import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

val file_path = "hw/input/motor-vehicle-collisions-2022.csv"
val data = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)

//Date Formatting
val dataFormatted = data.withColumn("CRASH DATE", to_date(col("CRASH DATE"), "yyyy-MM-dd"))

//Text formatting
val dataClean = dataFormatted.withColumn("BOROUGH", upper(trim(col("BOROUGH"))))

dataClean.show()

val numericalColumns = Seq(
  "NUMBER OF PERSONS INJURED", "NUMBER OF PERSONS KILLED",
  "NUMBER OF PEDESTRIANS INJURED", "NUMBER OF PEDESTRIANS KILLED",
  "NUMBER OF CYCLIST INJURED", "NUMBER OF CYCLIST KILLED",
  "NUMBER OF MOTORIST INJURED", "NUMBER OF MOTORIST KILLED"
)

numericalColumns.foreach { column =>
  dataClean.select(
    mean(col(column)).alias(s"${column}_mean"),
    stddev(col(column)).alias(s"${column}_stddev")
  ).show()
}

val collisionCounts = dataClean.groupBy("CRASH DATE").count().withColumnRenamed("count", "Crash Number")
collisionCounts.show()

// Calculate mean and standard deviation
val meanStddevDF = collisionCounts.agg(
  mean("Crash Number").alias("mean"),
  stddev("Crash Number").alias("stddev")
)

// Calculate median
val median = collisionCounts.stat.approxQuantile("Crash Number", Array(0.5), 0.0)(0)

// Calculate mode
val modeRow = collisionCounts.groupBy("Crash Number").count().orderBy(desc("count")).first()
val mode = modeRow.getAs[Long]("Crash Number")

// Combine mean, stddev, median, and mode into one DataFrame
val aggStats = meanStddevDF.withColumn("median", lit(median))
                           .withColumn("mode", lit(mode))

aggStats.show()

// Save dataClean, collisionCounts, and aggStats to HDFS
dataClean.write.option("header", "true").csv("/user/tc3180_nyu_edu/hw/output/dataClean.csv")
collisionCounts.write.option("header", "true").csv("/user/tc3180_nyu_edu/hw/output/collisionCounts.csv")
aggStats.write.option("header", "true").csv("/user/tc3180_nyu_edu/hw/output/aggStats.csv")
