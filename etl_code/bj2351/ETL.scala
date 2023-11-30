import org.apache.spark.sql.DataFrame

val inputFile = "hw7/central-park-2022-weather.csv"
val weatherDF = spark.read.option("header", true).csv(inputFile)

val usingColumns =
      Array(
          "DATE",
          "ELEVATION",
          "REPORT_TYPE",
          "SOURCE",
          "HourlyAltimeterSetting",
          "HourlyDewPointTemperature",
          "HourlyDryBulbTemperature",
          "HourlyPrecipitation",
          "HourlyPresentWeatherType",
          "HourlyPressureChange",
          "HourlyPressureTendency",
          "HourlyRelativeHumidity",
          "HourlySkyConditions",
          "HourlySeaLevelPressure",
          "HourlyStationPressure",
          "HourlyVisibility",
          "HourlyWetBulbTemperature",
          "HourlyWindDirection",
          "HourlyWindGustSpeed",
          "HourlyWindSpeed",
          "Sunrise",
          "Sunset"
      )

def shrinkColumns(df: DataFrame, colsInUse: Seq[String]): DataFrame = {
    val cols = df.columns
    val dropCols = cols.toSet -- colsInUse.toSet
    val droppedDf = df.drop(dropCols.toSeq: _*)
    droppedDf
}

val droppedWeatherDF = shrinkColumns(weatherDF, usingColumns)

droppedWeatherDF.printSchema()

val fm15 = droppedWeatherDF.filter('REPORT_TYPE === "FM-15").drop("REPORT_TYPE")
val fm16 = droppedWeatherDF.filter('REPORT_TYPE === "FM-16").drop("REPORT_TYPE")
val sod = droppedWeatherDF.filter('REPORT_TYPE === "SOD  ").drop("REPORT_TYPE")
val som = droppedWeatherDF.filter('REPORT_TYPE === "SOM  ").drop("REPORT_TYPE")

def countNulls(df: DataFrame) {
    println(df.count() + " rows in total")
    val cols = df.columns
    cols.foreach(col => {
        val col_nulls = (col, df.filter(df(col).isNull).count())
        println(col_nulls)
    })
}

println("FM-15 reports null counts")
countNulls(fm15)

println("FM-15 has only null values in Sunrise and Sunset columns")
val cleanFm15: DataFrame = fm15.drop("Sunrise", "Sunset")

println("FM-16 reports null counts")
countNulls(fm16)
println("FM-16 has only null values in HourlyPressureChange, HourlyPressureTendency, Sunrise and Sunset columns")
val cleanFm16: DataFrame = fm16.drop(
    "HourlyPressureChange",
    "HourlyPressureTendency",
    "Sunrise", "Sunset")

println("SOD reports null counts")
countNulls(sod)

println("For SOD, we only non-null values for Sunrise and Sunset columns")
val cleanSod = shrinkColumns(sod, Array(
    "DATE",
    "ELEVATION",
    "SOURCE",
    "Sunrise",
    "Sunset"
))

println("SOM reports null counts")
countNulls(som)

println("For SOM, we only have null values for all columns")
println("Don't use SOM")

cleanFm15.coalesce(1).write.option("header", true).mode("overwrite").csv("final/fm-15")
cleanFm16.coalesce(1).write.option("header", true).mode("overwrite").csv("final/fm-16")
cleanSod.coalesce(1).write.option("header", true).mode("overwrite").csv("final/sod")

