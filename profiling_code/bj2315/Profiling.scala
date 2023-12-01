import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

val configuration = new Configuration()
val fileSystem = FileSystem.get(configuration)

val path = "/user/bj2351_nyu_edu/final/cleaned/weather/"

def getFilePath(path: Path): String =  {
  val file = fileSystem
      .listStatus(path)
      .filter(_.getPath.getName.endsWith(".csv"))

  file.head.getPath.toString
}

val aggPath = new Path(path + "agg")
val condPath = new Path(path + "cond")
println(aggPath)
println(condPath)
val aggFile = getFilePath(aggPath)
val condFile = getFilePath(condPath)

val aggDf = spark.read.option("header", true).csv(aggFile)
val condDf = spark.read.option("header", true).csv(condFile)

println("Total Number of Rainy days: " + condDf.filter($"Rain" === "true").count())
println("Total Number of Snowy days: " + condDf.filter($"Snow" === "true").count())
println("Total Number of Hazy days: " + condDf.filter($"Haze" === "true").count())
println("Total Number of Misty days: " + condDf.filter($"Mist" === "true").count())
println("Total Number of Foggy days: " + condDf.filter($"Fog" === "true").count())



