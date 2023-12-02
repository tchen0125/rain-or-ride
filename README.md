# Rain or Ride

## Project Overview

### Members

|     Name      | NetID  |
| :-----------: | :----: |
| Bongjun Jang  | bj2351 |
|  Luka Tragic  | lt2205 |
| Terrance Chen | tc3325 |

### Structure

1. Ingestion: commands or codes to download data
2. ETL: transform or clean data and store them in HDFS
3. Profiling: provides insights for indivial data
4. Analytics: statistical analysis on all dataset (correlation, etc.)

## Data Access in HDFS

### Location

You can access the data resides in HDFS, by issueing the following command in dataproc.

```
hdfs dfs -ls /user/bj2351_nyu_edu/final/data

Found 3 items
-rw-rwxr--+  1 bj2351_nyu_edu bj2351_nyu_edu     121642 2023-11-30 14:57 final/data/MTA-Daily-Ridership-Data-Beginning-2020.csv
-rw-rwxr--+  1 bj2351_nyu_edu bj2351_nyu_edu    5310911 2023-11-30 14:57 final/data/hourly-weather-nyc-2022.csv
-rw-rwxr--+  1 bj2351_nyu_edu bj2351_nyu_edu   20090169 2023-11-30 14:57 final/data/motor-vehicle-collisions-2022.csv
```

Please ask bj2351@nyu.edu to get your access granted to use `ls` command on `final` directory.
Otherwise, you will not be able to list contents under the directory.

Also, under `hdfs:///user/bj2351_nyu_edu/final` directory, there are several directories:
1. `data` directory for data ingested, which exist in a raw form
2. `cleaned` directory for data cleaned after ETL process.
    - `weather/cond`: weather conditions such as rain, snow, haze, etc. for each day in 2022
    - `weather/agg`: aggregatated weather data for each day, such as temperature (min, max, avg)



For weather data, you can access the file in your spark-shell with the code below.
The filename changes every time we run ETL code, and it's cryptic to read.
Therefore the filename must be identified programmatically as suggested below:

```scala
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
```


### Permissions

The directories have following permissions:

```
$ hdfs dfs -getfcal /user/bj2351_nyu_edu
# file: /user/bj2351_nyu_edu/final
# owner: bj2351_nyu_edu
# group: bj2351_nyu_edu
user::rwx
user:lt2205_nyu_edu:--x
user:tc3180_nyu_edu:--x
group::---
mask::--x
other::---

$ hdfs dfs -getfacl /user/bj2351_nyu_edu/final
# file: /user/bj2351_nyu_edu/final
# owner: bj2351_nyu_edu
# group: bj2351_nyu_edu
user::rwx
user:lt2205_nyu_edu:rwx
user:tc3180_nyu_edu:rwx
group::r-x
mask::rwx
other::r-x
```



