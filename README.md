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


## Analysis

| Fog | tt crash | ppl injured | ppl killed | sub        | bus        | days | ppl injured per day | ppl killed per day | crashes per day | sub per day | bus per day |
|------------|----------|-------------|------------|------------|------------|------|---------------------|--------------------|-----------------|-------------|-------------|
| true       | 8416.0   | 4129.0      | 13.0       | 7.691216E7 | 3.1878012E7| 29   | 142.379             | 0.448              | 290.0           | 2652143.0   | 1099242.0   |
| false      | 95461.0  | 47799.0     | 274.0      | 9.35593719E8| 3.92068812E8| 336  | 142.259             | 0.815              | 284.0           | 2784505.0   | 1166871.0   |

| Mist | tt crash | ppl injured | ppl killed | sub         | bus         | days | ppl injured per day | ppl killed per day | crashes per day | sub per day | bus per day |
|-------------|----------|-------------|------------|-------------|-------------|------|---------------------|--------------------|-----------------|-------------|-------------|
| true        | 35998.0  | 17774.0     | 78.0       | 3.55289869E8| 1.47911003E8| 126  | 141.063             | 0.619              | 286.0           | 2819761.0   | 1173897.0   |
| false       | 67879.0  | 34154.0     | 209.0      | 6.5721601E8 | 2.76035821E8| 239  | 142.904             | 0.874              | 284.0           | 2749858.0   | 1154962.0   |

| Rain | tt crash | ppl injured | ppl killed | sub         | bus         | days | ppl injured per day | ppl killed per day | crashes per day | sub per day | bus per day |
|-------------|----------|-------------|------------|-------------|-------------|------|---------------------|--------------------|-----------------|-------------|-------------|
| true        | 37112.0  | 18576.0     | 87.0       | 3.59742874E8| 1.49923187E8| 130  | 142.892             | 0.669              | 285.0           | 2767253.0   | 1153255.0   |
| false       | 66765.0  | 33352.0     | 200.0      | 6.52763005E8| 2.74023637E8| 235  | 141.923             | 0.851              | 284.0           | 2777715.0   | 1166058.0   |

| Snow | tt crash | ppl injured | ppl killed | sub         | bus         | days | ppl injured per day | ppl killed per day | crashes per day | sub per day | bus per day |
|-------------|----------|-------------|------------|-------------|-------------|------|---------------------|--------------------|-----------------|-------------|-------------|
| true        | 4725.0   | 2024.0      | 9.0        | 3.7627331E7 | 1.5598916E7 | 18   | 112.444             | 0.5                | 263.0           | 2090407.0   | 866606.0    |
| false       | 99152.0  | 49904.0     | 278.0      | 9.74878548E8| 4.08347908E8| 347  | 143.816             | 0.801              | 286.0           | 2809448.0   | 1176795.0   |

| Haze | tt crash | ppl injured | ppl killed | sub         | bus         | days | ppl injured per day | ppl killed per day | crashes per day | sub per day | bus per day |
|-------------|----------|-------------|------------|-------------|-------------|------|---------------------|--------------------|-----------------|-------------|-------------|
| true        | 8360.0   | 4169.0      | 31.0       | 8.5338812E7 | 3.6530611E7 | 30   | 138.967             | 1.033              | 279.0           | 2844627.0   | 1217687.0   |
| false       | 95517.0  | 47759.0     | 256.0      | 9.27167067E8| 3.87416213E8| 335  | 142.564             | 0.764              | 285.0           | 2767663.0   | 1156466.0   |


