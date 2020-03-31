// Databricks notebook source
val inputDirectoryPath = "/data/dis/marketing_data/raw"
val outputDirectoryPath = "/data/dis/marketing_data/test"

dbutils.notebook.run("hdfscleanup", 180, Map(
  "inputDirectoryPath" -> inputDirectoryPath ,
  "outputDirectoryPath" -> outputDirectoryPath ,
  "startyear" -> "2018" ,
   "endyear" -> "2018" ,
   "startday" -> "1" ,
   "endday" -> "2" ,
   "startmonth" -> "1" ,
   "endmonth" -> "1"
))

// COMMAND ----------

val date = 20180101
val df1 = spark.read.parquet(inputDirectoryPath+"/ ses_dts="+date)
val df2 = spark.read.parquet(outputDirectoryPath+"/ ses_dts="+date)

// COMMAND ----------

assert (df1.count() == df2.count())
