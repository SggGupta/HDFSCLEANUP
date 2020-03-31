// Databricks notebook source
// MAGIC %md
// MAGIC ### If following script has to be run from Bash the use following,
// MAGIC 
// MAGIC ####  spark-shell -i hdfscleanup.scale args1 args2
// MAGIC 
// MAGIC ###  since I am using databricks for my development , I would use ADF pipeline to execute my script. Where I have to give configuration of Spark cluster and notebook name and parameters in json file.

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ####### Since we want that Jan to be able to run the script as number of times he wants for different set of dates , I made 6 new parameters to be passed when spark job is executed. Please not directory path can also be parameterized.

// COMMAND ----------

  dbutils.widgets.text("inputDirectoryPath", "")
  val inputDirectoryPath = dbutils.widgets.get("inputDirectoryPath")
  print ("Param -\'inputDirectoryPath':")
  print (inputDirectoryPath)

  dbutils.widgets.text("outputDirectoryPath", "")
  val outputDirectoryPath = dbutils.widgets.get("outputDirectoryPath")
  print ("Param -\'outputDirectoryPath':")
  print (outputDirectoryPath)
  
  dbutils.widgets.text("startyear", "")
  val startyear = dbutils.widgets.get("startyear")
  print ("Param -\'startyear':")
  print (startyear)

  dbutils.widgets.text("startmonth", "")
  val startmonth = dbutils.widgets.get("startmonth")
  print ("Param -\'startmonth':")
  print (startmonth)

  dbutils.widgets.text("startday", "")
  val startday = dbutils.widgets.get("startday")
  print ("Param -\'startday':")
  print (startday)

  dbutils.widgets.text("endyear", "")
  val endyear = dbutils.widgets.get("endyear")
  print ("Param -\'endyear':")
  print (endyear)

  dbutils.widgets.text("endmonth", "")
  val endmonth = dbutils.widgets.get("endmonth")
  print ("Param -\'endmonth':")
  print (endmonth)

  dbutils.widgets.text("endday", "")
  val endday = dbutils.widgets.get("endday")
  print ("Param -\'endday':")
  print (endday)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Following lines of code is reading multiple small files from the same folder and loading data into spark dataframe and writing back into same folder using overwrite mode and merging all small files into single file using coalesce(1) , here we could also use repartition but that my couse reshufling.  

// COMMAND ----------

import java.time.temporal.ChronoUnit
import java.time.{LocalDate, Period }
import java.time.format.{ DateTimeFormatter, DateTimeParseException }

val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
//val start = LocalDate.of(2018, 1, 1)
//val end   = LocalDate.of(2018, 3, 30)

val start = LocalDate.of(startyear.toInt, startmonth.toInt, startday.toInt)
val end   = LocalDate.of(endyear.toInt, endmonth.toInt, endday.toInt)
val inputDirectory = inputDirectoryPath
val outputDirectory = outputDirectoryPath

// Create List of `LocalDate` for the period between start and end date

val dates: IndexedSeq[LocalDate] = (0L to (end.toEpochDay - start.toEpochDay))
  .map(days => start.plusDays(days))

dates.foreach(e => 
              { val date = e.format(formatter)
                println("file path = "+inputDirectory+"/ ses_dts="+date)
                val df1 = spark.read.parquet(inputDirectory+"/ ses_dts="+date)
                       df1.coalesce(1).write.mode(SaveMode.Overwrite).parquet(outputDirectory+"/ ses_dts="+date)
              })
