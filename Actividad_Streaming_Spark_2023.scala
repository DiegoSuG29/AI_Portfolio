// Databricks notebook source
dbutils.fs.rm("/FileStore/tables/actividad1/", true)


// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/actividad1/

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/actividad1/

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/actividad1/Familia1.json

// COMMAND ----------

// MAGIC %md <h2> Explorar el contenido de un archivo JSON </h2>

// COMMAND ----------

// MAGIC %fs head /FileStore/tables/streaming/file-1.json

// COMMAND ----------

val varint = 1
val varint2 : Int = 1
var varint3 = 5
varint = 2
varint3 = 4
varint3 = "dato"

// COMMAND ----------

// Cons ::
val list1: List[String] = "clase" :: "de" :: "Streaming" :: Nil
val list2: List[String] = List("clase","de","Streaming")
list1 == list2

// COMMAND ----------

val list1: List[String] = "clase" :: "de" :: "Streaming"

// COMMAND ----------

// StructType
//import org.apache.spark.sql.types._  // _ == *
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType


/*val jsonSchema = StructType(
 StructField("time", TimestampType, true) ::
 StructField("action", StringType, true) ::
 Nil
) */

val jsonSchema = new StructType()
         .add("nombre", StringType)
         .add("apellido",StringType)

// val jsonSchema = new StructType().add("time", TimestampType).add("action",StringType)

// COMMAND ----------

import org.apache.spark.sql.types.StructField


val jsonSchema = StructType(
 StructField("nombre", StringType, true) ::
 StructField("apellido", StringType, true) ::
 Nil
)

// COMMAND ----------

val inputPath = "/FileStore/tables/actividad1/"


// COMMAND ----------

spark

// COMMAND ----------

// Static DF
// val inputPath = "/databricks-datasets/structured-streaming/events/"
val staticInputDF = (
 spark
   .read
   .schema(jsonSchema)
   .json(inputPath)
)

// COMMAND ----------

display(staticInputDF)

// COMMAND ----------

staticInputDF.printSchema

// COMMAND ----------

staticInputDF.show(12, true)

// COMMAND ----------

staticInputDF.count

// COMMAND ----------

import org.apache.spark.sql.functions.window
import org.apache.spark.sql.functions.col

val staticCountDF =
  staticInputDF
      .groupBy(col("nombre"))
      .count()

// COMMAND ----------

staticCountDF.createOrReplaceTempView("static_counts")

// COMMAND ----------

staticCountDF.count

// COMMAND ----------

// MAGIC %sql select count(1) from static_counts

// COMMAND ----------

// MAGIC %sql select * from static_counts

// COMMAND ----------

val staticInputDF = (
 spark
   .read
   .schema(jsonSchema)
   .json(inputPath)
)

// COMMAND ----------

val streamingInputDF = (
 spark
   .readStream
   .schema(jsonSchema)
   .option("maxFilesPerTrigger", 1)
   .json(inputPath)
)

// COMMAND ----------

streamingInputDF.show(10)

// COMMAND ----------

val streamingCountDF =
  streamingInputDF
     .groupBy(col("nombre"))
     .count()

// COMMAND ----------



// COMMAND ----------

streamingCountDF.isStreaming

// COMMAND ----------

staticCountDF.isStreaming

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")

val query = 
 streamingCountDF
  .writeStream
  .format("memory")
  .queryName("counts_streaming")
  .outputMode("complete")
  


// COMMAND ----------

val queryhandler = query.start()

// COMMAND ----------

// MAGIC %sql select count(1) from counts_streaming

// COMMAND ----------

// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from counts_streaming order by time, action

// COMMAND ----------

// MAGIC %sql select * from counts_streaming

// COMMAND ----------

queryhandler.stop()
