// Databricks notebook source
// MAGIC %run /Users/yavuz_karabiyik/Spark-Programming-1.0.0-IL/Scala/Includes/Classroom-Setup

// COMMAND ----------

val df2 = spark.read.textFile("dbfs:/mnt/training/retail-org/README.md")
display(df2)

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/training/retail-org/sales_orders"))

// COMMAND ----------

val input_file = "dbfs:/mnt/training/retail-org/sales_orders/"

val df = spark.read.json(input_file)
display(df)

// COMMAND ----------

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.aggregate
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.from_unixtime
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.to_timestamp


val sales_df = df.withColumn("product_ids", col("ordered_products.id"))
  .withColumn("total_purchase_amount", aggregate($"ordered_products.price", lit(0),  (x,y) => (x.cast("int") + y.cast("int"))))
  .withColumn("date", to_timestamp(from_unixtime(col("order_datetime")).cast("timestamp")))
  .withColumn("weekday", date_format(col("date").cast("timestamp"), "EEE"))
  .withColumn("my_hour", hour(col("date")))
  .drop("clicked_items", "ordered_products", "promo_info", "order_datetime")
  .na.drop
  .select("*")
display(sales_df)


// COMMAND ----------

val delta_dir = workingDir + "/mine"

sales_df.write.format("delta").partitionBy("my_hour").option("overwriteSchema", true).mode("overwrite").save(delta_dir)

// COMMAND ----------

sales_df.write.format("delta").option("overwriteSchema", true).mode("overwrite").saveAsTable("my_sales")

// COMMAND ----------

// MAGIC %sql
// MAGIC Select weekday, sum(total_purchase_amount) from my_sales GROUP BY weekday 

// COMMAND ----------

//consist of 3 json log files
display(spark.read.json(delta_dir+ "/_delta_log"))

// COMMAND ----------

//spark.sql("DROP TABLE IF EXISTS my_sales")
//spark.sql(s"CREATE TABLE my_sales USING DELTA LOCATION '$delta_dir'")

// COMMAND ----------

val df = spark.read.format("delta").option("versionAsOf", 0).load(delta_dir)
display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY my_sales

// COMMAND ----------



val df_readStream = spark.readStream
  .option("maxFilesPerTrigger", 1)
  .format("delta")
  .load(delta_dir)


// COMMAND ----------

val orders_df = df_readStream
  .withColumn("date", col("date"))
  .withWatermark("date", "2 hours")


assert(orders_df.schema.mkString("").contains("StructField(date,TimestampType,true"))

// COMMAND ----------

import org.apache.spark.sql.functions._

val hourly_order_traffic = orders_df
  .groupBy(col("weekday"), window(col("date"), "1 hour"))
  .agg(sum("total_purchase_amount").alias("total_amount"))
  .select(col("total_amount"), col("weekday"), hour(col("window.start")).alias("hour"))
  .sort("hour")

// COMMAND ----------

display(hourly_order_traffic, streamName = "daily_order_traffic")

// COMMAND ----------

untilStreamIsReady("daily_order_traffic")

for (s <- spark.streams.active) {
  if (s.name == "daily_order_traffic") {
    s.stop()
  }
}

// COMMAND ----------

// MAGIC %run /Users/yavuz_karabiyik/Spark-Programming-1.0.0-IL/Scala/Includes/Classroom-Cleanup
