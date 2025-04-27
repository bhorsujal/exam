import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("Spark").master("local[*]").getOrCreate()

import spark.implicits._

// ************ TAKE NC INPUT AND CONTINUOUSLY PRINT ON SPARK SHELL **************
val lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
val words = lines.as[String].flatMap(_.split(" "))
val wordCounts = words.groupBy("value").count()
val query = wordCounts.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()

// ************ TAKE TEXT FILE INPUT AND PRINT ON SPARK SHELL ****************
//val lines = spark.read.text("/home/siddhesh/input.txt")
//val words = lines.as[String].flatMap(_.split(" "))
//val wordCounts = words.groupBy("value").count()
//wordCounts.show()

// ********** FOR SAVING OUTPUT IN CSV FILE *************
//wordCounts.write.format("csv").option("path", "/home/siddhesh/cute").save()

// ************* TAKE INPUT AS CSV DATAFRAME AND STORE INTO CSV FILE *****************
//val df = spark.read.option("header", "true").option("inferenceType", "true").csv("/home/ayush/Documents/DSBDA/Iris.csv")
//df.show()
//df.describe().show()
//df.select(max("SepalLengthCm").alias("Sepal chi length")).show()
//val x = df.groupBy("SepalLengthCm").count()      ------->> for value_counts()
//x.write.format("csv").option("path", "/home/ayush/output").save()     ------->> can write df.write. for storing entire file


