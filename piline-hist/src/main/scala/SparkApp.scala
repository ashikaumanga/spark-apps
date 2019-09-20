import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DateType


object SparkApp {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MyApp")
      .setMaster("local[*]")
    //val sc = new SparkContext(conf)


    val session = SparkSession.builder.config(conf = conf)
      .appName("MyApp")
      .getOrCreate()

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._


    //Read incremental data coming from Sink Connector
    val incrementalDF = session.read
      .option("header","true")
      //.option("dateFormat", "yyyy-MM-dd HH:mm:00")
      .option("inferSchema", "true")
      .csv("/Users/ashika.umagiliya/Desktop/binlog2.csv").toDF
      .withColumnRenamed("elt_time","etl_str")
      .withColumn("etl_start_time",to_timestamp(col("etl_str"),"yyyy-MM-dd'T'HH:mm")) //add
      .withColumn("etl_end_time", to_timestamp(expr("NULL"),"yyyy-MM-dd'T'HH:mm"))
      .drop(col("etl_str"))


    //Read current history tables  active=true parition. (this is same as the query table)
    val currentHistActiveDF =  session.read
      .option("header", "true")
      //.option("dateFormat", "yyyy-MM-dd'T'HH:mm")
      .option("inferSchema", "true")
      .csv(path = "/Users/ashika.umagiliya/Desktop/spdb-history")
      //.csv(path = "/Users/ashika.umagiliya/Desktop/currtable.csv")
      .toDF

      .withColumnRenamed("etl_start_time","etl_start_time_str")
      .withColumnRenamed("etl_end_time","etl_end_time_str")


      .withColumn("etl_start_time",to_timestamp(col("etl_start_time_str"),"yyyy-MM-dd'T'HH:mm"))
      .withColumn("etl_end_time", to_timestamp(col("etl_end_time_str"),"yyyy-MM-dd'T'HH:mm"))

      .where("active=true")
      .drop("etl_start_time_str","etl_end_time_str","active")
      .drop("dt")

    //currentHistActiveDF.show()
    //print(currentHistActiveDF.schema)

   //union active historical partition and incremental data
    val combinedDf = currentHistActiveDF.union(incrementalDF)//.drop("etl_end_time")
    combinedDf.show()

    //create active Column,use the latest record as active=true
    val window = Window.orderBy(desc("etl_start_time")).partitionBy("pk")
    val unionActiveColumnAddedDF = combinedDf.withColumn("rank", dense_rank.over(window))
        .withColumn("active", expr("(rank==1 AND deleted==false)"))

    unionActiveColumnAddedDF.show()

    //create etl_start_date , etl_end_date Column
    val finalUnionDF = unionActiveColumnAddedDF
        .withColumn("etl_end_time_tmp", lag(col("etl_start_time"),1)
         .over(window))
        .withColumn("etl_end_time", when(expr("deleted==true AND rank==1"), to_timestamp(current_timestamp,"yyyy-MM-dd'T'HH:mm")).otherwise(col("etl_end_time_tmp")))
        .drop("etl_end_time_tmp", "rank")
        .withColumn("dt", col("etl_start_time").cast(DateType))

    finalUnionDF.show()

    //System.exit(1)


    finalUnionDF.cache()

    //Append active=false to current active=false partition
    finalUnionDF
      .where("active=false")
      .write.format("com.databricks.spark.csv")
      .partitionBy("dt")
      .option("header", "true")
      .mode(SaveMode.Append).save("/Users/ashika.umagiliya/Desktop/spdb-history/active=false")

    //Overwrite active=false to replace curremt active parition
    finalUnionDF
      .where("active=true")
      .write.format("com.databricks.spark.csv")
      .partitionBy("dt")
      .option("header", "true")
      .mode(SaveMode.Overwrite).save("/Users/ashika.umagiliya/Desktop/spdb-history/active=true")

  }
}
