package com.g.app.week11

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DataFrameParquet extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","My application 1")
  sparkConf.set("spark.master","local[2]")
/*
val spark = SparkSession.builder().
  appName("My application 1")
  .master("local[2]")
  .getOrCreate()
*/
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
 /* val ordersDF = spark.read
                .format("Csv")
                .option("header",true)
                .option("inferSchema",true)
                .option("path","data/week11/orders.csv") //returns dataframe
                .load()*/

  val ordersDF = spark.read
    .format("parquet")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", "data/week11/users.parquet") //returns dataframe
    .option("mode","DROPMALFORMED") //drops bad data
    .load()
  ordersDF.printSchema()
  ordersDF.show()
  scala.io.StdIn.readLine()
  spark.stop()
}
