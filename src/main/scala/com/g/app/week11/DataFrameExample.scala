package com.g.app.week11

import org.apache.commons.net.ntp.TimeStamp
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DataFrameExample extends App {

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
  val ordersDF = spark.read
                .option("header",true)
                .option("inferSchema",true)
                .csv("data/week11/orders.csv") //returns dataframe

  ordersDF.printSchema()
  ordersDF.show()
  val groupedDF = ordersDF.repartition(4)
    .where("order_customer_id > 10000")
    .select("order_id","order_customer_id")
    .groupBy("order_customer_id")
    .count()
  groupedDF.show()

  Logger.getLogger(getClass.getName).info("My application is successfully completed")
  scala.io.StdIn.readLine()
  spark.stop()
}
