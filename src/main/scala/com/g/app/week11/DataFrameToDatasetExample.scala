package com.g.app.week11


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.unix_timestamp

import java.sql.Timestamp


case class OrdersData(order_id:Int, order_date:Timestamp, order_customer_id:Int, order_status:String)
object DataFrameToDatasetExample extends App {

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

  import spark.implicits._
  val ts = unix_timestamp($"order_date", "yyyy:MM:dd HH:mm:ss").cast("timestamp")
 val fomattedDF=  ordersDF.withColumn("order_date", ts)
  val orderDs = fomattedDF.as[OrdersData]
  orderDs.filter(x=>x.order_id<10) //with ds you can get the column names
  orderDs.show()
  Logger.getLogger(getClass.getName).info("My application is successfully completed")
  //scala.io.StdIn.readLine()
  spark.stop()
}
