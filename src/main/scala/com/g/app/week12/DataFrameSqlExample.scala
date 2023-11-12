package com.g.app.week12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}


object DataFrameSqlExample extends App {

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

  ordersDF.createOrReplaceTempView("orders")
  val resultDF = spark.sql("select order_status,count(*) as status_count from orders group by order_status")
  resultDF.show()
  spark.sql("create database if not exists retail")
  resultDF.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .bucketBy(4,"status_count")
    .sortBy("status_count")
    .saveAsTable("retail.orders1") //stores in spark-warehouse folder in the project
spark.catalog.listTables("retail").show()
  Logger.getLogger(getClass.getName).info("My application is successfully completed")
  //scala.io.StdIn.readLine()
  spark.stop()
}
