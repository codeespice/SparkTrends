package com.g.app.week12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{column, expr}
import org.apache.spark.sql.{SaveMode, SparkSession}


object DataFrameunStructuredlExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","My application 1")
  sparkConf.set("spark.master","local[2]")

val myregEx = """^(\S+) (\S+)\t(\S+)\,(\S+)""".r
case class Orders(order_id:Int,customer_id:Int,order_status:String)

  def parser(line:String)={
    line match{
      case myregEx(order_id,date,customer_id,order_status)=>
        Orders(order_id.toInt,customer_id.toInt,order_status)
    }
  }
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  var lines = spark.sparkContext.textFile("data/week11/orders_new.csv")
val ordersRdd = lines.map(parser)

  import spark.implicits._
  val ordersDS= ordersRdd.toDS().cache() //toDS
  val ordersDF= ordersRdd.toDF().cache() //toDF
  ordersDF.select(column("order_id"),column("customer_id"),expr("concat(order_status,'_STATUS')")).show()
}
