package com.g.app.week12

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


object DataFrameunStructuredlExample extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","My application 1")
  sparkConf.set("spark.master","local[2]")

val myregEx = """^(\S+) (\S+) (\S+) (\S+) """.r
case class Orders(order_id:Int,customer_id:Int,order_status:String)

  def parser(line:String)={
    line match{
      case myregEx(order_id,data,customer_id,order_status)=>
        Orders(order_id.toInt,customer_id.toInt,order_status)
    }
  }
  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

}
