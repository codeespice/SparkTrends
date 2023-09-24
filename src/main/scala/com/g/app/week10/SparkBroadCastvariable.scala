package com.g.app.week10
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkContext
object SparkBroadCastvariable {

  def main (args:Array[String]){


    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","broadcastVariable")
    val initalRdd = sc.textFile("data\\week10\\bigdatacampaigndata.csv")
    val mappedInput = initalRdd.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))

    val words = mappedInput.flatMapValues(x=>x.split(" "))
    val finalMapped  =  words.map(x=>(x._2.toLowerCase,x._1))

    val total = finalMapped
                  .reduceByKey((x,y)=>x+y)
    val sorted = total.sortBy(x=>x._2,false) //ascending false i.e descending

    sorted.take(20).foreach(println)

  }

}
