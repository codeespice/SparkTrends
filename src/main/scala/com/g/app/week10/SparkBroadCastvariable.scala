package com.g.app.week10

import org.apache.log4j._
import org.apache.spark.SparkContext

import scala.io.Source
object SparkBroadCastvariable {

  def main (args:Array[String]){
    def loadBoringData():Set[String]={
      var boringDataSet :Set[String] = Set()
      val lines = Source.fromFile("data/week10/boringwords.txt").getLines()
      for(line <- lines){
        boringDataSet += line
      }
      boringDataSet
    }

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","broadcastVariable")
   // val initalRdd = sc.textFile("data\\week10\\bigdatacampaigndata.csv") // windows
   val boringWords = sc.broadcast(loadBoringData())
   val initalRdd = sc.textFile("data/week10/bigdatacampaigndata.csv") //Mac
    val mappedInput = initalRdd.map(x=>(x.split(",")(10).toFloat,x.split(",")(0)))

    val words = mappedInput.flatMapValues(x=>x.split(" "))
    val finalMapped  =  words.map(x=>(x._2.toLowerCase,x._1))


    val filterBoringWords = finalMapped.filter(x => !boringWords.value(x._1))
    val total = filterBoringWords
                  .reduceByKey((x,y)=>x+y)
    val sorted = total.sortBy(x=>x._2,false) //ascending false i.e descending

    sorted.take(20).foreach(println)

  }

}
