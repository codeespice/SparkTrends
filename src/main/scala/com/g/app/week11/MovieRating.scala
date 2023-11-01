package com.g.app.week11

import org.apache.spark.SparkContext

import scala.io.Source
/*
1.1000 people should have rated for that movie
2.Average rating is greater than 4.5
 */
object MovieRating {
  def main (args:Array[String]): Unit = {

    val sc = new SparkContext("local[*]", "broadcastVariable")

    val ratingsRdd = sc.textFile("data/week11/ratings.dat")
    val moviesRdd = sc.textFile("data/week11/movies.dat").map(x=>{
      val fields = x.split("::")
      (fields(0),fields(1))
    })
    val mapRdd = ratingsRdd.map(x=>{
      val fields = x.split("::")
      (fields(1),fields(2))
    })

    val ratingsIdentityValues = mapRdd.mapValues(x=>(x.toFloat,1))

    val summedRdd = ratingsIdentityValues.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
    val filteredRdd = summedRdd.filter(x=>x._2._2>1000)
    val avrageRdd = filteredRdd.mapValues(x=>x._1/x._2)
    val avrageFilteredRdd = avrageRdd.filter(x=>x._2>4.5)
  //  avrageFilteredRdd.collect().foreach(println)
   val joinedRdd =  moviesRdd.join(avrageFilteredRdd) //join two datasets
    val resultRdd = joinedRdd.map(x=>(x._2._1,x._2._2)).sortBy(x=>x._2,false)
    resultRdd.collect().foreach(println)

  }

}
