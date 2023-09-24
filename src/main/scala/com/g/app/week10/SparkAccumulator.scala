package com.g.app.week10

import org.apache.spark.SparkContext

object SparkAccumulator {
  def main (args:Array[String]){
    val sc = new SparkContext("local[*]","broadcastVariable")
    val linesRdd = sc.textFile("data/week10/simplefile.txt")
    val blankAccumulator = sc.longAccumulator("Count blank lines")

    linesRdd.foreach(line => if(line.isBlank) blankAccumulator.add(1))
    println("blank line :" + blankAccumulator.count)

  }
}
