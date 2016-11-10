package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object aggregateByKeyEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    aggregateByKey(sc)
  }

  def aggregateByKey(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 3)
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val word2LineRdd: RDD[(String, Int)] = filteredRdd.map(lines => {
      val split: Array[String] = lines.split(" ")
      (split(0), split.length)
    })
    val aggregatedRdd: RDD[(String, Int)] = word2LineRdd.aggregateByKey(0)(seqOp, combOp)
    aggregatedRdd.foreach(println)
  }

  def seqOp(acc: Int, valu: Int): Int = {
    val sum = acc + valu
    println(s"sum of $acc, $valu, $sum")
    sum
  }

  def combOp(valu1: Int, valu2: Int): Int = {
    val sum = valu1 + valu2
    println(s"combOp sum of $valu1, $valu2, $sum")
    sum
  }


}
