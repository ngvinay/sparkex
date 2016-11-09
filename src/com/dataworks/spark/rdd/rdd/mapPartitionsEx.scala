package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object mapPartitionsEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("mapPartitionsEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    mapPartitionsStr(sc)
  }

  def mapPartitionsStr(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark", 4)
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty)
    val flattenedRdd: RDD[String] = filteredRdd.flatMap(line => line.split(" "))
    val mapPartitions: RDD[(String, Int)] = flattenedRdd.mapPartitions((partition: Iterator[String]) => {
      var pair: List[(String, Int)] = List[(String, Int)]()
      partition.foreach(string => {
        pair = pair :+ (string, string.length)
      })
      pair.iterator
    })

    mapPartitions.foreach(println)
  }

}
