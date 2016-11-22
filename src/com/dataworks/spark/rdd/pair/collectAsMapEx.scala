package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by Sandeep on 10/28/16.
  */
object collectAsMapEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("collectAsMapEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    collectAsMap(sc)
  }

  def collectAsMap(sc: SparkContext): Unit = {
    val baseRDD: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/spark")
    val filteredRdd: RDD[String] = baseRDD.filter(line => !line.isEmpty).flatMap(_.split(" "))
      .filter(x => !x.isEmpty)
    val pairRdd: RDD[(String, Int)] = filteredRdd.map(x => (x, x.length))

    val collectedMap: Map[String, Int] = pairRdd.collectAsMap()
    collectedMap.foreach { case (k, v) => println(k + "->" + v) }
  }

}
