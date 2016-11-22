package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by Sandeep on 10/28/16.
  */
object countByKeyEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("countByKeyEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    countByKey(sc)
  }

  def countByKey(sc: SparkContext): Unit = {
    val baseRdd: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/this", 8)
      .filter(x => !x.isEmpty)

    val thisPair: RDD[(String, Int)] = baseRdd.flatMap(x => x.split(" ")).map(x => (x, x.length))
    val countKey: Map[String, Long] = thisPair.countByKey()
    countKey.foreach { case (k, v) => println(k + "=" + v) }

  }

}
