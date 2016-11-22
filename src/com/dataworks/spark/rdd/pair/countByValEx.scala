package com.dataworks.spark.rdd.pair

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by Sandeep on 11/22/16.
  */
object countByValEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("countByValEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    countByVal(sc)
  }

  def countByVal(sc: SparkContext): Unit = {
    val baseRdd: RDD[String] = sc.textFile("/Users/Sandeep/dataworks/data/this", 4)
      .filter(x => !x.isEmpty)

    val thisPair: RDD[(String, Int)] = baseRdd.flatMap(x => x.split(" ")).map(x => (x, x.length))
    val countValue: Map[(String, Int), Long] = thisPair.countByValue()

    countValue.foreach { case (k, v) => println(k._1 + k._2 + "=" + v) }

  }

}
