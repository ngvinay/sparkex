package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * Created by Sandeep on 10/28/16.
  */
object countByValueEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("foldEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    countByValue(sc)
  }

  def countByValue(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(Seq(1, 4, 3, 2, 5, 1, 6, 7, 10, 5, 3, 2))
    val countedMap: Map[Int, Long] = baseRdd.countByValue()
    countedMap.foreach { case (key, value) => println(s"Val = $key, count = $value") }
  }

}
