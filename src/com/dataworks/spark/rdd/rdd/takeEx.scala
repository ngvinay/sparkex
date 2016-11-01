package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/31/16.
  */
object takeEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("takeEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    takeInts(sc)
  }

  def takeInts(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 100, 10)
    val collectedArray: Array[Int] = baseRdd.take(28)

    collectedArray.foreach(x => println(x))
  }

}
