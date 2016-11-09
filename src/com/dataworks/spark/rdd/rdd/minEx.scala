package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object minEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("maxEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    minInt(sc)
  }

  def minInt(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 25)
    val min: Int = baseRdd.min()

    println("Min: " + min)
  }

}
