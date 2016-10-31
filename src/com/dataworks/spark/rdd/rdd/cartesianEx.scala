package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/31/16.
  */
object cartesianEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("cartesianEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    cartesianEx(sc)
  }

  def cartesianEx(sc: SparkContext): Unit = {
    val baseRdd1: RDD[Int] = sc.parallelize(1 to 5)
    val baseRdd2: RDD[Int] = sc.parallelize(6 to 10)

    val cartesianRdd: RDD[(Int, Int)] = baseRdd1.cartesian(baseRdd2)
    cartesianRdd.foreach(x => println(x))

  }

}
