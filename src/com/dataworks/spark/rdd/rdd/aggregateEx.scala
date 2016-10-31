package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object aggregateEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("aggregateEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    aggregateInt(sc)
  }

  def aggregateInt(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 10, 2)
    val aggregatedRdd: Int = baseRdd.aggregate(0)(f, f)

    println("Computed value = " + aggregatedRdd)
  }

  def f(a: Int, b: Int): Int = {
    val sum = a + b

    println(s"sum of $a, $b, $sum")
    sum
  }

}
