package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object foldEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("foldEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    foldIntSum(sc)
  }

  def foldIntSum(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 10, 2)
    val reducedVal: Int = baseRdd.fold(0)((x, y) => {
      val sum = x + y

      println(s"sum of $x, $y, $sum")
      sum
    })

    println("Folded val = " + reducedVal)

  }

  def foldIntSum2(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 10)
    val reducedVal: Int = baseRdd.fold(0)((x, y) => x + y)

    println("Folded val = " + reducedVal)

  }

  def foldIntProduct(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 10)
    val reducedVal: Int = baseRdd.fold(1)((x, y) => {
      val product = x * y

      println(s"Product of $x, $y, $product")
      product
    })

    println("Folded val = " + reducedVal)

  }

}
