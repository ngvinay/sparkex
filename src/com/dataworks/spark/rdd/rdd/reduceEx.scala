package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object reduceEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("reduceEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    reduceInt(sc)
    reduceInt2(sc)
  }

  def reduceInt(sc: SparkContext): Unit = {
    //val reducedVal: Int = baseRdd.reduce((x, y) => x+y)
    val baseRdd: RDD[Int] = sc.parallelize(1 to 10)
    val reducedVal: Int = baseRdd.reduce((x, y) => {
      val sum = x + y

      println(s"sum of $x to $y, $sum")
      sum
    })

    println("Reduced val = " + reducedVal)

  }

  def reduceInt2(sc: SparkContext): Unit = {

    val baseRdd: RDD[Int] = sc.parallelize(1 to 10)
    val reducedVal: Int = baseRdd.reduce((x, y) => x + y)

    println("Reduced val = " + reducedVal)

  }

}
