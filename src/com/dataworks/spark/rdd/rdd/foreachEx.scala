package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object foreachEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("foreachEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    forEachPrint(sc)
  }

  def forEachPrint(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 100)
    baseRdd.foreach(x => println(x))
  }

}
