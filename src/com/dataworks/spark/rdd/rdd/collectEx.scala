package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object collectEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("foldEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    collectInts(sc)
  }

  def collectInts(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(1 to 100)
    val collectedArray: Array[Int] = baseRdd.collect()

    collectedArray.foreach(x => println(x))
  }

}
