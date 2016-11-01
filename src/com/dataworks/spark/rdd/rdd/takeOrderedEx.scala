package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 11/1/16.
  */
object takeOrderedEx {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("takeOrderedEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    takeOrderedInt(sc)
  }

  def takeOrderedInt(sc: SparkContext): Unit = {
    val baseRdd: RDD[Int] = sc.parallelize(Seq(97, 100, 99, 0, 3, 98, 1, 96))
    val collectedArray: Array[Int] = baseRdd.takeOrdered(3)

    collectedArray.foreach(x => println(x))
  }

}
