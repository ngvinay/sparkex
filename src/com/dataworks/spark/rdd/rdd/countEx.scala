package com.dataworks.spark.rdd.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Sandeep on 10/28/16.
  */
object countEx {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("countEx").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("OFF")

    val baseRdd: RDD[Int] = sc.parallelize(1 to 100, 10)
    val mappedRdd: RDD[Int] = baseRdd.map(x => x + 2)

    println("Count:" + mappedRdd.count())
  }

}
